// Command lab runs one arm of the experiment and prints the numbers that arm produced.
//
// The repository exists because someone asked "isn't ClickHouse unable to take a lot of requests per
// second?" and the honest answer is a measurement, not an opinion. Each arm differs in exactly one
// thing - how many INSERT statements the ingest path issues - and everything else is held still.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/lasttoss/event-ingest-lab/internal/harness"
)

func main() {
	var (
		mode    = flag.String("mode", "batched", "arm under test: batched (WAL + Vector) or per-event (one INSERT per event)")
		api     = flag.String("api", "http://127.0.0.1:8099/v1/events", "ingest endpoint")
		chURL   = flag.String("clickhouse", "http://127.0.0.1:8123", "clickhouse http endpoint")
		chUser  = flag.String("clickhouse-user", "lab", "clickhouse user")
		chPass  = flag.String("clickhouse-password", "lab", "clickhouse password")
		events  = flag.Int("events", 100000, "events to push")
		chunk   = flag.Int("chunk", 200, "events per POST")
		users   = flag.Int("users", 5000, "distinct synthetic players")
		offline = flag.Duration("offline-for", 0, "how long the synthetic client was offline before this flush")
		settle  = flag.Duration("settle", 20*time.Second, "wait for Vector and merges after the last event")
		label   = flag.String("label", "", "what to call this arm in the report")
	)
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancel()

	ch := harness.NewCH(*chURL, *chUser, *chPass)
	// Every arm starts from the same empty table, or the comparison is between two histories.
	if err := ch.Exec(ctx, "TRUNCATE TABLE IF EXISTS lab.events"); err != nil {
		fail("truncate events: %v", err)
	}
	if err := ch.Exec(ctx, "TRUNCATE TABLE IF EXISTS lab.events_dlq"); err != nil {
		fail("truncate dlq: %v", err)
	}

	// A funnel needs a timeline, and a client that was offline for three days delivers events whose
	// event_time is three days old. Both are modelled here, or the query timings below would be
	// timings for a table that only ever contains today.
	offlineFors := []time.Duration{0, 24 * time.Hour, 48 * time.Hour, 72 * time.Hour}
	if *offline > 0 {
		offlineFors = []time.Duration{*offline, *offline / 2, *offline / 4, 0}
	}
	evs := harness.Build(7, *events, *users, 3, offlineFors)
	c := harness.NewClient(*api, *chunk)

	start := time.Now()
	if err := c.Flush(ctx, evs); err != nil {
		fail("flush: %v", err)
	}
	delivered := time.Since(start)

	// The store is asynchronous by design, so "the client is done" is not "the data is queryable".
	// Polling the target table is the only honest way to know which of the two we are looking at.
	var landed string
	deadline := time.Now().Add(*settle)
	var rows uint64
	for time.Now().Before(deadline) {
		landed, _, _ = ch.Scalar(ctx, "SELECT count() FROM lab.events")
		fmt.Sscanf(landed, "%d", &rows)
		if rows >= uint64(*events) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	time.Sleep(3 * time.Second) // let the merges say what they are going to say

	sent, accepts, rejected, refused, errors, retries, bytes := c.Report()

	report := &strings.Builder{}
	fmt.Fprintf(report, "### %s\n\n", armTitle(*label, *mode))
	fmt.Fprintf(report, "| metric | value |\n|---|---|\n")
	fmt.Fprintf(report, "| events generated | %d |\n", *events)
	fmt.Fprintf(report, "| events per POST | %d |\n", *chunk)
	rowsPOST := accepts / max(*chunk, 1)
	fmt.Fprintf(report, "| POSTs the client made | ~%d |\n", rowsPOST)
	fmt.Fprintf(report, "| accepted (202) | %d |\n", accepts)
	fmt.Fprintf(report, "| refused (503, backpressure) | %d |\n", refused)
	fmt.Fprintf(report, "| rejected by validation | %d |\n", rejected)
	fmt.Fprintf(report, "| client errors / retries | %d / %d |\n", errors, retries)
	fmt.Fprintf(report, "| bytes on the wire (gzipped) | %s |\n", humanBytes(bytes))
	fmt.Fprintf(report, "| time to hand over all events | %s |\n", delivered.Round(time.Millisecond))
	fmt.Fprintf(report, "| accepted rate | %.0f events/s |\n", float64(accepts)/max(delivered.Seconds(), 0.001))
	fmt.Fprintf(report, "| rows in ClickHouse after settle | %s |\n", landed)
	// The number the arm is actually about. The API answers 202 as soon as an event is queued, so
	// "accepted" measures the edge and not the storage - a pipeline can accept every event and keep
	// half of them. This line is the difference, and it is the reason the arms exist.
	if n, err := strconv.ParseUint(landed, 10, 64); err == nil {
		lost := accepts - int(n)
		if lost < 0 {
			lost = 0
		}
		fmt.Fprintf(report, "| **events the API accepted but the table never got** | **%d** |\n", lost)
	}
	_ = sent

	// Duplicates are the failure a client retry causes, and a ReplacingMergeTree does not remove them
	// until a merge has run - so both numbers are reported, and the difference is the honest one.
	if v, d, err := ch.Scalar(ctx, "SELECT count() - uniqExact(event_id) FROM lab.events"); err == nil {
		fmt.Fprintf(report, "| duplicate rows (before merge) | %s |\n", strings.TrimSpace(v))
		_ = d
	}
	if v, _, err := ch.Scalar(ctx, "SELECT count() FROM lab.events FINAL"); err == nil {
		fmt.Fprintf(report, "| rows after FINAL (deduplicated) | %s |\n", strings.TrimSpace(v))
	}
	parts, _, _ := ch.Scalar(ctx, "SELECT count() FROM system.parts WHERE active AND database='lab' AND table='events'")
	partRows, _, _ := ch.Scalar(ctx, "SELECT sum(rows) FROM system.parts WHERE active AND database='lab' AND table='events'")
	fmt.Fprintf(report, "| active parts | %s |\n", strings.TrimSpace(parts))
	fmt.Fprintf(report, "| rows across those parts | %s |\n", strings.TrimSpace(partRows))
	if v, _, err := ch.Scalar(ctx, "SELECT count() FROM lab.events_dlq"); err == nil {
		fmt.Fprintf(report, "| dead-lettered lines (not silently dropped) | %s |\n", strings.TrimSpace(v))
	}
	fmt.Fprintf(report, "\n")

	// The queries a product asks, timed on the data this arm just wrote.
	type q struct {
		name string
		sql  string
	}
	queries := []q{
		{"DAU per day, last 7 days (scans the raw table, which is what a dashboard would do)",
			"SELECT toDate(event_time) AS day, uniqExact(user_id) AS dau FROM lab.events WHERE event_time >= now() - INTERVAL 7 DAY GROUP BY day ORDER BY day"},
		{"the same DAU read from the materialized view instead of the raw rows",
			"SELECT day, uniqMerge(users) AS dau FROM lab.events_daily WHERE day >= today() - 7 GROUP BY day ORDER BY day"},
		{"funnel: session_start -> level_complete -> iap_purchase, 24h window",
			"SELECT countIf(chain > 0) AS started, countIf(chain > 1) AS reached_level, countIf(chain > 2) AS purchased FROM (SELECT user_id, windowFunnel(86400)(toDateTime(event_time), event = 'session_start', event = 'level_complete', event = 'iap_purchase') AS chain FROM lab.events GROUP BY user_id)"},
		{"players active 3 days ago (events a client delivered late, by event_time)",
			"SELECT uniqExact(user_id) FROM lab.events WHERE toDate(event_time) = today() - 3"},
		{"revenue by placement, from the raw rows",
			"SELECT sum(value), uniqExact(user_id) FROM lab.events WHERE event = 'iap_purchase'"},
	}
	fmt.Fprintf(report, "| query | rows | server time |\n|---|---|---|\n")
	for _, query := range queries {
		t0 := time.Now()
		data, server, err := ch.Rows(ctx, query.sql)
		if err != nil {
			fmt.Fprintf(report, "| %s | error: %v | - |\n", query.name, err)
			continue
		}
		fmt.Fprintf(report, "| %s | %d | %s (round trip %s) |\n", query.name, len(data), server.Round(time.Millisecond), time.Since(t0).Round(time.Millisecond))
	}
	fmt.Fprintf(report, "\n")

	out := report.String()
	if path := os.Getenv("LAB_OUT"); path != "" {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			fail("open %s: %v", path, err)
		}
		defer f.Close()
		_, _ = f.WriteString(out)
	}
	fmt.Print(out)
}

func armTitle(label, mode string) string {
	if label != "" {
		return label
	}
	if mode == "per-event" {
		return "Arm A - one INSERT per event (the mistake, kept so it can be measured)"
	}
	return "Arm B - WAL, Vector, batched inserts, async_insert"
}

func humanBytes(n int) string {
	switch {
	case n > 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	case n > 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%d B", n)
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
