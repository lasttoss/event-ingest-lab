package store

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/lasttoss/event-ingest-lab/internal/ingest"
)

// Mode is the experiment. Both modes write the same rows to the same table; they differ only in how
// many INSERT statements they issue, which is the variable that decides whether ClickHouse copes.
type Mode string

const (
	// ModePerEvent is the mistake: one INSERT per event. It is what a client that POSTs one event at
	// a time produces, and it is what "ClickHouse cannot take 100k requests/s" actually means -
	// 100k INSERT statements, each becoming at least one part.
	ModePerEvent Mode = "per-event"
	// ModeBatched is the documented answer: at least 5000 rows per insert, or at most one request
	// per second, and let the server coalesce whatever smaller writers remain with async_insert.
	ModeBatched Mode = "batched"
)

type ClickHouse struct {
	BaseURL string
	Table   string
	User    string
	Pass    string
	Mode    Mode
	// BatchRows and BatchWindow are the flush triggers for ModeBatched.
	BatchRows   int
	BatchWindow time.Duration

	client *http.Client
	mu     sync.Mutex
	buf    []ingest.Row
	timer  *time.Timer
	stats  Stats
}

func NewClickHouse(baseURL, table, user, pass string, mode Mode) *ClickHouse {
	return &ClickHouse{
		BaseURL: strings.TrimRight(baseURL, "/"), Table: table, User: user, Pass: pass, Mode: mode,
		BatchRows: 5000, BatchWindow: 2 * time.Second,
		client: &http.Client{Timeout: 60 * time.Second},
	}
}

func (c *ClickHouse) Write(ctx context.Context, rows []ingest.Row) error {
	if c.Mode == ModePerEvent {
		for i := range rows {
			if err := c.insert(ctx, rows[i:i+1], false); err != nil {
				c.record(1, 1)
				return err
			}
			c.record(1, 0)
		}
		return nil
	}
	c.mu.Lock()
	c.buf = append(c.buf, rows...)
	flush := len(c.buf) >= c.BatchRows
	full := make([]ingest.Row, 0, len(c.buf))
	if flush {
		full, c.buf = c.buf, nil
	}
	c.mu.Unlock()
	if flush {
		if err := c.insert(ctx, full, true); err != nil {
			c.record(uint64(len(full)), 1)
			return err
		}
		c.record(uint64(len(full)), 0)
		return nil
	}
	// A partial batch has to leave eventually or a slow client is never measured at all.
	c.mu.Lock()
	if c.timer == nil {
		c.timer = time.AfterFunc(c.BatchWindow, func() { _ = c.Flush(context.Background()) })
	}
	c.mu.Unlock()
	return nil
}

func (c *ClickHouse) Flush(ctx context.Context) error {
	c.mu.Lock()
	rows := c.buf
	c.buf = nil
	c.mu.Unlock()
	if len(rows) == 0 {
		return nil
	}
	err := c.insert(ctx, rows, true)
	if err != nil {
		c.record(uint64(len(rows)), 1)
		return err
	}
	c.record(uint64(len(rows)), 0)
	return nil
}

func (c *ClickHouse) record(rows, errs uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stats.Batches++
	c.stats.Rows += rows
	c.stats.InsertErr += errs
}

func (c *ClickHouse) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

func (c *ClickHouse) Close() error { return nil }

// insert posts rows as JSONEachRow. async_insert is only used in the batched mode: turning it on for
// the per-event mode would quietly fix the mode that exists to be broken, and a benchmark that
// measures something other than what it claims is worse than no benchmark.
func (c *ClickHouse) insert(ctx context.Context, rows []ingest.Row, asyncWait bool) error {
	var body bytes.Buffer
	zw := gzip.NewWriter(&body)
	enc := json.NewEncoder(zw)
	for i := range rows {
		if err := enc.Encode(rows[i]); err != nil {
			return err
		}
	}
	if err := zw.Close(); err != nil {
		return err
	}

	q := url.Values{}
	q.Set("query", fmt.Sprintf("INSERT INTO %s FORMAT JSONEachRow", c.Table))
	q.Set("date_time_input_format", "best_effort")
	if asyncWait {
		q.Set("async_insert", "1")
		// Acknowledging only after the flush means a 200 from this API still means "on disk".
		q.Set("wait_for_async_insert", "1")
	}

	var last error
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/?"+q.Encode(), bytes.NewReader(body.Bytes()))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Encoding", "gzip")
		req.Header.Set("Content-Type", "application/x-ndjson")
		if c.User != "" {
			req.SetBasicAuth(c.User, c.Pass)
		}
		resp, err := c.client.Do(req)
		if err != nil {
			last = err
			time.Sleep(time.Duration(attempt+1) * 200 * time.Millisecond)
			continue
		}
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		if resp.StatusCode/100 == 2 {
			return nil
		}
		last = fmt.Errorf("clickhouse insert: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
		c.mu.Lock()
		c.stats.Retries++
		c.mu.Unlock()
		// 4xx means this batch will never be accepted as it stands. Retrying it forever is what
		// turns one bad row into a stalled pipeline.
		if resp.StatusCode/100 == 4 {
			return last
		}
		time.Sleep(time.Duration(attempt+1) * 200 * time.Millisecond)
	}
	return last
}
