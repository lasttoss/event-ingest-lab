package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// Hand-rolled Prometheus text: no dependency, and every counter here is one this pipeline is
// measured by. A metric nobody reads is a metric that lies.
func writeMetrics(w http.ResponseWriter, accepted, rejected, refused, batches, drainErrs uint64, queueDepth int) {
	var b strings.Builder
	metricLine(&b, "ingest_accepted_events_total", "events accepted and written to the store", accepted)
	metricLine(&b, "ingest_rejected_events_total", "events refused by validation, with a reason", rejected)
	metricLine(&b, "ingest_refused_events_total", "events answered 503 because the queue was full", refused)
	metricLine(&b, "ingest_batches_total", "batches handed to the store", batches)
	metricLine(&b, "ingest_drain_errors_total", "batches the store refused", drainErrs)
	fmt.Fprintf(&b, "# HELP ingest_queue_depth events waiting for the writer\n# TYPE ingest_queue_depth gauge\ningest_queue_depth %d\n", queueDepth)
	_, _ = w.Write([]byte(b.String()))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func itoa(v any) string { return fmt.Sprintf("%d", v) }

func metricLine(b *strings.Builder, name, help string, value uint64) {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, value)
}
