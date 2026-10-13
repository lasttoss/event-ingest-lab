// Package wal is the durability floor of the pipeline: the API answers 202 only after the event is
// on disk, so a ClickHouse outage, a Vector restart or a pod kill costs nothing.
package wal

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/lasttoss/event-ingest-lab/internal/ingest"
	"github.com/lasttoss/event-ingest-lab/internal/store"
)

type Writer struct {
	dir       string
	maxBytes  int64
	mu        sync.Mutex
	file      *os.File
	size      int64
	written   uint64
	rotations uint64
	now       func() time.Time
}

func NewWriter(dir string, maxBytes int64) (*Writer, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	w := &Writer{dir: dir, maxBytes: maxBytes, now: time.Now}
	if err := w.rotateLocked(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *Writer) rotateLocked() error {
	if w.file != nil {
		_ = w.file.Sync()
		_ = w.file.Close()
		w.rotations++
	}
	name := fmt.Sprintf("events-%d-%06d.ndjson", w.now().Unix(), w.rotations)
	f, err := os.OpenFile(filepath.Join(w.dir, name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.file, w.size = f, 0
	return nil
}

// Write makes the WAL a store.Store, so the API has one delivery path whether it is writing to disk
// or straight into ClickHouse - the comparison between the two modes has to differ in the store, not
// in the handler.
func (w *Writer) Write(ctx context.Context, rows []ingest.Row) error {
	if len(rows) == 0 {
		return nil
	}
	encoded := make([]json.RawMessage, 0, len(rows))
	for i := range rows {
		b, err := json.Marshal(rows[i])
		if err != nil {
			return err
		}
		encoded = append(encoded, b)
	}
	return w.Append(encoded)
}

// Flush syncs the segment. Vector reads completed lines, so a crash between appends costs at most the
// rows of one batch that was never acknowledged with a 202.
func (w *Writer) Flush(context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	return w.file.Sync()
}

func (w *Writer) Stats() store.Stats {
	rows, bytes, files, _ := w.WALStats()
	return store.Stats{Batches: uint64(files), Rows: rows + uint64(bytes)}
}

// Append writes rows as newline-delimited JSON. One write per batch, not per row: the file is the
// handoff to Vector, and a partial line is recoverable while a partial batch is not.
func (w *Writer) Append(rows []json.RawMessage) error {
	if len(rows) == 0 {
		return nil
	}
	var buf []byte
	for _, r := range rows {
		buf = append(buf, r...)
		buf = append(buf, '\n')
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.size+int64(len(buf)) > w.maxBytes {
		if err := w.rotateLocked(); err != nil {
			return err
		}
	}
	n, err := w.file.Write(buf)
	w.size += int64(n)
	w.written += uint64(len(rows))
	return err
}

// Close flushes what a container kill would otherwise lose. It is called from the HTTP handler for
// SIGTERM, not from a defer that a panic can skip.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Sync()
	_ = w.file.Close()
	w.file = nil
	return err
}

// WALStats is about the log on disk; Stats is the store.Store contract. Two different questions,
// so they get two names.
func (w *Writer) WALStats() (rows uint64, bytes int64, files int, rotations uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	list, _ := filepath.Glob(filepath.Join(w.dir, "events-*.ndjson"))
	sort.Strings(list)
	return w.written, w.size, len(list), w.rotations
}
