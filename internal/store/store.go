// Package store is the write path, behind one interface, because the point of this repository is to
// compare write paths.
package store

import (
	"context"

	"github.com/lasttoss/event-ingest-lab/internal/ingest"
)

type Store interface {
	// Write persists a batch. Implementations may buffer; Flush is what makes a measurement honest.
	Write(ctx context.Context, rows []ingest.Row) error
	Flush(ctx context.Context) error
	Stats() Stats
	Close() error
}

type Stats struct {
	Batches   uint64
	Rows      uint64
	InsertErr uint64
	Retries   uint64
}
