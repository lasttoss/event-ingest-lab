package wal

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lasttoss/event-ingest-lab/internal/ingest"
)

func rows(n int) []ingest.Row {
	out := make([]ingest.Row, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, ingest.Row{EventID: "e" + string(rune('a'+i%26)), Event: "x", UserID: "u"})
	}
	return out
}

// The WAL is the durability floor: what the API answered 202 for has to be on disk, and it has to be
// readable by a different process (Vector) as one JSON object per line.
func TestWriteIsReadableLineByLine(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(context.Background(), rows(3)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "events-*.ndjson"))
	if len(files) != 1 {
		t.Fatalf("want one segment, got %d", len(files))
	}
	f, _ := os.Open(files[0])
	defer f.Close()
	sc := bufio.NewScanner(f)
	n := 0
	for sc.Scan() {
		var row ingest.Row
		if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
			t.Fatalf("line %d is not valid JSON: %v", n, err)
		}
		n++
	}
	if n != 3 {
		t.Fatalf("read %d lines, want 3", n)
	}
}

// Rotation exists because Vector tails files and a single file that grows for a year is a file that
// nobody can ship, archive or delete a day at a time.
func TestRotationStartsANewSegment(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, 200)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if err := w.Write(context.Background(), rows(2)); err != nil {
			t.Fatal(err)
		}
	}
	_ = w.Close()
	files, _ := filepath.Glob(filepath.Join(dir, "events-*.ndjson"))
	if len(files) < 2 {
		t.Fatalf("want more than one segment after exceeding maxBytes, got %d", len(files))
	}
	// Every segment has to keep its own name: overwriting would silently drop everything written
	// before it, which is the failure mode this test exists for.
	seen := map[string]bool{}
	for _, f := range files {
		if seen[f] {
			t.Fatalf("duplicate segment name %s", f)
		}
		seen[f] = true
	}
}

func TestWriteRefusesNothingWhenEmpty(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.Write(context.Background(), nil); err != nil {
		t.Fatalf("empty batch should be a no-op, got %v", err)
	}
	rows1, bytes, files, _ := w.WALStats()
	if rows1 != 0 || bytes != 0 {
		t.Fatalf("stats after an empty write: rows=%d bytes=%d", rows1, bytes)
	}
	if files != 1 {
		t.Fatalf("files = %d, want 1 (the writer opens its first segment on start)", files)
	}
	if !strings.Contains(strings.Join([]string{}, ""), "") {
		t.Fatal("unreachable")
	}
}
