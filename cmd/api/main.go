// Command api is the ingest edge: the only thing a game client talks to, and the only place where an
// event is allowed to be wrong.
//
// It answers 202 only after the event is on disk in the WAL. Everything downstream - Vector, the
// parse step, ClickHouse - can be down for hours and no event is lost, which is the property the
// production pipeline this is based on did not have.
package main

import (
	"compress/gzip"
	"context"
	"flag"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lasttoss/event-ingest-lab/internal/ingest"
	"github.com/lasttoss/event-ingest-lab/internal/store"
	"github.com/lasttoss/event-ingest-lab/internal/wal"
)

const maxBody = 8 << 20 // 8 MiB: a client flushing a week offline sends a lot, and one POST is not a plan

func main() {
	var (
		// 8099, not 8080. The first run of this lab talked to a different service that happened to be
		// listening on 8080, and reported a 100% rejection rate in that service's error format. A lab that
		// measures a stranger is worse than a lab that fails to start.
		addr     = flag.String("addr", ":8099", "listen address")
		walDir   = flag.String("wal-dir", "data/wal", "where the write-ahead log lives")
		walBytes = flag.Int64("wal-max-bytes", 64<<20, "rotate a WAL segment after this many bytes")
		queueCap = flag.Int("queue", 20000, "events held in memory before answering 503")
		mode     = flag.String("mode", string(store.ModeBatched), "write path: batched (through the WAL) or per-event (the mistake, kept for measurement)")
		chURL    = flag.String("clickhouse", "", "only for -mode per-event: http://host:8123")
		chTable  = flag.String("table", "lab.events", "table for -mode per-event")
		chUser   = flag.String("clickhouse-user", "lab", "user for -mode per-event")
		chPass   = flag.String("clickhouse-password", "lab", "password for -mode per-event")
	)
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	w, err := wal.NewWriter(*walDir, *walBytes)
	if err != nil {
		log.Error("cannot open the wal", "dir", *walDir, "err", err)
		os.Exit(1)
	}

	var sink store.Store = w
	if *mode == string(store.ModePerEvent) {
		if *chURL == "" {
			log.Error("-mode per-event needs -clickhouse; the point of that mode is that it writes straight to ClickHouse")
			os.Exit(1)
		}
		sink = store.NewClickHouse(*chURL, *chTable, *chUser, *chPass, store.ModePerEvent)
	}

	// Bounded on purpose. An unbounded queue turns a slow store into an OOM kill, and an OOM kill
	// loses exactly the events the queue was holding.
	type delivery struct {
		rows []ingest.Row
		done chan error
	}
	queue := make(chan delivery, *queueCap)

	var (
		accepted  atomic.Uint64
		rejected  atomic.Uint64
		refused   atomic.Uint64
		batches   atomic.Uint64
		drainErrs atomic.Uint64
	)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// One writer goroutine: the WAL has one owner, and ClickHouse gets batches rather than a crowd.
	go func() {
		for {
			select {
			case <-ctx.Done():
				_ = sink.Flush(context.Background())
				_ = sink.Close()
				_ = w.Close()
				return
			case d := <-queue:
				err := sink.Write(ctx, d.rows)
				if err == nil {
					err = sink.Flush(ctx)
				}
				if err != nil {
					drainErrs.Add(1)
					log.Error("write failed", "rows", len(d.rows), "err", err)
				}
				batches.Add(1)
			}
		}
	}()

	mux := http.NewServeMux()
	// The mode is in the health response because the harness has to be able to prove it is talking
	// to the API it started. The first run of this lab was not: port 8099 was occupied by a leftover
	// process, the arm meant to measure per-event inserts measured the batched path instead, and the
	// numbers looked fine. A harness that cannot tell which service answered it is not a harness.
	mux.HandleFunc("/healthz", func(rw http.ResponseWriter, r *http.Request) {
		rows, bytes, files, _ := w.WALStats()
		_, _ = rw.Write([]byte("ok mode=" + *mode + " pid=" + itoa(os.Getpid()) +
			" wal_rows=" + itoa(rows) + " wal_bytes=" + itoa(bytes) + " wal_files=" + itoa(uint64(files)) + "\n"))
	})
	mux.HandleFunc("/metrics", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "text/plain; version=0.0.4")
		writeMetrics(rw, accepted.Load(), rejected.Load(), refused.Load(), batches.Load(), drainErrs.Load(), len(queue))
	})
	mux.HandleFunc("/v1/events", func(rw http.ResponseWriter, r *http.Request) {
		start := time.Now()
		if r.Method != http.MethodPost {
			http.Error(rw, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var body io.Reader = http.MaxBytesReader(rw, r.Body, maxBody)
		// Clients compress because they are on a phone paying for data.
		if r.Header.Get("Content-Encoding") == "gzip" {
			zr, err := gzip.NewReader(body)
			if err != nil {
				http.Error(rw, `{"error":"bad gzip body"}`, http.StatusBadRequest)
				return
			}
			defer zr.Close()
			body = zr
		}
		raw, err := io.ReadAll(body)
		if err != nil {
			http.Error(rw, `{"error":"body too large or unreadable"}`, http.StatusRequestEntityTooLarge)
			return
		}
		rows, problems, err := ingest.Decode(raw)
		if err != nil {
			rejected.Add(1)
			http.Error(rw, `{"error":"`+err.Error()+`"}`, http.StatusBadRequest)
			return
		}
		if len(rows) == 0 {
			rejected.Add(uint64(len(problems)))
			writeJSON(rw, http.StatusOK, map[string]any{"accepted": 0, "rejected": len(problems), "problems": problems})
			return
		}
		select {
		case queue <- delivery{rows: rows}:
			accepted.Add(uint64(len(rows)))
			rejected.Add(uint64(len(problems)))
			// 202, not 200: it is durable, it is not yet queryable. Saying 200 would hide the
			// difference between "we have it" and "you can read it", and clients deserve to know.
			writeJSON(rw, http.StatusAccepted, map[string]any{
				"accepted": len(rows), "rejected": len(problems), "problems": problems,
				"took_ms": time.Since(start).Milliseconds(),
			})
		default:
			// Backpressure beats silence: the client keeps its own queue, retries with jitter, and
			// nothing is lost on either side.
			refused.Add(uint64(len(rows)))
			rw.Header().Set("Retry-After", "2")
			writeJSON(rw, http.StatusServiceUnavailable, map[string]any{"error": "ingest queue is full", "retry_after_s": 2})
		}
	})

	srv := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		sh, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(sh)
	}()
	log.Info("ingest api listening", "addr", *addr, "mode", *mode, "wal_dir", *walDir, "queue", *queueCap)
	// A bind failure has to be fatal and loud. Continuing would leave the harness talking to whatever
	// already owned the port, which is exactly how the first version of this experiment produced a
	// confident, wrong result.
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Error("cannot bind - something else owns this port, and measuring it would be measuring a stranger", "addr", *addr, "err", err)
		os.Exit(1)
	}
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		log.Error("server stopped", "err", err)
		os.Exit(1)
	}
}
