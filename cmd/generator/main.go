// Command generator is the load generator, and it is also a stand-in for the Unity client: it can run
// at a steady rate, or hold events for days and then deliver them all at once.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/lasttoss/event-ingest-lab/internal/harness"
)

func main() {
	var (
		api     = flag.String("api", "http://127.0.0.1:8099/v1/events", "ingest endpoint")
		total   = flag.Int("events", 100000, "how many events to generate")
		rate    = flag.Int("rate", 0, "events per second; 0 means as fast as the API accepts")
		users   = flag.Int("users", 5000, "distinct synthetic players")
		games   = flag.Int("games", 3, "distinct synthetic games")
		chunk   = flag.Int("chunk", 200, "events per POST - one event per request is the mistake being measured")
		seed    = flag.Int64("seed", 7, "deterministic")
		offline = flag.Duration("offline-for", 0, "stamp events as if the device had been offline this long")
	)
	flag.Parse()

	forDays := []time.Duration{}
	if *offline > 0 {
		forDays = []time.Duration{*offline, *offline / 2, 0}
	}
	events := harness.Build(*seed, *total, *users, *games, forDays)
	c := harness.NewClient(*api, *chunk)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	start := time.Now()
	if *rate <= 0 {
		if err := c.Flush(ctx, events); err != nil {
			fmt.Fprintln(os.Stderr, "flush:", err)
			os.Exit(1)
		}
	} else {
		ticker := time.NewTicker(time.Second / time.Duration(*rate))
		defer ticker.Stop()
		i := 0
		for i < len(events) {
			<-ticker.C
			end := min(i+*rate, len(events)) // one second's worth per tick
			if err := c.Flush(ctx, events[i:end]); err != nil {
				fmt.Fprintln(os.Stderr, "flush:", err)
				os.Exit(1)
			}
			i = end
		}
	}
	elapsed := time.Since(start)
	sent, accepts, rejected, refused, errors, retries, bytes := c.Report()
	fmt.Printf("sent=%d accepted=%d rejected=%d refused503=%d errors=%d retries=%d bytes=%d elapsed=%s rate=%.0f ev/s\n",
		sent, accepts, rejected, refused, errors, retries, bytes, elapsed.Round(time.Millisecond),
		float64(accepts)/elapsed.Seconds())
}
