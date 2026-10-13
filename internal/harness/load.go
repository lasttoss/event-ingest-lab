// Package harness is the client side of the experiment: the load generator, and the client-shaped
// behaviour a mobile game actually has - a queue, chunked flushes, retries with jitter, and a clock
// that may be days behind.
package harness

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/lasttoss/event-ingest-lab/internal/ingest"
)

// Mix is the event vocabulary of a casual game, weighted the way an ad-funded one behaves.
var Mix = []struct {
	Name   string
	Weight int
}{
	{"session_start", 10}, {"level_start", 20}, {"level_complete", 14}, {"level_fail", 6},
	{"ad_impression", 30}, {"ad_failed", 8}, {"rewarded_shown", 6}, {"iap_purchase", 1},
	{"economy_gain", 3}, {"economy_spend", 1}, {"cheat_suspect", 1},
}

func pickEvent(r *rand.Rand) string {
	total := 0
	for _, m := range Mix {
		total += m.Weight
	}
	n := r.Intn(total)
	for _, m := range Mix {
		if n < m.Weight {
			return m.Name
		}
		n -= m.Weight
	}
	return Mix[0].Name
}

// Build produces n events for a synthetic game. Deterministic in the seed so two runs are comparable,
// and synthetic on purpose: a real game's logs carry a real player's identifiers.
func Build(seed int64, n int, users, games int, offlineFors []time.Duration) []ingest.Event {
	r := rand.New(rand.NewSource(seed))
	out := make([]ingest.Event, 0, n)
	now := time.Now()
	for i := 0; i < n; i++ {
		user := fmt.Sprintf("u-%06d", r.Intn(users))
		ev := pickEvent(r)
		value := 0.0
		attrs := map[string]string{"level": fmt.Sprint(1 + r.Intn(120)), "ab": fmt.Sprintf("v%d", 1+r.Intn(3))}
		if ev == "iap_purchase" {
			value = []float64{0.99, 1.99, 4.99, 9.99, 19.99}[r.Intn(5)]
			attrs["sku"] = fmt.Sprintf("gems_%d", 1+r.Intn(4))
		}
		if ev == "ad_impression" {
			attrs["placement"] = []string{"inter_middle_level", "rewarded_double", "banner_menu"}[r.Intn(3)]
			attrs["network"] = []string{"AppLovin", "InMobi", "Unity Ads"}[r.Intn(3)]
			value = 0.00001
		}
		// A client that was offline stamps the events with the time they happened, not the time they
		// were finally delivered. A pipeline that loses that distinction destroys every funnel.
		ago := time.Duration(0)
		if len(offlineFors) > 0 {
			ago = offlineFors[i%len(offlineFors)]
		}
		out = append(out, ingest.Event{
			EventID:     fmt.Sprintf("01%024x", i+1),
			TSMillis:    now.Add(-ago).UnixMilli(),
			GameID:      fmt.Sprintf("game-%d", r.Intn(games)),
			Event:       ev,
			UserID:      user,
			DeviceID:    fmt.Sprintf("dev-%08x", r.Intn(users*2)),
			SessionID:   fmt.Sprintf("s-%06d", r.Intn(users*3)),
			Seq:         uint32(i),
			CountryCode: []string{"VN", "US", "BR", "IN", "ID"}[r.Intn(5)],
			Platform:    []string{"AND", "IOS"}[r.Intn(2)],
			AppVersion:  []string{"1.042", "1.041"}[r.Intn(2)],
			Value:       value,
			Attrs:       attrs,
		})
	}
	return out
}

// Client posts chunks the way a game build does, and is the reason the server needs backpressure at
// all: when a million devices come back online, they are all a batch of queued events.
type Client struct {
	URL    string
	Chunk  int
	client *http.Client

	mu       sync.Mutex
	Sent     int
	Accepts  int
	Rejected int
	Refused  int
	Errors   int
	Retries  int
	Bytes    int
}

func NewClient(url string, chunk int) *Client {
	return &Client{URL: url, Chunk: chunk, client: &http.Client{Timeout: 60 * time.Second}}
}

// Flush sends every event, chunk by chunk, in order: sequence within a user has to survive, or the
// funnel is a bag of unordered events.
func (c *Client) Flush(ctx context.Context, events []ingest.Event) error {
	for i := 0; i < len(events); i += c.Chunk {
		end := min(i+c.Chunk, len(events))
		if err := c.sendChunk(ctx, events[i:end]); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) sendChunk(ctx context.Context, chunk []ingest.Event) error {
	var raw bytes.Buffer
	enc := json.NewEncoder(&raw)
	for i := range chunk {
		if err := enc.Encode(chunk[i]); err != nil {
			return err
		}
	}
	var body bytes.Buffer
	zw := gzip.NewWriter(&body)
	if _, err := zw.Write(raw.Bytes()); err != nil {
		return err
	}
	_ = zw.Close()

	backoff := 50 * time.Millisecond
	for attempt := 0; attempt < 8; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL, bytes.NewReader(body.Bytes()))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/x-ndjson")
		req.Header.Set("Content-Encoding", "gzip")
		resp, err := c.client.Do(req)
		if err != nil {
			c.bump(func(s *stats) { s.Errors++ })
			time.Sleep(backoff)
			backoff *= 2
			continue
		}
		c.mu.Lock()
		c.Bytes += body.Len()
		if resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusOK {
			c.Accepts += len(chunk)
		} else if resp.StatusCode == http.StatusServiceUnavailable {
			c.Refused += len(chunk)
			c.Retries++
		} else if resp.StatusCode/100 == 4 {
			c.Rejected += len(chunk)
		} else {
			c.Errors++
		}
		c.Sent += len(chunk)
		c.mu.Unlock()
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusOK || resp.StatusCode/100 == 4 {
			return nil
		}
		// Jitter, because a retry storm is what turns a slow store into a dead one.
		sleep := backoff + time.Duration(rand.Int63n(int64(backoff)+1))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}
		backoff *= 2
	}
	return fmt.Errorf("chunk of %d events never accepted", len(chunk))
}

type stats = struct{ Errors int }

func (c *Client) bump(f func(*stats)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var s stats
	f(&s)
	c.Errors += s.Errors
}

func (c *Client) Report() (sent, accepts, rejected, refused, errors, retries, bytes int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Sent, c.Accepts, c.Rejected, c.Refused, c.Errors, c.Retries, c.Bytes
}
