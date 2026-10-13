// Package ingest is the contract. Everything a client sends is untrusted: a mobile client can be a
// modified build, a replayed request, or a device with a clock set to 2031.
package ingest

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

const (
	MaxAttrs      = 25  // the same ceiling Firebase Analytics documents per event
	MaxEventName  = 40  // documented Firebase limit for event names
	MaxValueLen   = 100 // documented Firebase limit for parameter values
	MaxSkewFuture = 5 * time.Minute
	MaxAge        = 400 * 24 * time.Hour
)

// Event is the wire format. Deliberately flat and boring: the client SDKs are the hard part, this
// is what they all have to agree on.
type Event struct {
	EventID     string            `json:"event_id"`
	TSMillis    int64             `json:"ts_ms"`
	GameID      string            `json:"game_id"`
	Event       string            `json:"event"`
	UserID      string            `json:"user_id"`
	DeviceID    string            `json:"device_id"`
	SessionID   string            `json:"session_id"`
	Seq         uint32            `json:"seq"`
	CountryCode string            `json:"country_code"`
	Platform    string            `json:"platform"`
	AppVersion  string            `json:"app_version"`
	Value       float64           `json:"value"`
	Attrs       map[string]string `json:"attrs"`
}

// Problem is a rejection that names the index and the reason, so a client can fix it instead of
// guessing. A silent 200 for a dropped event is how a pipeline loses a week of data unnoticed.
type Problem struct {
	Index  int    `json:"index"`
	Field  string `json:"field,omitempty"`
	Reason string `json:"reason"`
}

func (p Problem) Error() string { return fmt.Sprintf("event %d: %s", p.Index, p.Reason) }

// Validate checks one event against the contract. It is called per event rather than per batch so
// that one bad event cannot fail the batch it arrived in.
func (e *Event) Validate(i int, now time.Time) *Problem {
	bad := func(field, reason string) *Problem {
		return &Problem{Index: i, Field: field, Reason: reason}
	}
	switch {
	case e.EventID == "" || len(e.EventID) > 64:
		return bad("event_id", "required, at most 64 characters - the client generates it so retries can be deduplicated")
	case e.GameID == "" || len(e.GameID) > 64:
		return bad("game_id", "required, at most 64 characters")
	case e.Event == "":
		return bad("event", "required")
	case len(e.Event) > MaxEventName:
		return bad("event", fmt.Sprintf("at most %d characters", MaxEventName))
	case e.UserID == "" || len(e.UserID) > 128:
		return bad("user_id", "required, at most 128 characters")
	case e.TSMillis <= 0:
		return bad("ts_ms", "required, epoch milliseconds")
	}
	if len(e.Attrs) > MaxAttrs {
		return bad("attrs", fmt.Sprintf("at most %d keys", MaxAttrs))
	}
	for k, v := range e.Attrs {
		if len(k) == 0 || len(k) > MaxEventName {
			return bad("attrs", fmt.Sprintf("key %q must be 1..%d characters", k, MaxEventName))
		}
		if len(v) > MaxValueLen {
			return bad("attrs."+k, fmt.Sprintf("value at most %d characters", MaxValueLen))
		}
	}
	// The clock is the client's, and some of them are wrong. A device that is offline for a week is
	// normal and must be accepted; a device that claims tomorrow is not.
	ts := time.UnixMilli(e.TSMillis)
	if ts.After(now.Add(MaxSkewFuture)) {
		return bad("ts_ms", "in the future beyond the skew window - a wrong device clock, not a prediction")
	}
	if ts.Before(now.Add(-MaxAge)) {
		return bad("ts_ms", "older than the retention window")
	}
	if e.Value < 0 {
		return bad("value", "must not be negative")
	}
	return nil
}

// EventTime is the client's timestamp as a time, formatted the way ClickHouse's best-effort parser
// expects it.
func (e *Event) EventTime() time.Time { return time.UnixMilli(e.TSMillis) }

func (e *Event) EventTimeSQL() string {
	return e.EventTime().UTC().Format("2006-01-02 15:04:05.000")
}

// Row is the flat shape written to the WAL and to ClickHouse. attrs stays a map; everything a
// dashboard filters on gets its own column.
type Row struct {
	EventID     string            `json:"event_id"`
	EventTime   string            `json:"event_time"`
	GameID      string            `json:"game_id"`
	Event       string            `json:"event"`
	UserID      string            `json:"user_id"`
	DeviceID    string            `json:"device_id"`
	SessionID   string            `json:"session_id"`
	Seq         uint32            `json:"seq"`
	CountryCode string            `json:"country_code"`
	Platform    string            `json:"platform"`
	AppVersion  string            `json:"app_version"`
	Value       float64           `json:"value"`
	Attrs       map[string]string `json:"attrs"`
}

func (e *Event) Row() Row {
	attrs := e.Attrs
	if attrs == nil {
		attrs = map[string]string{}
	}
	return Row{
		EventID: e.EventID, EventTime: e.EventTimeSQL(), GameID: e.GameID, Event: e.Event,
		UserID: e.UserID, DeviceID: e.DeviceID, SessionID: e.SessionID, Seq: e.Seq,
		CountryCode: e.CountryCode, Platform: e.Platform, AppVersion: e.AppVersion,
		Value: e.Value, Attrs: attrs,
	}
}

// Decode accepts either a JSON array or newline-delimited JSON, and reports which events were
// rejected without throwing away the ones that were fine.
func Decode(body []byte) (accepted []Row, problems []*Problem, err error) {
	body = []byte(strings.TrimSpace(string(body)))
	if len(body) == 0 {
		return nil, nil, errors.New("empty body")
	}
	var events []Event
	if body[0] == '[' {
		if err := json.Unmarshal(body, &events); err != nil {
			return nil, nil, fmt.Errorf("json array: %w", err)
		}
	} else {
		dec := json.NewDecoder(strings.NewReader(string(body)))
		for {
			var e Event
			if err := dec.Decode(&e); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return nil, nil, fmt.Errorf("ndjson line: %w", err)
			}
			events = append(events, e)
		}
	}
	now := time.Now()
	for i := range events {
		if p := events[i].Validate(i, now); p != nil {
			problems = append(problems, p)
			continue
		}
		accepted = append(accepted, events[i].Row())
	}
	return accepted, problems, nil
}
