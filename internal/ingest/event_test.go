package ingest

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

func valid(now time.Time) Event {
	return Event{
		EventID: "01abc", TSMillis: now.UnixMilli(), GameID: "game-0", Event: "level_complete",
		UserID: "u-1", DeviceID: "d-1", SessionID: "s-1", Seq: 1, CountryCode: "VN",
		Platform: "AND", AppVersion: "1.042", Value: 0, Attrs: map[string]string{"level": "3"},
	}
}

// The cases that matter are the ones a real client produces: a device with a wrong clock, an event
// with too many parameters, and the batch that mixes a good event with a bad one. The last one is the
// reason validation is per event: one bad row must not take the batch down with it.
func TestValidate(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name    string
		mutate  func(*Event)
		wantErr bool
		field   string
	}{
		{"valid", func(*Event) {}, false, ""},
		{"no event_id", func(e *Event) { e.EventID = "" }, true, "event_id"},
		{"no event name", func(e *Event) { e.Event = "" }, true, "event"},
		{"event name too long", func(e *Event) { e.Event = strings.Repeat("x", MaxEventName+1) }, true, "event"},
		{"no user", func(e *Event) { e.UserID = "" }, true, "user_id"},
		// A device that claims 2031 is a wrong clock, not a prediction. This is the case the
		// production data actually contained.
		{"clock set to 2031", func(e *Event) { e.TSMillis = time.Date(2031, 9, 29, 17, 50, 34, 0, time.UTC).UnixMilli() }, true, "ts_ms"},
		{"no timestamp", func(e *Event) { e.TSMillis = 0 }, true, "ts_ms"},
		// Offline for a week is normal and must be accepted.
		{"offline for a week", func(e *Event) { e.TSMillis = now.Add(-7 * 24 * time.Hour).UnixMilli() }, false, ""},
		{"beyond the retention window", func(e *Event) { e.TSMillis = now.Add(-MaxAge - time.Hour).UnixMilli() }, true, "ts_ms"},
		{"too many attrs", func(e *Event) {
			e.Attrs = map[string]string{}
			for i := 0; i <= MaxAttrs; i++ {
				e.Attrs[string(rune('a'+i%26))+strings.Repeat("k", i%5)] = "v"
			}
		}, true, "attrs"},
		{"value overlong", func(e *Event) { e.Attrs = map[string]string{"k": strings.Repeat("v", MaxValueLen+1)} }, true, "attrs.k"},
		{"negative value", func(e *Event) { e.Value = -1 }, true, "value"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := valid(now)
			c.mutate(&e)
			p := e.Validate(0, now)
			if c.wantErr && p == nil {
				t.Fatalf("expected a problem, got none")
			}
			if !c.wantErr && p != nil {
				t.Fatalf("expected no problem, got %v", p)
			}
			if c.wantErr && c.field != "" && p.Field != c.field {
				t.Fatalf("problem named field %q, want %q", p.Field, c.field)
			}
		})
	}
}

func TestDecodeKeepsTheGoodEventsOfABadBatch(t *testing.T) {
	now := time.Now().UnixMilli()
	body := strings.Join([]string{
		`{"event_id":"a","ts_ms":` + itoa(now) + `,"game_id":"g","event":"session_start","user_id":"u"}`,
		`{"event_id":"","ts_ms":` + itoa(now) + `,"game_id":"g","event":"session_start","user_id":"u"}`,
		`{"event_id":"c","ts_ms":` + itoa(now) + `,"game_id":"g","event":"level_complete","user_id":"u"}`,
	}, "\n")
	accepted, problems, err := Decode([]byte(body))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(accepted) != 2 {
		t.Fatalf("accepted %d events, want 2 - a bad row must not reject the batch", len(accepted))
	}
	if len(problems) != 1 {
		t.Fatalf("problems = %d, want 1", len(problems))
	}
	if problems[0].Index != 1 {
		t.Fatalf("problem index = %d, want 1 - the client needs to know which row to fix", problems[0].Index)
	}
}

func TestDecodeArrayAndEmptyBody(t *testing.T) {
	now := time.Now().UnixMilli()
	if _, _, err := Decode([]byte("   ")); err == nil {
		t.Fatal("empty body should be an error")
	}
	arr := `[{"event_id":"a","ts_ms":` + itoa(now) + `,"game_id":"g","event":"x","user_id":"u"}]`
	accepted, _, err := Decode([]byte(arr))
	if err != nil || len(accepted) != 1 {
		t.Fatalf("json array: accepted=%d err=%v", len(accepted), err)
	}
}

func TestRowCarriesBothTimes(t *testing.T) {
	now := time.Now()
	e := valid(now)
	e.TSMillis = now.Add(-3 * 24 * time.Hour).UnixMilli()
	row := e.Row()
	if row.EventTime == "" {
		t.Fatal("event_time must be set: a client that was offline for three days still says when it happened")
	}
	if !strings.HasPrefix(row.EventTime, time.Now().Add(-3*24*time.Hour).UTC().Format("2006-01-02")) {
		t.Fatalf("event_time = %q, want the day the event happened", row.EventTime)
	}
	if row.Attrs == nil {
		t.Fatal("attrs must never be nil: ClickHouse's Map column wants an object")
	}
}

// The wire format carries epoch milliseconds as a number, not a formatted date: a phone in a
// basement should not have to know what a timezone is to report an event.
func itoa(v int64) string { return strconv.FormatInt(v, 10) }
