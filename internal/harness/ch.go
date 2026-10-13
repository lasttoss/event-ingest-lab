package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// CH is a tiny ClickHouse HTTP client. No driver, because the three facts this repository needs are
// count, parts and elapsed time, and a dependency for that would be a dependency for nothing.
type CH struct {
	BaseURL string
	User    string
	Pass    string
	client  *http.Client
}

func NewCH(baseURL, user, pass string) *CH {
	return &CH{BaseURL: strings.TrimRight(baseURL, "/"), User: user, Pass: pass,
		client: &http.Client{Timeout: 120 * time.Second}}
}

func (c *CH) Exec(ctx context.Context, sql string) error {
	_, _, err := c.do(ctx, sql)
	return err
}

// Scalar runs a query expected to return exactly one row of one column, and reports how long
// ClickHouse itself says it took - server time, not round trip.
//
// JSONCompact, not JSON: the plain JSON format returns each row as an object keyed by column name,
// which a reader that expects positional columns silently mangles - it took a run of the experiment
// to notice, because the failure looked like a query error rather than a parsing one.
func (c *CH) Scalar(ctx context.Context, sql string) (string, time.Duration, error) {
	body, stat, err := c.do(ctx, sql+" FORMAT JSONCompact")
	if err != nil {
		return "", stat, err
	}
	var parsed struct {
		Data       [][]any `json:"data"`
		Statistics struct {
			Elapsed float64 `json:"elapsed"`
		} `json:"statistics"`
		Exception string `json:"exception"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", stat, fmt.Errorf("decode %q: %w", truncate(string(body), 300), err)
	}
	if parsed.Exception != "" {
		return "", stat, errors.New(truncate(parsed.Exception, 300))
	}
	out := ""
	if len(parsed.Data) > 0 && len(parsed.Data[0]) > 0 {
		out = fmt.Sprintf("%v", parsed.Data[0][0])
	}
	return out, time.Duration(parsed.Statistics.Elapsed * float64(time.Second)), nil
}

func (c *CH) Rows(ctx context.Context, sql string) ([][]any, time.Duration, error) {
	body, stat, err := c.do(ctx, sql+" FORMAT JSONCompact")
	if err != nil {
		return nil, stat, err
	}
	var parsed struct {
		Data       [][]any `json:"data"`
		Statistics struct {
			Elapsed float64 `json:"elapsed"`
		} `json:"statistics"`
		Exception string `json:"exception"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, stat, fmt.Errorf("decode %q: %w", truncate(string(body), 300), err)
	}
	if parsed.Exception != "" {
		return nil, stat, errors.New(truncate(parsed.Exception, 300))
	}
	return parsed.Data, time.Duration(parsed.Statistics.Elapsed * float64(time.Second)), nil
}

func (c *CH) do(ctx context.Context, sql string) ([]byte, time.Duration, error) {
	start := time.Now()
	q := url.Values{}
	q.Set("date_time_input_format", "best_effort")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/?"+q.Encode(), bytes.NewReader([]byte(sql)))
	if err != nil {
		return nil, 0, err
	}
	if c.User != "" {
		req.SetBasicAuth(c.User, c.Pass)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, time.Since(start), err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode/100 != 2 {
		return nil, time.Since(start), fmt.Errorf("clickhouse %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return body, time.Since(start), nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
