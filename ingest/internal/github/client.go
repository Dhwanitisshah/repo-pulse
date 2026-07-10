// Package github is a minimal stdlib-only client for the GitHub REST
// "list public events for a repository" endpoint, built for long-running
// polling: it honors ETags, rate-limit headers, and the Poll-Interval hint.
package github

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

const apiVersion = "2022-11-28"

// Actor is the GitHub user who triggered an event.
type Actor struct {
	Login string `json:"login"`
}

// Repo identifies the repository an event belongs to.
type Repo struct {
	Name string `json:"name"` // "owner/name"
}

// Event is the subset of the GitHub events API payload we care about.
type Event struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Actor     Actor           `json:"actor"`
	Repo      Repo            `json:"repo"`
	CreatedAt string          `json:"created_at"`
	Payload   json.RawMessage `json:"payload"`
}

// Action returns the payload's "action" field (e.g. "opened", "closed") if
// present, else "". Most GitHub event payloads that represent a state
// transition carry this field; events like PushEvent or WatchEvent do not.
func (e Event) Action() string {
	if len(e.Payload) == 0 {
		return ""
	}
	var p struct {
		Action string `json:"action"`
	}
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		return ""
	}
	return p.Action
}

// Client polls the GitHub repository events endpoint.
type Client struct {
	httpClient *http.Client
	token      string
}

// NewClient builds a Client authenticating with a GitHub token.
func NewClient(token string) *Client {
	return &Client{
		httpClient: &http.Client{Timeout: 15 * time.Second},
		token:      token,
	}
}

// PollResult carries the parsed events plus the response metadata a caller
// needs to schedule the next poll and stay a good API citizen.
type PollResult struct {
	StatusCode int
	NotModified bool
	Events      []Event

	ETag string // send back as If-None-Match next time

	// PollInterval is the server-suggested minimum seconds between polls of
	// this endpoint (GitHub's "X-Poll-Interval" header). Zero if absent.
	PollInterval time.Duration

	RateLimitRemaining int
	RateLimitReset     time.Time // zero if header absent
}

// Poll fetches GET /repos/{owner}/{name}/events. Pass the ETag from the
// previous PollResult (empty string on the first call). A network/transport
// error is returned as err; HTTP-level errors (403/404/5xx) are returned in
// PollResult.StatusCode with err == nil so callers can log and back off
// without crashing.
func (c *Client) Poll(ctx context.Context, owner, name, etag string) (*PollResult, error) {
	url := fmt.Sprintf("https://api.github.com/repos/%s/%s/events", owner, name)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("building request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", apiVersion)
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("performing request: %w", err)
	}
	defer resp.Body.Close()

	result := &PollResult{
		StatusCode: resp.StatusCode,
		ETag:       resp.Header.Get("ETag"),
	}

	if seconds, ok := parseInt(resp.Header.Get("X-Poll-Interval")); ok {
		result.PollInterval = time.Duration(seconds) * time.Second
	}
	if remaining, ok := parseInt(resp.Header.Get("X-RateLimit-Remaining")); ok {
		result.RateLimitRemaining = remaining
	} else {
		result.RateLimitRemaining = -1 // unknown, don't trigger sleep logic
	}
	if reset, ok := parseInt(resp.Header.Get("X-RateLimit-Reset")); ok {
		result.RateLimitReset = time.Unix(int64(reset), 0)
	}

	switch resp.StatusCode {
	case http.StatusNotModified:
		result.NotModified = true
		// A 304 does not consume rate limit and has no body to read.
		return result, nil
	case http.StatusOK:
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("reading response body: %w", err)
		}
		var events []Event
		if err := json.Unmarshal(body, &events); err != nil {
			return nil, fmt.Errorf("decoding events: %w", err)
		}
		result.Events = events
		return result, nil
	default:
		// 403 (rate limited / forbidden), 404 (repo gone/renamed), 5xx, etc.
		// Not an error we return to the caller as `err` -- the caller decides
		// how to log/back off based on StatusCode.
		return result, nil
	}
}

func parseInt(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, false
	}
	return n, true
}
