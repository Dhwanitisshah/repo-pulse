package main

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	ghclient "repo-pulse/ingest/internal/github"
)

// streamName is the Redis Stream the Python api consumes from.
const streamName = "events"

// errorBackoff is used after network errors and unexpected (non-200/304)
// responses, so a flaky repo/token doesn't hot-loop against the API.
const errorBackoff = 30 * time.Second

// repoPoller owns per-repo poll state (ETag, dedup watermark, next-allowed
// poll time) for one watched repository.
type repoPoller struct {
	owner, name string
	fullName    string

	client      *ghclient.Client
	rdb         *redis.Client
	minInterval time.Duration

	etag        string
	lastEventID int64
}

func newRepoPoller(client *ghclient.Client, rdb *redis.Client, fullName string, minInterval time.Duration) *repoPoller {
	owner, name, _ := strings.Cut(fullName, "/")
	return &repoPoller{
		owner:       owner,
		name:        name,
		fullName:    fullName,
		client:      client,
		rdb:         rdb,
		minInterval: minInterval,
	}
}

func (p *repoPoller) run(ctx context.Context) {
	slog.Info("poller starting", "repo", p.fullName, "min_interval", p.minInterval)
	nextPollAt := time.Now()

	for {
		if wait := time.Until(nextPollAt); wait > 0 {
			select {
			case <-ctx.Done():
				slog.Info("poller stopping", "repo", p.fullName)
				return
			case <-time.After(wait):
			}
		}
		if ctx.Err() != nil {
			slog.Info("poller stopping", "repo", p.fullName)
			return
		}

		result, err := p.client.Poll(ctx, p.owner, p.name, p.etag)
		if err != nil {
			slog.Error("poll request failed", "repo", p.fullName, "error", err)
			nextPollAt = time.Now().Add(errorBackoff)
			continue
		}

		interval := p.minInterval
		if result.PollInterval > interval {
			interval = result.PollInterval
		}

		if result.RateLimitRemaining == 0 && !result.RateLimitReset.IsZero() {
			slog.Warn("rate limit exhausted, sleeping until reset",
				"repo", p.fullName, "reset_at", result.RateLimitReset)
			nextPollAt = result.RateLimitReset
			continue
		}

		switch {
		case result.NotModified:
			slog.Info("no new events", "repo", p.fullName, "status", 304)

		case result.StatusCode == http.StatusOK:
			p.etag = result.ETag
			published := p.publishNew(ctx, result.Events)
			slog.Info("poll complete", "repo", p.fullName,
				"new_events", published, "rate_remaining", result.RateLimitRemaining)

		default:
			slog.Error("unexpected response, backing off",
				"repo", p.fullName, "status", result.StatusCode)
			if interval < errorBackoff {
				interval = errorBackoff
			}
		}

		nextPollAt = time.Now().Add(interval)
	}
}

// publishNew XADDs events newer than the highest event ID already published
// for this repo, oldest-first, and advances the dedup watermark. GitHub
// returns events newest-first with monotonically increasing numeric IDs.
func (p *repoPoller) publishNew(ctx context.Context, events []ghclient.Event) int {
	fresh := make([]ghclient.Event, 0, len(events))
	maxID := p.lastEventID

	for _, e := range events {
		id, err := strconv.ParseInt(e.ID, 10, 64)
		if err != nil {
			slog.Warn("skipping event with unparseable id", "repo", p.fullName, "event_id", e.ID)
			continue
		}
		if id <= p.lastEventID {
			continue
		}
		fresh = append(fresh, e)
		if id > maxID {
			maxID = id
		}
	}

	for i := len(fresh) - 1; i >= 0; i-- {
		p.publishOne(ctx, fresh[i])
	}

	p.lastEventID = maxID
	return len(fresh)
}

func (p *repoPoller) publishOne(ctx context.Context, e ghclient.Event) {
	values := map[string]interface{}{
		"source":         "github",
		"event_id":       e.ID,
		"event_type":     e.Type,
		"repo":           p.fullName,
		"actor":          e.Actor.Login,
		"created_at":     e.CreatedAt,
		"ts":             time.Now().UnixMilli(),
		"payload_action": e.Action(),
	}

	if number, merged, ok := e.PullRequest(); ok {
		values["pr_number"] = number
		values["pr_merged"] = merged
	}

	id, err := p.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: streamName,
		Values: values,
	}).Result()
	if err != nil {
		slog.Error("failed to publish event", "repo", p.fullName, "event_id", e.ID, "error", err)
		return
	}
	slog.Info("event published", "repo", p.fullName, "stream_id", id,
		"event_id", e.ID, "event_type", e.Type, "actor", e.Actor.Login)
}
