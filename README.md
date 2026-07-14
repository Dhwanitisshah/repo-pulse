# repo-pulse

A Go → Redis Streams → Python/FastAPI → React pipeline. The Go `ingest`
service polls the GitHub Events API for a configured list of repositories and
publishes normalized events onto a Redis Stream, the Python `api` service
consumes the stream, aggregates windowed counts and PR merge-time stats, and
exposes it all over HTTP, and the React `frontend` polls the API and shows a
live dashboard.

## GitHub polling (`ingest`)

`ingest` runs one poller goroutine per watched repo against
`GET /repos/{owner}/{name}/events`. It is a well-behaved API citizen:

- Uses conditional requests (`ETag` / `If-None-Match`) — a `304` costs no
  rate limit and publishes nothing.
- Reads `X-RateLimit-Remaining` / `X-RateLimit-Reset` and sleeps until reset
  if the budget hits zero.
- Honors the `X-Poll-Interval` response header as a floor on top of
  `POLL_INTERVAL_SECONDS`.
- Dedupes by tracking the highest GitHub event ID already published per repo,
  so restarts and overlapping pages don't republish events.

Each event is normalized before being added to the `events` stream:

```
source, event_id, event_type, repo, actor, created_at, ts, payload_action
```

For `PullRequestEvent` only, two extra fields are added so the API can
correlate a PR's open and close: `pr_number` and `pr_merged` (the latter is
only meaningful when `payload_action` is `closed`). No other event type
carries these fields.

### Env vars

| Var | Required | Default | Notes |
|---|---|---|---|
| `REDIS_ADDR` | no | `localhost:6379` | |
| `GITHUB_TOKEN` | **yes** | — | A GitHub token with public-repo read scope (a fine-grained or classic PAT works). `ingest` fails fast at startup if unset. |
| `WATCHED_REPOS` | **yes** | — | Comma-separated `owner/name` list, e.g. `pallets/flask,psf/requests,fastapi/fastapi`. |
| `POLL_INTERVAL_SECONDS` | no | `15` | Floor between polls of a given repo; GitHub's `Poll-Interval` header can push it higher. |

## Stream consumption (`api`): consumer groups

The API consumes the `events` stream via a Redis **consumer group**
(`XREADGROUP`), not a plain `XREAD`, so consumption is resumable and gives
**at-least-once delivery** across restarts/crashes.

- **`XGROUP CREATE ... MKSTREAM`** — on startup the API creates its consumer
  group (`CONSUMER_GROUP`, default `pulse-workers`) if it doesn't already
  exist (a `BUSYGROUP` error just means it's already there). It's created at
  id `"0"`, not `"$"`, so a brand-new group backfills from the start of the
  stream instead of only seeing events published after it was created.
- **Two-phase read loop** — every consumer (`CONSUMER_NAME`, default the
  container `HOSTNAME` or a random suffix) reads in two phases:
  1. **Pending first**: `XREADGROUP ... streams {events: "0"}` asks Redis for
     *this consumer's own already-delivered-but-unacked* messages. This
     drains anything left over from a previous crash before touching new
     work — that's the crash-recovery step.
  2. **Then new**: `XREADGROUP ... streams {events: ">"}` reads
     never-delivered messages, blocking up to 5s at a time.
- **`XACK`** — a message is only acknowledged *after* it's been appended to
  the in-memory recent-events buffer. If the process dies between delivery
  and ack, the message stays in the group's pending entries list and gets
  redelivered to this same consumer on restart (phase 1 above). Messages are
  never acked before they're processed, which is what makes delivery
  at-least-once rather than at-most-once.
- **`XPENDING`** — exposed via `GET /stream/health`, which returns stream
  length, the group/consumer names, and a pending-entries summary (count,
  id range, per-consumer breakdown) so the guarantee is visible without
  reaching for `redis-cli`.

### Demo: crash recovery

```bash
# 1. Start redis + api, let a few events flow in.
# 2. Check /stream/health — pending count should be 0 (everything acked).
curl http://localhost:8000/stream/health

# 3. Kill the api process mid-stream (Ctrl+C or `docker compose kill api`)
#    right after some events are logged as consumed but before you'd expect
#    the next ack cycle — or simpler: stop redis's network briefly so xack
#    calls fail while xreadgroup still succeeds.
# 4. Restart the api. On boot it drains its own pending entries (phase 1)
#    before reading anything new — check the logs for "consumed event ..."
#    lines appearing again for messages that were in flight.
# 5. Confirm recovery: /stream/health should show pending back at 0 and
#    /events/recent should include the recovered events.
curl http://localhost:8000/stream/health
curl http://localhost:8000/events/recent
```

## Windowed aggregation (`api`): tumbling minute buckets

As each event is consumed and acked (see above), `record_event` increments a
few Redis counters keyed by **minute bucket** (`ts // 60000`):

- `stats:count:{repo}:{bucket}` — per-repo total for that minute
- `stats:type:{repo}:{event_type}:{bucket}` — per-repo, per-type
- `stats:global:{event_type}:{bucket}` — global per-type
- `stats:repos` / `stats:event_types` — small sets tracking everything seen,
  so the reader knows what to look up

All three counters (plus their `EXPIRE`) go out in a single pipeline per
event — one round trip. Every bucket key expires **2 hours** after its last
write, well past the widest supported query window, so old buckets self-clean
without a separate cleanup job.

`GET /stats?window={5,15,60}` (default 15) reads the last N one-minute
buckets and sums them in Python via batched `MGET`s (no N+1 Redis calls),
returning per-repo counts, per-type counts, a per-repo-by-type breakdown, and
a `timeline` (per-minute totals across the window) for the frontend's
sparkline. Aggregates are **only** computed from these Redis counters, never
from the in-memory `recent_events` deque — that deque is a capped display
buffer for `/events/recent`, not a source of truth.

## PR merge-time tracking (`api`): stateful open→merge correlation

Unlike the 3a counts above, this is **correlational, not a simple counter**:
a PR's `opened` event and its `closed` (merged) event can be hours or days
apart, so the API has to remember in-flight PRs between the two.

- `pr:open:{repo}` — a Redis hash of `{pr_number: open_ts_ms}` for PRs
  currently in flight.
- On `opened`: `HSET` the PR into that hash.
- On `closed` with `merged=true`: `HGET` the open timestamp.
  - **Found** → compute the duration, `ZADD` it into
    `pr:durations:{repo}` (score = duration in seconds, so `ZRANGE
    ...WITHSCORES` gives sorted durations for percentile math), then
    `HDEL` the in-flight entry.
  - **Not found** → increment `pr:unmatched:{repo}` and move on. See below —
    this is expected, not an error.
- On `closed` with `merged=false`: just `HDEL` the in-flight entry (a closed,
  unmerged PR isn't a "merge time").
- `pr:durations:{repo}` is capped at ~500 entries via `ZREMRANGEBYRANK` so it
  can't grow unbounded.

`GET /stats/pr` returns, per repo: `merged_count`, `median_merge_seconds` /
`_hours`, `p90_merge_seconds` / `_hours`, `fastest`/`slowest`, plus
`unmatched_merges` and `open_prs_tracked`. Percentiles use the nearest-rank
method over the sorted duration list.

### The honest limitation: "unmatched" merges

The GitHub Events API only returns recent activity per repo (roughly the last
~300 events / 90 days). If a PR was opened before `ingest` started watching a
repo, its `opened` event was never seen — but its eventual `closed` (merged)
event will be. That merge is real, but its true duration is unknowable from
this data source alone, so it's counted in `unmatched_merges` instead of
being assigned a fabricated or estimated duration. This is surfaced directly
in the API response and in the frontend (with an explanation), rather than
silently dropped or guessed at — the gap is part of the design, not a bug to
paper over.

## Live transport: WebSocket push (`WS /ws`)

The dashboard no longer polls. It opens a WebSocket to `WS /ws` and the
server pushes updates as events arrive.

- **On connect**, the server immediately sends a `snapshot` message —
  `{ type, stats, pr, recent }`, where `stats` is `read_window(15)` (3a),
  `pr` is `read_pr_stats()` (3b), and `recent` is the last N raw events — so
  a newly-connected client has full state without waiting for anything else.
- **Coalesced push, not per-event.** The consumer loop doesn't broadcast
  synchronously per event (that would firehose the client on the startup
  backlog — ~90 events at once). Instead each processed event is appended to
  an in-memory buffer, and a separate flush task drains it **every 500ms**
  (`FLUSH_INTERVAL_SECONDS` in `api/app/ws.py` — tune it there) and, if
  anything happened, broadcasts one `update` message:
  `{ type, event_count, events, stats, pr }`. `events` is capped at 50
  entries (`MAX_EVENTS_PER_PUSH`) — a huge burst still sends its true
  `event_count` plus a truncated sample, never thousands of rows. An empty
  interval sends nothing (no heartbeat) — the client's own `onclose`/
  `onerror` handlers are what detect a dropped connection, not a missed tick.
- **`ConnectionManager`** (`api/app/ws.py`) tracks connected sockets and
  broadcasts to a snapshot of the set, so one dead connection can't block
  delivery to the rest.

**Client** (`frontend/src/useEventStream.js`): replaces `snapshot` state
wholesale, merges `update` state (prepends/caps the event list, replaces
stats/pr), and **auto-reconnects** on close/error with exponential backoff
(1s → 2s → 4s → ... capped at 15s), resetting the backoff on a successful
open. A small green/amber dot next to "Pulse" shows `live` vs
`connecting`/`reconnecting` — the visible proof it's push, not poll.

The 15-minute window streams live at no HTTP cost. Switching to the 5m/60m
window falls back to a lightweight REST poll of `GET /stats` for as long as
that window is selected (simpler than teaching the socket protocol a
"change window" message); switching back to 15m goes back to pure push.

**REST endpoints are unchanged and still work** — `/stats`, `/stats/pr`,
`/events/recent`, `/stream/health` all remain, useful for debugging, `curl`,
or the 5m/60m fallback above. The WebSocket is additive transport, not a
replacement for the HTTP API.

## Run everything with Docker Compose

```bash
export GITHUB_TOKEN=ghp_xxx   # required
docker compose up --build
```

- Redis: `localhost:6379`
- API: http://localhost:8000 (`WS /ws`, `/health`, `/events/recent`, `/stats`, `/stats/pr`, `/stream/health`)
- Frontend: http://localhost:4173

Watch the `ingest` logs for polls, new-event counts, and rate-limit state, and
open the frontend to see recent GitHub activity for the watched repos.

## Run services locally for development

### Redis

```bash
docker run --rm -p 6379:6379 redis:7-alpine
```

### Ingest (Go)

```bash
cd ingest
cp .env.example .env   # fill in GITHUB_TOKEN and WATCHED_REPOS
go run .
```

### API (Python/FastAPI)

```bash
cd api
python -m venv .venv
.venv\Scripts\activate     # or `source .venv/bin/activate` on macOS/Linux
pip install -r requirements.txt
cp .env.example .env
uvicorn app.main:app --reload --port 8000
```

Extra env vars for the consumer group (both optional, see defaults above):
`CONSUMER_GROUP`, `CONSUMER_NAME`.

To run the API tests (requires `fakeredis`, used to exercise the consumer
group logic without a real Redis):

```bash
pip install -r requirements-dev.txt
pytest
```

### Frontend (Vite/React)

```bash
cd frontend
cp .env.example .env
npm install
npm run dev
```

Then open http://localhost:5173.

## Project layout

```
/ingest    Go service — polls the GitHub Events API and publishes normalized events to Redis Stream "events"
/api       FastAPI service — consumes the stream via a Redis consumer group, aggregates windowed counts and PR merge times, pushes live updates over WS /ws, serves /health, /events/recent, /stats, /stats/pr, /stream/health
/frontend  Vite + React app — polls the API and displays recent GitHub activity
```
