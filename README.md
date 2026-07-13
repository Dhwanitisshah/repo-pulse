# repo-pulse

A Go → Redis Streams → Python/FastAPI → React pipeline. The Go `ingest`
service polls the GitHub Events API for a configured list of repositories and
publishes normalized events onto a Redis Stream, the Python `api` service
consumes the stream and exposes it over HTTP, and the React `frontend` polls
the API and shows recent activity.

No aggregation yet — the frontend renders a raw recent-events list. That's a
later phase.

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

## Run everything with Docker Compose

```bash
export GITHUB_TOKEN=ghp_xxx   # required
docker compose up --build
```

- Redis: `localhost:6379`
- API: http://localhost:8000 (`/health`, `/events/recent`)
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
/api       FastAPI service — consumes the stream via a Redis consumer group, serves /health, /events/recent, /stream/health
/frontend  Vite + React app — polls the API and displays recent GitHub activity
```
