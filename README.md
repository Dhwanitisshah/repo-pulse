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
/api       FastAPI service — consumes the stream, serves /health and /events/recent
/frontend  Vite + React app — polls the API and displays recent GitHub activity
```
