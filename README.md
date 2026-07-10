# repo-pulse

Skeleton proving a Go → Redis Streams → Python/FastAPI → React pipeline works
end to end. The Go `ingest` service publishes a heartbeat every 3 seconds, the
Python `api` service consumes the stream and exposes it over HTTP, and the
React `frontend` polls the API and shows the counter ticking up.

No GitHub integration or aggregation yet — just the heartbeat.

## Run everything with Docker Compose

```bash
docker compose up --build
```

- Redis: `localhost:6379`
- API: http://localhost:8000 (`/health`, `/events/recent`)
- Frontend: http://localhost:4173

Watch the `ingest` logs for published heartbeats, and open the frontend to see
the `seq` counter increase every ~3 seconds.

## Run services locally for development

### Redis

```bash
docker run --rm -p 6379:6379 redis:7-alpine
```

### Ingest (Go)

```bash
cd ingest
cp .env.example .env   # optional, defaults to localhost:6379
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
/ingest    Go service — produces heartbeat events to Redis Stream "events"
/api       FastAPI service — consumes the stream, serves /health and /events/recent
/frontend  Vite + React app — polls the API and displays recent heartbeats
```
