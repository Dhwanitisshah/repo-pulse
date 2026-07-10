import asyncio
import logging
from collections import deque
from contextlib import asynccontextmanager

import redis.asyncio as redis
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("repo-pulse.api")

recent_events: deque[dict] = deque(maxlen=settings.max_events)

XREAD_BLOCK_MS = 5000


async def consume_events() -> None:
    client = redis.Redis.from_url(
        f"redis://{settings.redis_addr}",
        decode_responses=True,
        socket_timeout=None,
    )
    last_id = "$"
    logger.info("starting stream consumer on %s (stream=%s)", settings.redis_addr, settings.stream_name)

    try:
        while True:
            try:
                response = await client.xread(
                    {settings.stream_name: last_id}, block=XREAD_BLOCK_MS, count=10
                )
            except redis.TimeoutError:
                # Expected during idle periods: the blocking XREAD simply had
                # nothing to return within the block window.
                logger.debug("xread block window elapsed with no new events")
                continue
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("error reading from redis stream, retrying")
                continue

            if not response:
                continue

            for _stream_key, messages in response:
                for message_id, fields in messages:
                    last_id = message_id
                    event = {
                        "id": message_id,
                        "event_type": fields.get("event_type"),
                        "repo": fields.get("repo"),
                        "actor": fields.get("actor"),
                        "created_at": fields.get("created_at"),
                        "ts": int(fields["ts"]) if "ts" in fields else None,
                        "payload_action": fields.get("payload_action", ""),
                    }
                    recent_events.append(event)
                    logger.info(
                        "consumed event %s repo=%s type=%s",
                        message_id,
                        event["repo"],
                        event["event_type"],
                    )
    finally:
        await client.aclose()


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(consume_events())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="repo-pulse api", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/events/recent")
async def events_recent():
    return list(recent_events)
