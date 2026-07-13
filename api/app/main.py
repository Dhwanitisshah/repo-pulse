import asyncio
import logging
from collections import deque
from contextlib import asynccontextmanager

import redis.asyncio as redis
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from app import aggregate
from app.config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("repo-pulse.api")

recent_events: deque[dict] = deque(maxlen=settings.max_events)

XREAD_BLOCK_MS = 5000


def _make_client() -> redis.Redis:
    return redis.Redis.from_url(
        f"redis://{settings.redis_addr}",
        decode_responses=True,
        socket_timeout=None,
    )


async def ensure_consumer_group(client: redis.Redis) -> None:
    # id="0" (not "$") so a freshly-created group starts from the beginning
    # of the stream and picks up any events already published, rather than
    # only events that arrive after the group exists.
    try:
        await client.xgroup_create(
            settings.stream_name, settings.consumer_group, id="0", mkstream=True
        )
    except redis.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def _fields_to_event(message_id: str, fields: dict) -> dict:
    return {
        "id": message_id,
        "event_type": fields.get("event_type"),
        "repo": fields.get("repo"),
        "actor": fields.get("actor"),
        "created_at": fields.get("created_at"),
        "ts": int(fields["ts"]) if "ts" in fields else None,
        "payload_action": fields.get("payload_action", ""),
    }


async def _process_and_ack(client: redis.Redis, message_id: str, fields: dict) -> None:
    event = _fields_to_event(message_id, fields)
    recent_events.append(event)
    logger.info(
        "consumed event %s repo=%s type=%s",
        message_id,
        event["repo"],
        event["event_type"],
    )
    # Only ack after the event has been durably appended above: if the
    # process dies before this point the message stays pending and is
    # replayed to this consumer on restart (at-least-once delivery).
    await client.xack(settings.stream_name, settings.consumer_group, message_id)
    await aggregate.record_event(client, event)


async def _drain_pending(client: redis.Redis) -> None:
    """Recover any messages this consumer was delivered but never acked
    (e.g. it crashed mid-processing). Reads id "0" against XREADGROUP, which
    means "my already-delivered, still-pending messages", not new ones."""
    while True:
        response = await client.xreadgroup(
            settings.consumer_group,
            settings.consumer_name,
            {settings.stream_name: "0"},
            count=10,
        )
        if not response:
            return
        drained_any = False
        for _stream_key, messages in response:
            for message_id, fields in messages:
                drained_any = True
                await _process_and_ack(client, message_id, fields)
        if not drained_any:
            return


async def consume_events() -> None:
    client = _make_client()
    logger.info(
        "starting stream consumer on %s (stream=%s, group=%s, consumer=%s)",
        settings.redis_addr,
        settings.stream_name,
        settings.consumer_group,
        settings.consumer_name,
    )

    try:
        await ensure_consumer_group(client)
        await _drain_pending(client)

        while True:
            try:
                response = await client.xreadgroup(
                    settings.consumer_group,
                    settings.consumer_name,
                    {settings.stream_name: ">"},
                    block=XREAD_BLOCK_MS,
                    count=10,
                )
            except redis.TimeoutError:
                # Expected during idle periods: the blocking XREADGROUP simply
                # had nothing to return within the block window.
                logger.debug("xreadgroup block window elapsed with no new events")
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
                    await _process_and_ack(client, message_id, fields)
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


VALID_STATS_WINDOWS = {5, 15, 60}


@app.get("/stats")
async def stats(window: int = 15):
    if window not in VALID_STATS_WINDOWS:
        raise HTTPException(
            status_code=400,
            detail=f"window must be one of {sorted(VALID_STATS_WINDOWS)}",
        )
    client = _make_client()
    try:
        return await aggregate.read_window(client, window)
    finally:
        await client.aclose()


@app.get("/stream/health")
async def stream_health():
    client = _make_client()
    try:
        length = await client.xlen(settings.stream_name)
        try:
            summary = await client.xpending(settings.stream_name, settings.consumer_group)
        except redis.ResponseError:
            # Group doesn't exist yet (stream never created / no consumer ran).
            summary = None

        if summary:
            per_consumer = [
                {"consumer": c["name"], "pending": c["pending"]}
                for c in (summary.get("consumers") or [])
            ]
            pending = {
                "count": summary.get("pending", 0),
                "min_id": summary.get("min"),
                "max_id": summary.get("max"),
                "per_consumer": per_consumer,
            }
        else:
            pending = {"count": 0, "min_id": None, "max_id": None, "per_consumer": []}

        return {
            "stream": settings.stream_name,
            "stream_length": length,
            "group": settings.consumer_group,
            "consumer": settings.consumer_name,
            "pending": pending,
        }
    finally:
        await client.aclose()
