"""
Consumer-group behavior tests (XREADGROUP / XACK / XPENDING).

Requires `fakeredis[lua]` (see requirements-dev.txt). If it isn't installed,
these tests are skipped rather than faking the assertions — install
requirements-dev.txt to actually exercise this coverage.
"""
import pytest
import pytest_asyncio

fakeredis = pytest.importorskip("fakeredis")
from fakeredis import aioredis as fake_aioredis  # noqa: E402

from app import main as app_main  # noqa: E402
from app.config import settings  # noqa: E402


@pytest.fixture
def group_env(monkeypatch):
    monkeypatch.setattr(settings, "stream_name", "test-events")
    monkeypatch.setattr(settings, "consumer_group", "test-group")
    monkeypatch.setattr(settings, "consumer_name", "test-consumer")
    return settings


@pytest_asyncio.fixture
async def fake_client():
    client = fake_aioredis.FakeRedis(decode_responses=True)
    try:
        yield client
    finally:
        await client.aclose()


async def _pending_count(client) -> int:
    summary = await client.xpending(settings.stream_name, settings.consumer_group)
    return summary["pending"] if summary else 0


@pytest.mark.asyncio
async def test_publish_consume_ack_clears_pending(group_env, fake_client):
    await app_main.ensure_consumer_group(fake_client)

    for i in range(3):
        await fake_client.xadd(settings.stream_name, {"event_type": "push", "repo": "r", "ts": i})

    response = await fake_client.xreadgroup(
        settings.consumer_group, settings.consumer_name,
        {settings.stream_name: ">"}, count=10,
    )
    assert response, "expected the 3 published events to be delivered"

    for _stream_key, messages in response:
        for message_id, fields in messages:
            await app_main._process_and_ack(fake_client, message_id, fields)

    assert await _pending_count(fake_client) == 0


@pytest.mark.asyncio
async def test_crash_recovery_drains_pending_on_reconnect(group_env, fake_client):
    await app_main.ensure_consumer_group(fake_client)

    for i in range(2):
        await fake_client.xadd(settings.stream_name, {"event_type": "push", "repo": "r", "ts": i})

    # Deliver, but simulate a crash before acking.
    response = await fake_client.xreadgroup(
        settings.consumer_group, settings.consumer_name,
        {settings.stream_name: ">"}, count=10,
    )
    assert response
    assert await _pending_count(fake_client) > 0

    # "Reconnect" as the same consumer and drain its pending entries.
    await app_main._drain_pending(fake_client)

    assert await _pending_count(fake_client) == 0
    assert len(app_main.recent_events) >= 2
