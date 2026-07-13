"""
Bucket-math / windowing tests for app.aggregate.

The pure helpers (minute_bucket, bucket_range, key builders, sum_bucket_values)
are tested with no Redis at all. The record_event / read_window round trip is
tested against fakeredis (see test_stream_consumer.py for the same pattern);
skipped rather than faked if fakeredis isn't installed.
"""
import time

import pytest

from app import aggregate


def test_minute_bucket_is_integer_division_by_60000():
    assert aggregate.minute_bucket(0) == 0
    assert aggregate.minute_bucket(59_999) == 0
    assert aggregate.minute_bucket(60_000) == 1
    assert aggregate.minute_bucket(1_783_902_120_064) == 1_783_902_120_064 // 60000


def test_bucket_range_is_window_minutes_long_ending_at_current_bucket():
    now_ms = 1_783_902_120_064
    current_bucket = aggregate.minute_bucket(now_ms)

    buckets = aggregate.bucket_range(15, now_ms)

    assert len(buckets) == 15
    assert buckets[-1] == current_bucket
    assert buckets[0] == current_bucket - 14
    assert buckets == list(range(current_bucket - 14, current_bucket + 1))


def test_bucket_range_5_and_60():
    now_ms = 1_783_902_120_064
    assert len(aggregate.bucket_range(5, now_ms)) == 5
    assert len(aggregate.bucket_range(60, now_ms)) == 60


def test_key_builders():
    assert aggregate.count_key("pallets/flask", 42) == "stats:count:pallets/flask:42"
    assert (
        aggregate.type_key("pallets/flask", "WatchEvent", 42)
        == "stats:type:pallets/flask:WatchEvent:42"
    )
    assert aggregate.global_key("WatchEvent", 42) == "stats:global:WatchEvent:42"


def test_sum_bucket_values_treats_missing_keys_as_zero():
    assert aggregate.sum_bucket_values(["1", None, "3", None, "5"]) == 9
    assert aggregate.sum_bucket_values([None, None]) == 0
    assert aggregate.sum_bucket_values([]) == 0


fakeredis = pytest.importorskip("fakeredis")
from fakeredis import aioredis as fake_aioredis  # noqa: E402
import pytest_asyncio  # noqa: E402


@pytest_asyncio.fixture
async def fake_client():
    client = fake_aioredis.FakeRedis(decode_responses=True)
    try:
        yield client
    finally:
        await client.aclose()


def _event(repo, event_type, ts):
    return {"repo": repo, "event_type": event_type, "ts": ts}


@pytest.mark.asyncio
async def test_record_and_read_window_round_trip(fake_client):
    # read_window always keys off the real wall clock, so events must be
    # timestamped "now" (not a fixed instant) to land in its current bucket.
    now_ms = int(time.time() * 1000)
    current_bucket = aggregate.minute_bucket(now_ms)

    # 3 events this minute for repo A, 1 for repo B, mixed types.
    await aggregate.record_event(fake_client, _event("a/a", "WatchEvent", now_ms))
    await aggregate.record_event(fake_client, _event("a/a", "WatchEvent", now_ms))
    await aggregate.record_event(fake_client, _event("a/a", "ForkEvent", now_ms))
    await aggregate.record_event(fake_client, _event("b/b", "WatchEvent", now_ms))

    result = await aggregate.read_window(fake_client, 15)

    assert result["window_minutes"] == 15
    assert result["total_events"] == 4
    assert {"repo": "a/a", "count": 3} in result["per_repo"]
    assert {"repo": "b/b", "count": 1} in result["per_repo"]
    assert result["per_repo_type"]["a/a"] == {"WatchEvent": 2, "ForkEvent": 1}
    assert result["per_repo_type"]["b/b"] == {"WatchEvent": 1}

    per_type = {row["event_type"]: row["count"] for row in result["per_type"]}
    assert per_type == {"WatchEvent": 3, "ForkEvent": 1}

    assert len(result["timeline"]) == 15
    current_row = next(row for row in result["timeline"] if row["bucket_ts"] == current_bucket * 60000)
    assert current_row["count"] == 4


@pytest.mark.asyncio
async def test_events_outside_window_are_excluded(fake_client):
    now_ms = int(time.time() * 1000)
    old_ts = now_ms - (30 * 60_000)  # 30 minutes ago — outside a 15-min window

    await aggregate.record_event(fake_client, _event("a/a", "WatchEvent", old_ts))

    result = await aggregate.read_window(fake_client, 15)

    # The event was recorded (repo/type sets updated) but its bucket falls
    # outside the requested window, so it must not be counted.
    assert result["total_events"] == 0
    assert result["per_repo"] == []
