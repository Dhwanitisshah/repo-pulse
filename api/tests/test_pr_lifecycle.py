"""
PR open->merge duration tracking tests.

The percentile math is a pure function tested with no Redis at all. The
open/close correlation logic (record_pr_event / read_pr_stats) is tested
against fakeredis; skipped rather than faked if fakeredis isn't installed.
"""
import pytest

from app import pr_lifecycle


def test_percentile_pure_math_on_known_set():
    # 10 values, 1..10 (already sorted ascending).
    values = list(range(1, 11))

    assert pr_lifecycle.percentile(values, 50) == 5
    assert pr_lifecycle.percentile(values, 90) == 9
    assert pr_lifecycle.percentile(values, 100) == 10
    assert pr_lifecycle.percentile(values, 1) == 1


def test_percentile_empty_is_none():
    assert pr_lifecycle.percentile([], 50) is None


def test_percentile_single_value():
    assert pr_lifecycle.percentile([42], 50) == 42
    assert pr_lifecycle.percentile([42], 90) == 42


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


def _pr_event(repo, action, pr_number, ts, merged=None):
    return {
        "repo": repo,
        "event_type": "PullRequestEvent",
        "payload_action": action,
        "pr_number": pr_number,
        "pr_merged": merged,
        "ts": ts,
    }


@pytest.mark.asyncio
async def test_open_then_merged_close_records_duration_and_clears_open_entry(fake_client):
    repo = "a/a"
    open_ts = 1_000_000
    close_ts = open_ts + 3_600_000  # +1h, in ms

    await pr_lifecycle.record_pr_event(fake_client, _pr_event(repo, "opened", 1, open_ts))
    assert await fake_client.hget(pr_lifecycle.open_key(repo), "1") == str(open_ts)

    await pr_lifecycle.record_pr_event(
        fake_client, _pr_event(repo, "closed", 1, close_ts, merged=True)
    )

    # Open entry is cleared.
    assert await fake_client.hget(pr_lifecycle.open_key(repo), "1") is None

    stats = await pr_lifecycle.read_pr_stats(fake_client)
    assert stats[repo]["merged_count"] == 1
    assert stats[repo]["median_merge_seconds"] == 3600
    assert stats[repo]["unmatched_merges"] == 0
    assert stats[repo]["open_prs_tracked"] == 0


@pytest.mark.asyncio
async def test_merged_close_with_no_prior_open_counts_as_unmatched(fake_client):
    repo = "a/a"
    close_ts = 5_000_000

    # No "opened" event was ever seen for PR #7.
    await pr_lifecycle.record_pr_event(
        fake_client, _pr_event(repo, "closed", 7, close_ts, merged=True)
    )

    stats = await pr_lifecycle.read_pr_stats(fake_client)
    assert stats[repo]["merged_count"] == 0
    assert stats[repo]["median_merge_seconds"] is None
    assert stats[repo]["unmatched_merges"] == 1


@pytest.mark.asyncio
async def test_closed_unmerged_records_no_duration_and_clears_open_entry(fake_client):
    repo = "a/a"
    open_ts = 1_000_000
    close_ts = open_ts + 60_000

    await pr_lifecycle.record_pr_event(fake_client, _pr_event(repo, "opened", 2, open_ts))
    await pr_lifecycle.record_pr_event(
        fake_client, _pr_event(repo, "closed", 2, close_ts, merged=False)
    )

    assert await fake_client.hget(pr_lifecycle.open_key(repo), "2") is None
    assert await fake_client.zcard(pr_lifecycle.durations_key(repo)) == 0

    stats = await pr_lifecycle.read_pr_stats(fake_client)
    # No merges, no opens, no unmatched — repo shouldn't even show up.
    assert repo not in stats


@pytest.mark.asyncio
async def test_percentiles_from_a_known_duration_set(fake_client):
    repo = "a/a"
    base_ts = 0
    # Durations of 1..10 hours, one PR each.
    for i, hours in enumerate(range(1, 11), start=1):
        open_ts = base_ts
        close_ts = open_ts + hours * 3_600_000
        await pr_lifecycle.record_pr_event(fake_client, _pr_event(repo, "opened", i, open_ts))
        await pr_lifecycle.record_pr_event(
            fake_client, _pr_event(repo, "closed", i, close_ts, merged=True)
        )

    stats = await pr_lifecycle.read_pr_stats(fake_client)
    row = stats[repo]
    assert row["merged_count"] == 10
    assert row["median_merge_hours"] == 5.0
    assert row["p90_merge_hours"] == 9.0
    assert row["fastest_hours"] == 1.0
    assert row["slowest_hours"] == 10.0
