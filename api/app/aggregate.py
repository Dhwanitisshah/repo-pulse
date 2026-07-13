"""
Windowed event-count aggregation, stored in Redis as per-minute "tumbling
bucket" counters. Every consumed event increments a few counters; the reader
sums the last N one-minute buckets to answer "how much happened in the last
N minutes" without ever rescanning the stream or the recent_events deque
(that deque is just a capped display buffer, not a data store).

Retention: every bucket key is set to expire RETENTION_SECONDS after each
write, so buckets older than that self-clean and memory doesn't grow
unbounded. The retention window (2h) is comfortably larger than the widest
supported query window (60m) so a bucket is never queried after it expires.
"""
import time

RETENTION_SECONDS = 7200  # 2h — comfortably covers the widest window (60m)

REPOS_KEY = "stats:repos"
EVENT_TYPES_KEY = "stats:event_types"


def minute_bucket(ts_ms: int) -> int:
    return ts_ms // 60000


def bucket_range(window_minutes: int, now_ms: int | None = None) -> list[int]:
    """The `window_minutes` bucket ids ending at (and including) the current
    minute, oldest first."""
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    current = minute_bucket(now_ms)
    return [current - (window_minutes - 1) + i for i in range(window_minutes)]


def count_key(repo: str, bucket: int) -> str:
    return f"stats:count:{repo}:{bucket}"


def type_key(repo: str, event_type: str, bucket: int) -> str:
    return f"stats:type:{repo}:{event_type}:{bucket}"


def global_key(event_type: str, bucket: int) -> str:
    return f"stats:global:{event_type}:{bucket}"


def sum_bucket_values(values) -> int:
    """Sum a list of raw redis values (str or None for missing keys)."""
    return sum(int(v) for v in values if v is not None)


async def record_event(redis_client, event: dict) -> None:
    repo = event["repo"]
    event_type = event["event_type"]
    bucket = minute_bucket(event["ts"])

    count_k = count_key(repo, bucket)
    type_k = type_key(repo, event_type, bucket)
    global_k = global_key(event_type, bucket)

    pipe = redis_client.pipeline()
    pipe.incr(count_k)
    pipe.expire(count_k, RETENTION_SECONDS)
    pipe.incr(type_k)
    pipe.expire(type_k, RETENTION_SECONDS)
    pipe.incr(global_k)
    pipe.expire(global_k, RETENTION_SECONDS)
    pipe.sadd(REPOS_KEY, repo)
    pipe.sadd(EVENT_TYPES_KEY, event_type)
    await pipe.execute()


async def read_window(redis_client, window_minutes: int) -> dict:
    buckets = bucket_range(window_minutes)

    repos = sorted(await redis_client.smembers(REPOS_KEY))
    event_types = sorted(await redis_client.smembers(EVENT_TYPES_KEY))

    # Per-repo totals + timeline, from a single batched MGET.
    count_keys = [count_key(repo, bucket) for repo in repos for bucket in buckets]
    count_values = await redis_client.mget(count_keys) if count_keys else []

    per_repo_counts: dict[str, int] = {}
    bucket_totals = [0] * len(buckets)
    for i, repo in enumerate(repos):
        vals = count_values[i * len(buckets):(i + 1) * len(buckets)]
        repo_total = 0
        for j, v in enumerate(vals):
            n = int(v) if v is not None else 0
            repo_total += n
            bucket_totals[j] += n
        per_repo_counts[repo] = repo_total

    # Per-repo-per-type breakdown, from a single batched MGET.
    rt_keys = [
        type_key(repo, event_type, bucket)
        for repo in repos
        for event_type in event_types
        for bucket in buckets
    ]
    rt_values = await redis_client.mget(rt_keys) if rt_keys else []

    per_repo_type: dict[str, dict[str, int]] = {}
    idx = 0
    for repo in repos:
        type_counts = {}
        for event_type in event_types:
            vals = rt_values[idx:idx + len(buckets)]
            idx += len(buckets)
            total = sum_bucket_values(vals)
            if total:
                type_counts[event_type] = total
        if type_counts:
            per_repo_type[repo] = type_counts

    # Global per-type totals, from a single batched MGET.
    gt_keys = [global_key(event_type, bucket) for event_type in event_types for bucket in buckets]
    gt_values = await redis_client.mget(gt_keys) if gt_keys else []

    per_type_counts: dict[str, int] = {}
    for i, event_type in enumerate(event_types):
        vals = gt_values[i * len(buckets):(i + 1) * len(buckets)]
        per_type_counts[event_type] = sum_bucket_values(vals)

    total_events = sum(per_repo_counts.values())

    per_repo = sorted(
        ({"repo": repo, "count": count} for repo, count in per_repo_counts.items() if count),
        key=lambda x: -x["count"],
    )
    per_type = sorted(
        ({"event_type": t, "count": count} for t, count in per_type_counts.items() if count),
        key=lambda x: -x["count"],
    )
    timeline = [
        {"bucket_ts": bucket * 60000, "count": bucket_totals[i]} for i, bucket in enumerate(buckets)
    ]

    return {
        "window_minutes": window_minutes,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "total_events": total_events,
        "per_repo": per_repo,
        "per_type": per_type,
        "per_repo_type": per_repo_type,
        "timeline": timeline,
    }
