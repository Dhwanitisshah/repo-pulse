"""
PR open->merge duration tracking, stored statefully in Redis.

Unlike the counts in app.aggregate, this is correlational: a PullRequestEvent
with action "opened" now, and a *possibly much later* PullRequestEvent with
action "closed" (merged=true) for the same PR, must be matched up. That
requires per-PR state, kept in a Redis hash per repo:

  pr:open:{repo}            hash: {pr_number: open_ts_ms}   -- in-flight PRs
  pr:durations:{repo}       zset: {"{pr_number}:{close_ts}": duration_seconds}
  pr:unmatched:{repo}       counter: merges seen with no matching "opened"

Data-source limitation (intentionally not hidden): the GitHub Events API only
returns recent activity (~300 events / 90 days) per repo. A PR that was
opened before this service started watching will show up as a "closed,
merged" event with no matching "opened" event ever having been seen. That
merge is real, but its duration is unknowable from this data source, so it is
counted in `unmatched_merges` rather than assigned a fabricated duration.
"""
import math

MAX_DURATIONS_PER_REPO = 500

# Repos with any PullRequestEvent activity, tracked independently of
# app.aggregate's repo set so this module has no dependency on 3a's internals.
PR_REPOS_KEY = "pr:repos"


def open_key(repo: str) -> str:
    return f"pr:open:{repo}"


def durations_key(repo: str) -> str:
    return f"pr:durations:{repo}"


def unmatched_key(repo: str) -> str:
    return f"pr:unmatched:{repo}"


def percentile(sorted_values: list[float], p: float) -> float | None:
    """Nearest-rank percentile of an ascending-sorted list. p in [0, 100]."""
    if not sorted_values:
        return None
    n = len(sorted_values)
    rank = math.ceil(p / 100 * n)
    idx = max(0, min(n, rank) - 1)
    return sorted_values[idx]


async def record_pr_event(redis_client, event: dict) -> None:
    if event["event_type"] != "PullRequestEvent":
        return
    repo = event["repo"]
    pr_number = event["pr_number"]
    if repo is None or pr_number is None:
        return

    action = event["payload_action"]
    field = str(pr_number)

    await redis_client.sadd(PR_REPOS_KEY, repo)

    if action == "opened":
        await redis_client.hset(open_key(repo), field, event["ts"])
        return

    if action != "closed":
        return

    if not event["pr_merged"]:
        # Closed without merging: drop any in-flight state, nothing to record.
        await redis_client.hdel(open_key(repo), field)
        return

    open_ts = await redis_client.hget(open_key(repo), field)
    if open_ts is None:
        # Honest data-source limitation, not an error: this PR's "opened"
        # event predates our watch window. Do not fabricate a duration.
        await redis_client.incr(unmatched_key(repo))
        return

    close_ts = event["ts"]
    duration_seconds = (close_ts - int(open_ts)) // 1000
    dur_key = durations_key(repo)

    pipe = redis_client.pipeline()
    pipe.hdel(open_key(repo), field)
    pipe.zadd(dur_key, {f"{pr_number}:{close_ts}": duration_seconds})
    # Bound memory: caps the tracked set to MAX_DURATIONS_PER_REPO entries.
    # The zset is scored by duration (needed for percentile math below), so
    # this trims by duration rank rather than true chronological age.
    pipe.zremrangebyrank(dur_key, 0, -(MAX_DURATIONS_PER_REPO + 1))
    await pipe.execute()


async def read_pr_stats(redis_client) -> dict:
    repos = sorted(await redis_client.smembers(PR_REPOS_KEY))

    result: dict[str, dict] = {}
    for repo in repos:
        scored = await redis_client.zrange(durations_key(repo), 0, -1, withscores=True)
        durations = [score for _member, score in scored]  # ZRANGE returns ascending by score

        open_prs_tracked = await redis_client.hlen(open_key(repo))
        unmatched_raw = await redis_client.get(unmatched_key(repo))
        unmatched_merges = int(unmatched_raw) if unmatched_raw is not None else 0

        if not durations and open_prs_tracked == 0 and unmatched_merges == 0:
            continue  # no PR activity at all for this repo, skip the row

        median = percentile(durations, 50)
        p90 = percentile(durations, 90)
        fastest = durations[0] if durations else None
        slowest = durations[-1] if durations else None

        result[repo] = {
            "merged_count": len(durations),
            "median_merge_seconds": median,
            "median_merge_hours": round(median / 3600, 2) if median is not None else None,
            "p90_merge_seconds": p90,
            "p90_merge_hours": round(p90 / 3600, 2) if p90 is not None else None,
            "fastest_seconds": fastest,
            "fastest_hours": round(fastest / 3600, 2) if fastest is not None else None,
            "slowest_seconds": slowest,
            "slowest_hours": round(slowest / 3600, 2) if slowest is not None else None,
            "unmatched_merges": unmatched_merges,
            "open_prs_tracked": open_prs_tracked,
        }

    return result
