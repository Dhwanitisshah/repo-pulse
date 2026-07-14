"""
THROWAWAY dev script -- NOT part of the app, not imported by anything under
api/. Manual verification tool for Phase 5 anomaly detection: seeds a flat
baseline + a spike directly into Redis so you can watch the dashboard flag it
without waiting ANOMALY_BASELINE_MINUTES for real traffic to build a baseline.

Reuses the app's own bucket math and key builders (app.aggregate) so the
detector reads exactly what this writes -- no hand-rolled key strings that
could drift from the real ones.

Usage (from repo root, with the api venv active and Redis reachable):

    python scripts/inject_spike.py

Then watch the dashboard (or GET /anomalies) for up to ANOMALY_INTERVAL_SECONDS
(20s) -- the next detection tick should flag pallets/flask as a velocity_spike.
"""
import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))

import redis.asyncio as redis  # noqa: E402

from app import aggregate  # noqa: E402
from app.config import settings  # noqa: E402

REPO = "pallets/flask"
BASELINE_COUNT = 3
SPIKE_COUNT = 40


async def main() -> None:
    client = redis.Redis.from_url(
        f"redis://{settings.redis_addr}", decode_responses=True
    )

    now_ms = int(time.time() * 1000)
    current_bucket = aggregate.minute_bucket(now_ms)
    baseline_buckets = [
        current_bucket - offset
        for offset in range(1, settings.anomaly_baseline_minutes + 1)
    ]

    try:
        # So the detector's repo loop (which iterates aggregate.REPOS_KEY)
        # picks this repo up even if no real events for it have landed yet.
        await client.sadd(aggregate.REPOS_KEY, REPO)
        print(f"SADD {aggregate.REPOS_KEY} {REPO}")

        for bucket in baseline_buckets:
            key = aggregate.count_key(REPO, bucket)
            await client.set(key, BASELINE_COUNT, ex=aggregate.RETENTION_SECONDS)
            print(f"SET {key} = {BASELINE_COUNT} (ex={aggregate.RETENTION_SECONDS}s)")

        spike_key = aggregate.count_key(REPO, current_bucket)
        await client.set(spike_key, SPIKE_COUNT, ex=aggregate.RETENTION_SECONDS)
        print(f"SET {spike_key} = {SPIKE_COUNT} (ex={aggregate.RETENTION_SECONDS}s)  <- spike")

        print(
            f"\nSeeded {len(baseline_buckets)} flat baseline buckets "
            f"({BASELINE_COUNT} each) + 1 spike bucket ({SPIKE_COUNT}) for {REPO}."
        )
        print(
            "Wait up to ANOMALY_INTERVAL_SECONDS (20s) for the next detection "
            "tick, then check the dashboard's Alerts section or GET /anomalies."
        )
    finally:
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
