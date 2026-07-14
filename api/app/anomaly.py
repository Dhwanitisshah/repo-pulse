"""
Anomaly detection over the minute-bucket series already produced by
app.aggregate — no new raw-event storage, purely statistical synthesis of
existing counters. This is the "instrument that tells you when to look"
layer: a rolling mean/stddev baseline over the last N buckets, flagging the
current bucket when it breaks out of the expected band.

detect_anomalies() is a pure function (no Redis, no I/O) so the flagging
logic is unit-testable in isolation — see api/tests/test_anomaly.py. Fetching
the input series from Redis lives in app.aggregate.read_series(); cooldown
and "currently active" bookkeeping (which needs memory across calls) lives in
the AnomalyDetector class below.

Two kinds of signal:
  - velocity_spike: current bucket count is anomalously high vs. its own
    baseline (per-repo, and per repo+event-type).
  - activity_stall: a repo with a healthy baseline goes quiet for the last
    few buckets — the inverse signal, a repo going dark rather than spiking.

pr_queue_growth (open PRs rising while merges stall) was considered but
skipped: app.pr_lifecycle only exposes a current snapshot of open_prs_tracked,
not a bucketed history of it, and adding that history would mean new stored
state beyond the existing minute buckets -- out of scope for a pure-synthesis
detector.
"""
import time

from app.config import settings

# How many of the most recent buckets must be all-zero to call a repo
# "stalled", once its baseline shows healthy activity.
STALL_LOOKBACK_MINUTES = 3

# When the baseline is perfectly flat (stddev == 0), sigma-based flagging
# can't divide by zero. Fall back to an absolute floor plus this multiple of
# the (flat) mean -- a jump off a steady baseline still flags.
FLAT_BASELINE_MULTIPLIER = 4.0

# A "spike" (vs. merely "elevated") once sigma clears this multiple of the
# configured threshold.
SPIKE_SEVERITY_MULTIPLIER = 2.0

# How long a fired anomaly is kept in the "currently active" snapshot
# (GET /anomalies) after it last fired, absent a fresh re-fire.
ACTIVE_TTL_MINUTES = 15


def _stats(values: list[int]) -> tuple[float, float]:
    """Population mean and stddev of a list of counts."""
    n = len(values)
    if n == 0:
        return 0.0, 0.0
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / n
    return mean, variance ** 0.5


def _detect_velocity_spike(repo: str, scope: str, counts: list[int], bucket_ts: int) -> dict | None:
    if len(counts) < 2:
        return None

    baseline = counts[:-1]
    current = counts[-1]

    nonempty_baseline = sum(1 for c in baseline if c > 0)
    if nonempty_baseline < settings.anomaly_min_baseline:
        return None  # warm-up: not enough history to judge this series yet

    if current < settings.anomaly_min_absolute:
        return None  # too small to matter, even if it's a big relative jump

    mean, stddev = _stats(baseline)

    if stddev > 0:
        sigma = (current - mean) / stddev
        flagged = current > mean + settings.anomaly_sigma * stddev
    else:
        # Flat baseline: no variance to divide by. sigma here is a
        # multiple-of-mean ratio, not a true z-score -- informational only.
        threshold = max(settings.anomaly_min_absolute, mean * FLAT_BASELINE_MULTIPLIER)
        flagged = current > threshold
        sigma = current / max(mean, 1.0)

    if not flagged:
        return None

    severity = "spike" if sigma >= settings.anomaly_sigma * SPIKE_SEVERITY_MULTIPLIER else "elevated"

    return {
        "repo": repo,
        "scope": scope,
        "kind": "velocity_spike",
        "current": current,
        "baseline_mean": round(mean, 2),
        "baseline_stddev": round(stddev, 2),
        "sigma": round(sigma, 2),
        "bucket_ts": bucket_ts,
        "severity": severity,
    }


def _detect_activity_stall(repo: str, counts: list[int], bucket_ts: int) -> dict | None:
    if len(counts) < STALL_LOOKBACK_MINUTES + 1:
        return None

    baseline = counts[:-1]
    nonempty_baseline = sum(1 for c in baseline if c > 0)
    if nonempty_baseline < settings.anomaly_min_baseline:
        return None  # warm-up

    mean, _stddev = _stats(baseline)
    if mean < settings.anomaly_min_absolute:
        return None  # never had healthy activity to stall from

    recent = counts[-STALL_LOOKBACK_MINUTES:]
    if any(c > 0 for c in recent):
        return None

    return {
        "repo": repo,
        "scope": "repo",
        "kind": "activity_stall",
        "current": counts[-1],
        "baseline_mean": round(mean, 2),
        "baseline_stddev": 0.0,
        "sigma": 0.0,
        "bucket_ts": bucket_ts,
        "severity": "elevated",
    }


def detect_anomalies(series_per_repo: dict[str, dict], now_bucket: int) -> list[dict]:
    """series_per_repo: { repo: { "repo": [counts...], "types": { event_type:
    [counts...] } } }, each count list oldest-bucket-first with the current
    (or just-closed) bucket last. now_bucket is the current minute bucket id,
    used only to stamp bucket_ts on any anomalies found."""
    bucket_ts = now_bucket * 60_000
    anomalies = []

    for repo, data in series_per_repo.items():
        repo_counts = data.get("repo", [])

        spike = _detect_velocity_spike(repo, "repo", repo_counts, bucket_ts)
        if spike:
            anomalies.append(spike)

        stall = _detect_activity_stall(repo, repo_counts, bucket_ts)
        if stall:
            anomalies.append(stall)

        for event_type, type_counts in data.get("types", {}).items():
            type_spike = _detect_velocity_spike(repo, f"type:{event_type}", type_counts, bucket_ts)
            if type_spike:
                anomalies.append(type_spike)

    return anomalies


def warmup_status(series_per_repo: dict[str, dict]) -> tuple[str, int]:
    """Overall (status, buckets_seen) for GET /anomalies: buckets_seen is the
    healthiest baseline seen across any series, so a UI can show real warm-up
    progress even before any one repo has crossed the detection threshold."""
    buckets_seen = 0
    for data in series_per_repo.values():
        counts = data.get("repo", [])
        baseline = counts[:-1] if counts else []
        buckets_seen = max(buckets_seen, sum(1 for c in baseline if c > 0))

    status = "active" if buckets_seen >= settings.anomaly_min_baseline else "warming_up"
    return status, buckets_seen


class AnomalyDetector:
    """Stateful wrapper around detect_anomalies(): applies a per-(repo, scope,
    kind) cooldown so the same condition doesn't re-broadcast every tick, and
    tracks which anomalies are still "active" (fired recently enough to
    matter) for the REST snapshot."""

    def __init__(self) -> None:
        self._last_fired: dict[tuple[str, str, str], int] = {}
        self._active: dict[tuple[str, str, str], tuple[dict, int]] = {}

    def tick(self, series_per_repo: dict[str, dict], now_bucket: int) -> tuple[list[dict], list[dict]]:
        """Returns (newly_fired, currently_active). newly_fired is what
        should be broadcast; currently_active is the full snapshot for
        GET /anomalies (includes anomalies still within their cooldown that
        already fired on a previous tick)."""
        raw = detect_anomalies(series_per_repo, now_bucket)
        newly_fired = []

        for anomaly in raw:
            key = (anomaly["repo"], anomaly["scope"], anomaly["kind"])
            last_fired = self._last_fired.get(key)
            if last_fired is None or now_bucket - last_fired >= settings.anomaly_cooldown_minutes:
                self._last_fired[key] = now_bucket
                newly_fired.append(anomaly)
            self._active[key] = (anomaly, now_bucket)

        for key in list(self._active):
            _anomaly, fired_at = self._active[key]
            if now_bucket - fired_at > ACTIVE_TTL_MINUTES:
                del self._active[key]

        active = [anomaly for anomaly, _fired_at in self._active.values()]
        return newly_fired, active


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
