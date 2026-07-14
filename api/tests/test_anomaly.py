"""
Pure unit tests for app.anomaly — no Redis involved. detect_anomalies() and
AnomalyDetector operate entirely on plain Python data (bucket-count lists),
so the rolling-baseline math, warm-up guard, absolute floor, and cooldown
can all be exercised directly against known inputs.
"""
from app import anomaly


def series(repo_counts, types=None):
    return {"repo": repo_counts, "types": types or {}}


def test_flat_ish_baseline_with_big_spike_is_flagged_with_correct_sigma():
    # Small, non-zero variance baseline (mean=3.9, stddev=0.7) so this
    # exercises the real z-score path, not the stddev==0 fallback.
    baseline = [3, 4, 5, 4, 3, 4, 5, 4, 3, 4]
    current = 40
    data = {"a/a": series(baseline + [current])}

    anomalies = anomaly.detect_anomalies(data, now_bucket=1000)

    assert len(anomalies) == 1
    a = anomalies[0]
    assert a["repo"] == "a/a"
    assert a["scope"] == "repo"
    assert a["kind"] == "velocity_spike"
    assert a["current"] == 40
    assert a["baseline_mean"] == 3.9
    assert a["baseline_stddev"] == 0.7
    assert a["sigma"] == round((40 - 3.9) / 0.7, 2)
    assert a["severity"] == "spike"
    assert a["bucket_ts"] == 1000 * 60_000


def test_noisy_baseline_with_current_inside_band_is_not_flagged():
    baseline = [3, 4, 5, 4, 3, 4, 5, 4, 3, 4]  # mean=3.9, stddev=0.7
    current = 6  # exactly mean + 3*stddev -- inside band (not strictly greater)
    data = {"a/a": series(baseline + [current])}

    anomalies = anomaly.detect_anomalies(data, now_bucket=1000)

    assert anomalies == []


def test_insufficient_baseline_yields_no_anomalies_and_warming_up_status():
    baseline = [0, 0, 0, 2, 0, 0, 0, 0, 0, 0]  # only 1 non-empty bucket
    current = 10
    data = {"a/a": series(baseline + [current])}

    anomalies = anomaly.detect_anomalies(data, now_bucket=1000)
    assert anomalies == []

    status, buckets_seen = anomaly.warmup_status(data)
    assert status == "warming_up"
    assert buckets_seen == 1


def test_small_absolute_spike_below_floor_is_not_flagged():
    baseline = [1] * 10  # 10 non-empty buckets, satisfies warm-up
    current = 2  # 0->2-ish jump, but below ANOMALY_MIN_ABSOLUTE (5)
    data = {"a/a": series(baseline + [current])}

    anomalies = anomaly.detect_anomalies(data, now_bucket=1000)

    assert anomalies == []


def test_zero_stddev_baseline_does_not_divide_by_zero_and_still_flags():
    baseline = [4] * 10  # perfectly flat, stddev == 0
    current = 25
    data = {"a/a": series(baseline + [current])}

    anomalies = anomaly.detect_anomalies(data, now_bucket=1000)

    assert len(anomalies) == 1
    a = anomalies[0]
    assert a["baseline_stddev"] == 0.0
    assert a["current"] == 25
    assert a["severity"] in ("elevated", "spike")


def test_zero_stddev_baseline_with_small_jump_is_not_flagged():
    baseline = [4] * 10
    current = 6  # below the flat-baseline fallback threshold (max(5, 4*4)=16)
    data = {"a/a": series(baseline + [current])}

    anomalies = anomaly.detect_anomalies(data, now_bucket=1000)

    assert anomalies == []


def test_activity_stall_flags_repo_that_goes_quiet_after_healthy_baseline():
    # 10 healthy buckets (mean well above the absolute floor), then the last
    # 3 buckets are all zero.
    baseline = [8] * 10 + [0, 0]
    counts = baseline + [0]
    data = {"a/a": series(counts)}

    anomalies = anomaly.detect_anomalies(data, now_bucket=2000)

    stalls = [a for a in anomalies if a["kind"] == "activity_stall"]
    assert len(stalls) == 1
    assert stalls[0]["repo"] == "a/a"
    assert stalls[0]["scope"] == "repo"
    assert stalls[0]["current"] == 0


def test_activity_stall_not_flagged_if_only_recent_bucket_is_zero():
    # Only the current bucket is zero, not the full lookback window.
    baseline = [8] * 12
    counts = baseline + [0]
    data = {"a/a": series(counts)}

    anomalies = anomaly.detect_anomalies(data, now_bucket=2000)

    assert [a for a in anomalies if a["kind"] == "activity_stall"] == []


def test_cooldown_suppresses_repeat_firing_within_window():
    baseline = [4] * 10
    current = 40
    data = {"a/a": series(baseline + [current])}

    detector = anomaly.AnomalyDetector()

    first_new, first_active = detector.tick(data, now_bucket=1000)
    assert len(first_new) == 1
    assert len(first_active) == 1

    # Same condition, still within ANOMALY_COOLDOWN_MINUTES (5) later.
    second_new, second_active = detector.tick(data, now_bucket=1002)
    assert second_new == []
    # It's still considered "active" even while its re-fire is suppressed.
    assert len(second_active) == 1


def test_cooldown_allows_refiring_after_window_elapses():
    baseline = [4] * 10
    current = 40
    data = {"a/a": series(baseline + [current])}

    detector = anomaly.AnomalyDetector()
    detector.tick(data, now_bucket=1000)

    later_new, _later_active = detector.tick(data, now_bucket=1010)  # well past cooldown
    assert len(later_new) == 1


def test_per_type_scope_is_reported_as_type_prefixed():
    baseline = [3, 4, 5, 4, 3, 4, 5, 4, 3, 4]
    data = {
        "a/a": {
            "repo": [1] * 11,  # flat, low, no repo-level spike
            "types": {"WatchEvent": baseline + [40]},
        }
    }

    anomalies = anomaly.detect_anomalies(data, now_bucket=1000)

    assert len(anomalies) == 1
    assert anomalies[0]["scope"] == "type:WatchEvent"
