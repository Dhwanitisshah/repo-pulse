import { useEffect, useState } from "react";
import { fetchStats } from "./api.js";

const WINDOW_OPTIONS = [5, 15, 60];

function formatDuration(hours) {
  if (hours == null) return "—";
  if (hours < 1) return `${Math.round(hours * 60)}m`;
  if (hours < 48) return `${hours.toFixed(1)}h`;
  return `${(hours / 24).toFixed(1)}d`;
}

function BarRow({ label, count, maxCount }) {
  const pct = maxCount > 0 ? Math.round((count / maxCount) * 100) : 0;
  return (
    <div style={{ marginBottom: "0.4rem" }}>
      <div style={{ display: "flex", justifyContent: "space-between", fontSize: "0.85rem" }}>
        <span>{label}</span>
        <span>{count}</span>
      </div>
      <div style={{ background: "#eee", borderRadius: 3, height: 8 }}>
        <div
          style={{
            width: `${pct}%`,
            background: "#3b82f6",
            height: "100%",
            borderRadius: 3,
            transition: "width 0.3s ease",
          }}
        />
      </div>
    </div>
  );
}

const STATUS_COLORS = {
  live: "#22c55e",
  connecting: "#f59e0b",
  reconnecting: "#f59e0b",
};

const STATUS_LABELS = {
  live: "live",
  connecting: "connecting…",
  reconnecting: "reconnecting…",
};

function ConnectionStatus({ status }) {
  return (
    <span
      title={STATUS_LABELS[status] ?? status}
      style={{ display: "inline-flex", alignItems: "center", gap: "0.3rem", fontSize: "0.75rem", color: "#666" }}
    >
      <span
        style={{
          width: 8,
          height: 8,
          borderRadius: "50%",
          background: STATUS_COLORS[status] ?? "#999",
          display: "inline-block",
        }}
      />
      {STATUS_LABELS[status] ?? status}
    </span>
  );
}

function Timeline({ timeline }) {
  const max = Math.max(1, ...timeline.map((t) => t.count));
  return (
    <div style={{ display: "flex", alignItems: "flex-end", gap: 2, height: 60 }}>
      {timeline.map((point) => (
        <div
          key={point.bucket_ts}
          title={`${new Date(point.bucket_ts).toLocaleTimeString()} — ${point.count} events`}
          style={{
            flex: 1,
            height: `${Math.round((point.count / max) * 100)}%`,
            minHeight: point.count > 0 ? 2 : 1,
            background: point.count > 0 ? "#3b82f6" : "#eee",
            borderRadius: 1,
          }}
        />
      ))}
    </div>
  );
}

// prStats is pushed over the WebSocket (see useEventStream.js) — no polling
// of its own, and it isn't windowed like the Pulse stats above, so switching
// the 5m/15m/60m selector doesn't affect it.
function PrLifecycle({ prStats }) {
  const repos = prStats ? Object.keys(prStats).sort() : [];
  const totalUnmatched = repos.reduce((sum, r) => sum + prStats[r].unmatched_merges, 0);

  return (
    <section style={{ marginTop: "2rem" }}>
      <h2 style={{ marginBottom: "0.3rem" }}>PR merge times</h2>

      {!prStats ? (
        <p>Loading…</p>
      ) : repos.length === 0 ? (
        <p style={{ color: "#999" }}>No PR activity tracked yet</p>
      ) : (
        <>
          <table style={{ borderCollapse: "collapse", width: "100%", fontSize: "0.9rem" }}>
            <thead>
              <tr style={{ textAlign: "left", borderBottom: "1px solid #ccc" }}>
                <th style={{ padding: "0.3rem 0.6rem 0.3rem 0" }}>Repo</th>
                <th style={{ padding: "0.3rem 0.6rem" }}>Median</th>
                <th style={{ padding: "0.3rem 0.6rem" }}>p90</th>
                <th style={{ padding: "0.3rem 0.6rem" }}>Merged</th>
                <th style={{ padding: "0.3rem 0.6rem" }}>Open (tracked)</th>
                <th style={{ padding: "0.3rem 0.6rem" }}>Unmatched</th>
              </tr>
            </thead>
            <tbody>
              {repos.map((repo) => {
                const s = prStats[repo];
                return (
                  <tr key={repo} style={{ borderBottom: "1px solid #eee" }}>
                    <td style={{ padding: "0.3rem 0.6rem 0.3rem 0" }}>{repo}</td>
                    <td style={{ padding: "0.3rem 0.6rem" }}>
                      {formatDuration(s.median_merge_hours)}
                    </td>
                    <td style={{ padding: "0.3rem 0.6rem" }}>{formatDuration(s.p90_merge_hours)}</td>
                    <td style={{ padding: "0.3rem 0.6rem" }}>{s.merged_count}</td>
                    <td style={{ padding: "0.3rem 0.6rem" }}>{s.open_prs_tracked}</td>
                    <td style={{ padding: "0.3rem 0.6rem" }}>{s.unmatched_merges}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>

          {totalUnmatched > 0 && (
            <p style={{ color: "#666", fontSize: "0.8rem", marginTop: "0.5rem" }}>
              {totalUnmatched} unmatched merge{totalUnmatched === 1 ? "" : "s"}: PRs that were
              opened before this service started watching, so their full open→merge duration
              can't be measured. GitHub's Events API only exposes recent activity per repo — this
              is a known data-source limit, not a bug.
            </p>
          )}
        </>
      )}
    </section>
  );
}

const LIVE_WINDOW_MINUTES = 15;

// The server streams window=15 by default over the WebSocket. For the other
// window sizes, rather than teaching the socket protocol a "change window"
// message, we fetch /stats ONCE when the user selects a non-15 window — no
// setInterval. `liveStats` (from useEventStream) only changes identity when
// the server actually pushes a WS "update", which itself only happens when
// real events were processed (see flush_loop in api/app/ws.py — it skips the
// broadcast entirely on an empty interval). So using it as an effect
// dependency here means: refetch when there's genuinely new data, stay
// silent when idle. A tab parked on 5m/60m with no activity fires exactly
// one GET /stats on window-select and then nothing.
export default function Dashboard({ liveStats, prStats, connectionStatus }) {
  const [windowMinutes, setWindowMinutes] = useState(LIVE_WINDOW_MINUTES);
  const [manualStats, setManualStats] = useState(null);
  const [manualError, setManualError] = useState(null);

  useEffect(() => {
    if (windowMinutes === LIVE_WINDOW_MINUTES) {
      setManualStats(null);
      setManualError(null);
      return;
    }

    let cancelled = false;

    fetchStats(windowMinutes)
      .then((data) => {
        if (!cancelled) {
          setManualStats(data);
          setManualError(null);
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setManualError(err.message);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [windowMinutes, liveStats]);

  const stats = windowMinutes === LIVE_WINDOW_MINUTES ? liveStats : manualStats;
  const error = windowMinutes === LIVE_WINDOW_MINUTES ? null : manualError;

  const maxRepoCount = stats ? Math.max(0, ...stats.per_repo.map((r) => r.count)) : 0;
  const maxTypeCount = stats ? Math.max(0, ...stats.per_type.map((t) => t.count)) : 0;

  return (
    <section>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
          <h2 style={{ margin: 0 }}>Pulse</h2>
          <ConnectionStatus status={connectionStatus} />
        </div>
        <div>
          {WINDOW_OPTIONS.map((w) => (
            <button
              key={w}
              onClick={() => setWindowMinutes(w)}
              style={{
                marginLeft: "0.4rem",
                padding: "0.25rem 0.6rem",
                border: "1px solid #ccc",
                borderRadius: 4,
                background: w === windowMinutes ? "#3b82f6" : "white",
                color: w === windowMinutes ? "white" : "black",
                cursor: "pointer",
              }}
            >
              {w}m
            </button>
          ))}
        </div>
      </div>

      {error && <p style={{ color: "red" }}>Error: {error}</p>}

      {!stats ? (
        <p>Loading…</p>
      ) : (
        <>
          <p style={{ color: "#555", fontSize: "0.9rem" }}>
            {stats.total_events} events in the last {stats.window_minutes} min
          </p>

          <h3 style={{ marginBottom: "0.3rem" }}>Timeline</h3>
          <Timeline timeline={stats.timeline} />

          <div style={{ display: "flex", gap: "2rem", marginTop: "1rem" }}>
            <div style={{ flex: 1 }}>
              <h3 style={{ marginBottom: "0.3rem" }}>Per repo</h3>
              {stats.per_repo.length === 0 ? (
                <p style={{ color: "#999" }}>No activity yet</p>
              ) : (
                stats.per_repo.map((r) => (
                  <BarRow key={r.repo} label={r.repo} count={r.count} maxCount={maxRepoCount} />
                ))
              )}
            </div>

            <div style={{ flex: 1 }}>
              <h3 style={{ marginBottom: "0.3rem" }}>Per type</h3>
              {stats.per_type.length === 0 ? (
                <p style={{ color: "#999" }}>No activity yet</p>
              ) : (
                stats.per_type.map((t) => (
                  <BarRow
                    key={t.event_type}
                    label={t.event_type}
                    count={t.count}
                    maxCount={maxTypeCount}
                  />
                ))
              )}
            </div>
          </div>
        </>
      )}

      <PrLifecycle prStats={prStats} />
    </section>
  );
}
