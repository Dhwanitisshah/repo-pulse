import { useEffect, useState } from "react";
import { fetchStats } from "./api.js";

const WINDOW_OPTIONS = [5, 15, 60];

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

export default function Dashboard() {
  const [windowMinutes, setWindowMinutes] = useState(15);
  const [stats, setStats] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    let cancelled = false;

    async function poll() {
      try {
        const data = await fetchStats(windowMinutes);
        if (!cancelled) {
          setStats(data);
          setError(null);
        }
      } catch (err) {
        if (!cancelled) {
          setError(err.message);
        }
      }
    }

    poll();
    const interval = setInterval(poll, 3000);

    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, [windowMinutes]);

  const maxRepoCount = stats ? Math.max(0, ...stats.per_repo.map((r) => r.count)) : 0;
  const maxTypeCount = stats ? Math.max(0, ...stats.per_type.map((t) => t.count)) : 0;

  return (
    <section>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <h2 style={{ margin: 0 }}>Pulse</h2>
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
    </section>
  );
}
