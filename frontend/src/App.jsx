import { useState } from "react";
import Dashboard from "./Dashboard.jsx";
import useEventStream from "./useEventStream.js";

function relativeTime(ts) {
  if (!ts) return "";
  const seconds = Math.round((Date.now() - ts) / 1000);
  if (seconds < 5) return "just now";
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.round(hours / 24)}d ago`;
}

function describeEvent(e) {
  return e.payload_action ? `${e.event_type} (${e.payload_action})` : e.event_type;
}

export default function App() {
  const [showRawEvents, setShowRawEvents] = useState(false);
  const { status, stats, pr, recent, anomalies, anomalyStatus } = useEventStream();

  return (
    <div style={{ fontFamily: "sans-serif", maxWidth: 800, margin: "2rem auto" }}>
      <h1>repo-pulse</h1>

      <Dashboard
        liveStats={stats}
        prStats={pr}
        connectionStatus={status}
        anomalies={anomalies}
        anomalyStatus={anomalyStatus}
      />

      <section style={{ marginTop: "2rem" }}>
        <button
          onClick={() => setShowRawEvents((v) => !v)}
          style={{
            padding: "0.3rem 0.6rem",
            border: "1px solid #ccc",
            borderRadius: 4,
            background: "white",
            cursor: "pointer",
          }}
        >
          {showRawEvents ? "Hide" : "Show"} raw recent events
        </button>

        {showRawEvents && (
          <ul style={{ marginTop: "0.75rem" }}>
            {[...recent].reverse().map((e, i) => (
              // Events pushed via a WebSocket "update" are compact (no `id` —
              // see EventCoalescer in api/app/ws.py), unlike the full events
              // in the initial snapshot, so key needs a synthetic fallback.
              <li key={e.id ?? `${e.ts}-${e.repo}-${e.actor}-${i}`}>
                <strong>{e.repo}</strong> — {describeEvent(e)} by {e.actor} ({relativeTime(e.ts)})
              </li>
            ))}
          </ul>
        )}
      </section>
    </div>
  );
}
