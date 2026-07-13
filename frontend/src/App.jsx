import { useEffect, useState } from "react";
import { fetchRecentEvents } from "./api.js";
import Dashboard from "./Dashboard.jsx";

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
  const [events, setEvents] = useState([]);
  const [error, setError] = useState(null);
  const [showRawEvents, setShowRawEvents] = useState(false);

  useEffect(() => {
    let cancelled = false;

    async function poll() {
      try {
        const data = await fetchRecentEvents();
        if (!cancelled) {
          setEvents(data);
          setError(null);
        }
      } catch (err) {
        if (!cancelled) {
          setError(err.message);
        }
      }
    }

    poll();
    const interval = setInterval(poll, 2000);

    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, []);

  return (
    <div style={{ fontFamily: "sans-serif", maxWidth: 800, margin: "2rem auto" }}>
      <h1>repo-pulse</h1>

      {error && <p style={{ color: "red" }}>Error: {error}</p>}

      <Dashboard />

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
            {[...events].reverse().map((e) => (
              <li key={e.id}>
                <strong>{e.repo}</strong> — {describeEvent(e)} by {e.actor} ({relativeTime(e.ts)})
              </li>
            ))}
          </ul>
        )}
      </section>
    </div>
  );
}
