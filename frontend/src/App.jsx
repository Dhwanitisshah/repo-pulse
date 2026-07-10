import { useEffect, useState } from "react";
import { fetchRecentEvents } from "./api.js";

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

  const latest = events[events.length - 1];

  return (
    <div style={{ fontFamily: "sans-serif", maxWidth: 600, margin: "2rem auto" }}>
      <h1>repo-pulse</h1>

      {error && <p style={{ color: "red" }}>Error: {error}</p>}

      <section>
        <h2>Latest event</h2>
        {latest ? (
          <p>
            <strong>{latest.repo}</strong> — {describeEvent(latest)} by {latest.actor} (
            {relativeTime(latest.ts)})
          </p>
        ) : (
          <p>Waiting for events…</p>
        )}
      </section>

      <section>
        <h2>Recent events</h2>
        <ul>
          {[...events].reverse().map((e) => (
            <li key={e.id}>
              <strong>{e.repo}</strong> — {describeEvent(e)} by {e.actor} ({relativeTime(e.ts)})
            </li>
          ))}
        </ul>
      </section>
    </div>
  );
}
