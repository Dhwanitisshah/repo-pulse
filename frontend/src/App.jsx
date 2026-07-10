import { useEffect, useState } from "react";
import { fetchRecentEvents } from "./api.js";

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
        <h2>Latest heartbeat</h2>
        {latest ? (
          <p>
            seq <strong>{latest.seq}</strong> @ {new Date(latest.ts).toLocaleTimeString()}
          </p>
        ) : (
          <p>Waiting for heartbeats…</p>
        )}
      </section>

      <section>
        <h2>Recent events</h2>
        <ul>
          {[...events].reverse().map((e) => (
            <li key={e.id}>
              seq {e.seq} — {new Date(e.ts).toLocaleTimeString()}
            </li>
          ))}
        </ul>
      </section>
    </div>
  );
}
