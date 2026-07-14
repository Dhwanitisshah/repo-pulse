import { useEffect, useRef, useState } from "react";
import { API_BASE_URL, fetchAnomalies } from "./api.js";

const MAX_RECENT_EVENTS = 50;
const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 15000;

// How long a fired anomaly stays visible client-side if it isn't refreshed
// by a later push or REST fetch — a fading, not a hard cutoff tied to the
// server's own ANOMALY_ACTIVE_TTL (api/app/anomaly.py), just a UI nicety so
// a stale alert doesn't linger forever if this tab misses updates.
const ANOMALY_CLIENT_TTL_MS = 5 * 60 * 1000;
const ANOMALY_PRUNE_INTERVAL_MS = 30 * 1000;

function wsUrl() {
  const url = new URL("/ws", API_BASE_URL);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.toString();
}

function anomalyKey(a) {
  return `${a.repo}|${a.scope}|${a.kind}`;
}

/**
 * Drives the dashboard from the API's WebSocket push instead of polling.
 * Replaces state wholesale on "snapshot" (sent right after connect), merges
 * on "update" (sent every ~500ms by the server's coalescing flush — see
 * api/app/ws.py). Auto-reconnects on close/error with exponential backoff
 * (1s, 2s, 4s, ... capped at 15s), resetting the backoff once a connection
 * actually opens.
 *
 * Anomalies are a separate, additive push ("anomaly" message) and separate
 * REST snapshot (GET /anomalies, fetched once on mount) — they don't ride
 * along with "snapshot"/"update". Active anomalies are kept in a map keyed
 * by repo+scope+kind so a re-fire refreshes rather than duplicates, and
 * pruned on a timer against ANOMALY_CLIENT_TTL_MS so a stale one fades if
 * nothing refreshes it.
 */
export default function useEventStream() {
  const [status, setStatus] = useState("connecting"); // connecting | live | reconnecting
  const [stats, setStats] = useState(null);
  const [pr, setPr] = useState(null);
  const [recent, setRecent] = useState([]);
  const [anomalies, setAnomalies] = useState([]);
  const [anomalyStatus, setAnomalyStatus] = useState(null); // { status, baseline_minutes, buckets_seen }

  const anomalyMapRef = useRef(new Map());

  function commitAnomalies() {
    const now = Date.now();
    for (const [key, a] of anomalyMapRef.current) {
      if (now - a.bucket_ts > ANOMALY_CLIENT_TTL_MS) {
        anomalyMapRef.current.delete(key);
      }
    }
    setAnomalies([...anomalyMapRef.current.values()]);
  }

  useEffect(() => {
    let cancelled = false;
    fetchAnomalies()
      .then((data) => {
        if (cancelled) return;
        for (const a of data.anomalies) {
          anomalyMapRef.current.set(anomalyKey(a), a);
        }
        commitAnomalies();
        setAnomalyStatus({
          status: data.status,
          baseline_minutes: data.baseline_minutes,
          min_baseline: data.min_baseline,
          buckets_seen: data.buckets_seen,
        });
      })
      .catch(() => {
        // Non-fatal: the WS "anomaly" push will populate this once connected.
      });

    const pruneTimer = setInterval(commitAnomalies, ANOMALY_PRUNE_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(pruneTimer);
    };
  }, []);

  useEffect(() => {
    let closedByUs = false;
    let attempt = 0;
    let socket;
    let reconnectTimer;

    function connect() {
      socket = new WebSocket(wsUrl());

      socket.onopen = () => {
        attempt = 0;
        setStatus("live");
      };

      socket.onmessage = (event) => {
        let message;
        try {
          message = JSON.parse(event.data);
        } catch {
          return; // malformed frame, ignore
        }

        if (message.type === "snapshot") {
          setStats(message.stats);
          setPr(message.pr);
          setRecent(message.recent.slice(-MAX_RECENT_EVENTS));
        } else if (message.type === "update") {
          setStats(message.stats);
          setPr(message.pr);
          setRecent((prev) => [...prev, ...message.events].slice(-MAX_RECENT_EVENTS));
        } else if (message.type === "anomaly") {
          for (const a of message.anomalies) {
            anomalyMapRef.current.set(anomalyKey(a), a);
          }
          commitAnomalies();
        }
      };

      socket.onclose = () => {
        if (closedByUs) return;
        setStatus("reconnecting");
        const delay = Math.min(RECONNECT_BASE_MS * 2 ** attempt, RECONNECT_MAX_MS);
        attempt += 1;
        reconnectTimer = setTimeout(connect, delay);
      };

      socket.onerror = () => {
        // onclose fires right after and drives the actual reconnect logic.
        socket.close();
      };
    }

    connect();

    return () => {
      closedByUs = true;
      clearTimeout(reconnectTimer);
      socket?.close();
    };
  }, []);

  return { status, stats, pr, recent, anomalies, anomalyStatus };
}
