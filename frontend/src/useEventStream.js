import { useEffect, useState } from "react";
import { API_BASE_URL } from "./api.js";

const MAX_RECENT_EVENTS = 50;
const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 15000;

function wsUrl() {
  const url = new URL("/ws", API_BASE_URL);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.toString();
}

/**
 * Drives the dashboard from the API's WebSocket push instead of polling.
 * Replaces state wholesale on "snapshot" (sent right after connect), merges
 * on "update" (sent every ~500ms by the server's coalescing flush — see
 * api/app/ws.py). Auto-reconnects on close/error with exponential backoff
 * (1s, 2s, 4s, ... capped at 15s), resetting the backoff once a connection
 * actually opens.
 */
export default function useEventStream() {
  const [status, setStatus] = useState("connecting"); // connecting | live | reconnecting
  const [stats, setStats] = useState(null);
  const [pr, setPr] = useState(null);
  const [recent, setRecent] = useState([]);

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

  return { status, stats, pr, recent };
}
