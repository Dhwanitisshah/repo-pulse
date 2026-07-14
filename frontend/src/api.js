export const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

// Kept for debugging / non-streaming use (e.g. the console, or a future
// debug page). The live dashboard gets this data pushed over the WebSocket
// (see useEventStream.js) rather than polling these.
export async function fetchRecentEvents() {
  const res = await fetch(`${API_BASE_URL}/events/recent`);
  if (!res.ok) {
    throw new Error(`request failed: ${res.status}`);
  }
  return res.json();
}

export async function fetchStats(windowMinutes) {
  const res = await fetch(`${API_BASE_URL}/stats?window=${windowMinutes}`);
  if (!res.ok) {
    throw new Error(`request failed: ${res.status}`);
  }
  return res.json();
}

export async function fetchPrStats() {
  const res = await fetch(`${API_BASE_URL}/stats/pr`);
  if (!res.ok) {
    throw new Error(`request failed: ${res.status}`);
  }
  return res.json();
}

// Initial-load snapshot of active anomalies; live updates arrive over the
// WebSocket "anomaly" message (see useEventStream.js) after this.
export async function fetchAnomalies() {
  const res = await fetch(`${API_BASE_URL}/anomalies`);
  if (!res.ok) {
    throw new Error(`request failed: ${res.status}`);
  }
  return res.json();
}
