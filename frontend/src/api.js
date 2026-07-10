const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

export async function fetchRecentEvents() {
  const res = await fetch(`${API_BASE_URL}/events/recent`);
  if (!res.ok) {
    throw new Error(`request failed: ${res.status}`);
  }
  return res.json();
}
