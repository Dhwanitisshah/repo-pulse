"""
WebSocket transport: pushes live updates to connected dashboards instead of
making them poll REST endpoints. Two pieces:

  - ConnectionManager: tracks the set of connected sockets and broadcasts
    JSON to all of them, tolerating individual dead connections.
  - EventCoalescer: a shared buffer the consumer loop appends compact event
    summaries to. A periodic flush task (started in main.py's lifespan)
    drains it and broadcasts one batched "update" message, so a burst of N
    events (e.g. the startup backlog) becomes one push, not N.

FLUSH_INTERVAL_SECONDS is the coalescing window: how often pending events are
batched and pushed, and the rough upper bound on "how stale can the live
dashboard be." 500ms is a reasonable default: fast enough to feel live, slow
enough that a burst of dozens of events collapses into one message. Tune by
changing the constant below.
"""
import logging

from fastapi import WebSocket

logger = logging.getLogger("repo-pulse.api")

FLUSH_INTERVAL_SECONDS = 0.5
MAX_EVENTS_PER_PUSH = 50


class ConnectionManager:
    def __init__(self) -> None:
        self._connections: set[WebSocket] = set()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self._connections.add(ws)

    def disconnect(self, ws: WebSocket) -> None:
        self._connections.discard(ws)

    async def broadcast(self, message: dict) -> None:
        # Snapshot the set: a socket can disconnect (and call disconnect(),
        # mutating self._connections) while we're mid-iteration otherwise.
        for ws in list(self._connections):
            try:
                await ws.send_json(message)
            except Exception:
                logger.info("dropping dead websocket connection")
                self.disconnect(ws)


class EventCoalescer:
    """Buffers compact event summaries between flushes."""

    def __init__(self) -> None:
        self._pending: list[dict] = []

    def add(self, event: dict) -> None:
        self._pending.append(
            {
                "repo": event.get("repo"),
                "event_type": event.get("event_type"),
                "actor": event.get("actor"),
                "ts": event.get("ts"),
                "payload_action": event.get("payload_action"),
            }
        )

    def drain(self) -> list[dict]:
        """Pop everything buffered since the last drain."""
        pending, self._pending = self._pending, []
        return pending
