"""
ConnectionManager and EventCoalescer tests. No real network/websocket needed
-- ConnectionManager only ever calls .accept() / .send_json() on whatever
it's given, so a small fake stands in for fastapi.WebSocket.
"""
import pytest

from app.ws import ConnectionManager, EventCoalescer


class FakeWebSocket:
    def __init__(self, fail=False):
        self.fail = fail
        self.accepted = False
        self.sent: list[dict] = []

    async def accept(self):
        self.accepted = True

    async def send_json(self, message):
        if self.fail:
            raise ConnectionError("socket is gone")
        self.sent.append(message)


@pytest.mark.asyncio
async def test_connect_accepts_and_tracks_socket():
    manager = ConnectionManager()
    ws = FakeWebSocket()

    await manager.connect(ws)

    assert ws.accepted
    assert ws in manager._connections


@pytest.mark.asyncio
async def test_disconnect_removes_socket_and_is_safe_if_already_gone():
    manager = ConnectionManager()
    ws = FakeWebSocket()
    await manager.connect(ws)

    manager.disconnect(ws)
    assert ws not in manager._connections

    manager.disconnect(ws)  # no error on double-disconnect


@pytest.mark.asyncio
async def test_broadcast_sends_to_all_connected_sockets():
    manager = ConnectionManager()
    a, b = FakeWebSocket(), FakeWebSocket()
    await manager.connect(a)
    await manager.connect(b)

    await manager.broadcast({"type": "update", "n": 1})

    assert a.sent == [{"type": "update", "n": 1}]
    assert b.sent == [{"type": "update", "n": 1}]


@pytest.mark.asyncio
async def test_broadcast_removes_dead_socket_without_blocking_others():
    manager = ConnectionManager()
    dead = FakeWebSocket(fail=True)
    alive = FakeWebSocket()
    await manager.connect(dead)
    await manager.connect(alive)

    await manager.broadcast({"type": "update"})

    assert dead not in manager._connections
    assert alive in manager._connections
    assert alive.sent == [{"type": "update"}]


def test_coalescer_add_then_drain_returns_compact_events():
    coalescer = EventCoalescer()
    event = {
        "id": "1-0",
        "repo": "a/a",
        "event_type": "WatchEvent",
        "actor": "someone",
        "ts": 123,
        "payload_action": "started",
        "pr_number": None,
        "pr_merged": None,
    }

    coalescer.add(event)
    coalescer.add(event)
    drained = coalescer.drain()

    assert len(drained) == 2
    assert drained[0] == {
        "repo": "a/a",
        "event_type": "WatchEvent",
        "actor": "someone",
        "ts": 123,
        "payload_action": "started",
    }


def test_coalescer_drain_is_empty_when_nothing_added():
    coalescer = EventCoalescer()
    assert coalescer.drain() == []


def test_coalescer_drain_clears_the_buffer():
    coalescer = EventCoalescer()
    coalescer.add({"repo": "a/a", "event_type": "PushEvent", "actor": "x", "ts": 1, "payload_action": ""})

    first = coalescer.drain()
    second = coalescer.drain()

    assert len(first) == 1
    assert second == []


fakeredis = pytest.importorskip("fakeredis")
from fakeredis import aioredis as fake_aioredis  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from app import main as app_main  # noqa: E402


def test_ws_endpoint_sends_snapshot_on_connect(monkeypatch):
    fake_client = fake_aioredis.FakeRedis(decode_responses=True)
    # Skip the real Redis connection: every call to _make_client() (the
    # websocket handler, the flush loop, the REST endpoints) gets the same
    # in-memory fake instead. TestClient is used without `with`, so the
    # lifespan (and its background consume/flush tasks, which need a real
    # Redis) never starts -- only the /ws route itself is under test here.
    monkeypatch.setattr(app_main, "_make_client", lambda: fake_client)

    client = TestClient(app_main.app)
    with client.websocket_connect("/ws") as websocket:
        message = websocket.receive_json()

    assert message["type"] == "snapshot"
    assert message["stats"]["window_minutes"] == 15
    assert message["pr"] == {}
    # recent_events is a process-wide deque other tests may also populate;
    # assert the snapshot reflects it faithfully rather than assuming empty.
    assert message["recent"] == list(app_main.recent_events)
