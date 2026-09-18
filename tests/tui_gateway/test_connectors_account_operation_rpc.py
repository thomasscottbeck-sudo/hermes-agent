"""Account-owned connector operations through the JSON-RPC rail."""

from __future__ import annotations

import threading
import time

from tools.connectors import live


class _Transport:
    def __init__(self):
        self.frames = []
        self.event = threading.Event()

    def write(self, frame):
        self.frames.append(frame)
        self.event.set()
        return True

    def close(self):
        pass


def _reply(transport, method, params):
    from tui_gateway import server

    start = len(transport.frames)
    assert server.dispatch({"jsonrpc": "2.0", "id": 7, "method": method, "params": params}, transport) is None
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        replies = [frame for frame in transport.frames[start:] if frame.get("id") == 7]
        if replies:
            return replies[-1]
        transport.event.wait(0.05)
        transport.event.clear()
    raise AssertionError(f"{method} did not reply")


class _Client:
    def __init__(self):
        self.connected = threading.Event()
        self.mints = 0

    def connections(self, names, *, reinitiate=False, **_):
        self.mints += 1
        return {"results": [
            {"connector": name, "status": "initiated", "connect_url": f"https://connect.example/{name}",
             "connection_id": f"ca_{name}"}
            for name in names
        ]}

    def account_status(self, connection_id, *, timeout=None):
        name = connection_id.removeprefix("ca_")
        return {"connector": name, "connectionId": connection_id, "status": "active" if self.connected.is_set() else "pending"}


def test_connector_owner_union_accepts_both_tags_and_rejects_the_old_session_id():
    from pydantic import ValidationError
    from tui_gateway.contracts.connectors import ConnectorsListParams

    assert ConnectorsListParams.model_validate({"owner": {"type": "session", "session_id": "s"}}).owner.type == "session"
    assert ConnectorsListParams.model_validate({"owner": {"type": "account"}}).owner.type == "account"
    try:
        ConnectorsListParams.model_validate({"session_id": "s"})
    except ValidationError:
        pass
    else:
        raise AssertionError("the legacy top-level session_id must be refused")


def test_account_connect_starts_a_watcher_broadcasts_updates_and_closes(monkeypatch):
    from tools.connectors import account, managed
    from tui_gateway import server

    client = _Client()
    transport = _Transport()
    live.reset_for_tests()
    monkeypatch.setattr("tools.connectors.connectors_available", lambda: True)
    monkeypatch.setattr(managed, "WATCH_TICK_SECONDS", 0.01)
    monkeypatch.setattr(managed, "managed_client", lambda: client)
    monkeypatch.setattr(server, "_live_transports", {transport})
    monkeypatch.setattr(server, "_live_transports_lock", threading.Lock())
    try:
        reply = _reply(transport, "connectors.connect", {
            "owner": {"type": "account"}, "connectors": ["gmail"], "reconnect": False,
        })
        assert reply["result"]["op_id"]
        assert reply["result"]["targets"] == [{
            "name": "gmail", "kind": "connector", "action": "connect", "state": "initiated",
            "connect_url": "https://connect.example/gmail", "connection_id": "ca_gmail",
        }]
        operation = live.get_by_op_id(reply["result"]["op_id"])
        assert operation is not None and client.mints == 1
        client.connected.set()
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and live.get_by_op_id(operation.op_id) is not None:
            transport.event.wait(0.05)
            transport.event.clear()
        updates = [frame["params"]["payload"] for frame in transport.frames
                   if frame.get("method") == "event" and frame["params"].get("type") == "connection.update"]
        assert any(update["owner"] == {"type": "account"} and update["op_id"] == operation.op_id
                   and update.get("to") == "connected" for update in updates)
        assert live.get_by_op_id(operation.op_id) is None
    finally:
        live.reset_for_tests()


def test_account_connect_joins_the_open_operation_without_a_second_mint(monkeypatch):
    from tools.connectors import account, managed

    client = _Client()
    settled = threading.Event()
    live.reset_for_tests()
    monkeypatch.setattr(managed, "managed_client", lambda: client)
    original_close = live.close

    def close(operation):
        original_close(operation)
        settled.set()

    monkeypatch.setattr(live, "close", close)
    try:
        first = account.find_or_start_operation(["gmail"], action="connect", profile_home=None)
        assert account.wait_for_prepare(first)
        second = account.find_or_start_operation(["gmail"], action="connect", profile_home=None)
        assert second.started is False and second.operation is first.operation and client.mints == 1
        client.connected.set()
        assert settled.wait(2)
    finally:
        live.reset_for_tests()


def test_account_wake_is_profile_local(monkeypatch, tmp_path):
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override
    from tools.connectors.operation import ConnectionOperation, Target
    from tui_gateway import server

    home = tmp_path / "home"
    other = tmp_path / "other"
    home.mkdir()
    other.mkdir()
    live.reset_for_tests()
    monkeypatch.setattr(server, "_hermes_home", str(home))
    operation = ConnectionOperation([Target("gmail", "connector", "connect")], session_key="account:other")
    token = set_hermes_home_override(str(other))
    try:
        live.open(operation)
    finally:
        reset_hermes_home_override(token)
    try:
        reply = _reply(_Transport(), "connectors.operation.wake", {
            "owner": {"type": "account"}, "op_id": operation.op_id,
        })
        assert reply["error"]["data"]["reason"] == "UNKNOWN_OPERATION"
    finally:
        live.reset_for_tests()
