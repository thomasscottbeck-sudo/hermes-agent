"""Account-owned managed connector operations for the session-less Connectors page."""

from __future__ import annotations

import contextvars
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

from tools.connectors import live
from tools.connectors import managed
from tools.connectors.operation import ConnectionOperation, Target
from tools.connectors.run import run_operation


_PREPARE_WAIT_SECONDS = 31.0
_start_lock = threading.Lock()


@dataclass
class AccountOperationStart:
    operation: ConnectionOperation
    started: bool
    prepared: threading.Event = field(default_factory=threading.Event)
    finished: threading.Event = field(default_factory=threading.Event)
    failed: threading.Event = field(default_factory=threading.Event)


def _matching_open_operation(names: list[str], *, profile_home: str | None) -> ConnectionOperation | None:
    matches = [live.find_target(name, profile_home=profile_home) for name in names]
    first = next((operation for operation in matches if operation is not None), None)
    if first is None:
        return None
    if any(operation is not first for operation in matches):
        raise ValueError("requested connectors do not share one open operation")
    return first


def find_or_start_operation(
    names: list[str],
    *,
    action: str,
    profile_home: str | None,
    client_factory: Callable[[], Any] | None = None,
) -> AccountOperationStart:
    """Join the exact open managed operation, or atomically reserve and start a new one."""
    with _start_lock:
        if operation := _matching_open_operation(names, profile_home=profile_home):
            return AccountOperationStart(operation=operation, started=False)
        operation = ConnectionOperation(
            [Target(name, "connector", action) for name in names],
            session_key=f"account:{uuid.uuid4().hex}",
        )
        started = AccountOperationStart(operation=operation, started=True)
        context = contextvars.copy_context()
        thread = threading.Thread(
            target=lambda: context.run(_run, started, action, client_factory),
            daemon=True,
            name="connector-account-operation",
        )
        try:
            live.open(operation)
            thread.start()
        except Exception:
            live.close(operation)
            raise
    return started


def _run(start: AccountOperationStart, action: str, client_factory: Callable[[], Any] | None) -> None:
    def callback(_payload: dict[str, Any]) -> None:
        start.prepared.set()

    try:
        client = (client_factory or managed.managed_client)()
        run_operation(
            start.operation.targets,
            managed.managed_kind(client, action, force=False),
            session_key=start.operation.session_key,
            tool_call_id=None,
            connection_callback=callback,
            tick_seconds=managed.WATCH_TICK_SECONDS,
            with_urls_in_result=False,
            operation=start.operation,
            register_operation=False,
        )
    except Exception:
        start.failed.set()
    finally:
        start.finished.set()


def wait_for_prepare(start: AccountOperationStart) -> bool:
    """Wait only for the initial mint, whose HTTP client has its own bounded timeout."""
    deadline = time.monotonic() + _PREPARE_WAIT_SECONDS
    while not start.prepared.is_set() and not start.finished.is_set():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        start.prepared.wait(min(0.05, remaining))
    return True
