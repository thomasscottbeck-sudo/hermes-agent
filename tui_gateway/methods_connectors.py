"""Connector RPCs and the connection-operation bridge.

A session owner is authorized by its live transport. An account owner is scoped by its profile
and drives a session-less operation for the Connectors page.
"""

import contextvars

from .method_ctx import HandlerRegistry, bind_module

_registry = HandlerRegistry()
method = _registry.method
_CONNECTOR_RPC_METHODS = frozenset({
    "connectors.list",
    "connectors.connect",
    "connectors.operation.status",
    "connectors.operation.wake",
    "connection.respond",
})
_connector_rpc_origin: contextvars.ContextVar[tuple | None] = contextvars.ContextVar("connector_rpc_origin", default=None)


def _capture_connector_rpc_owner(params):
    owner = params.get("owner") if isinstance(params, dict) else None
    sid = owner.get("session_id") if isinstance(owner, dict) and owner.get("type") == "session" else ""
    _, session = _current_session_steer_authority(sid if isinstance(sid, str) else "")
    _connector_rpc_origin.set((session, session.get("profile_home") if session is not None else None))


def _connector_rpc_error(rid, code, reason, message):
    return _err(rid, code, message, data={"reason": reason})


def _reason(name):
    from tui_gateway.contracts.connectors import ConnectorErrorReason

    return getattr(ConnectorErrorReason, name)


def _connector_owner_matches(sid, owner, profile_home):
    _, current = _current_session_steer_authority(sid)
    return current is owner and not owner.get("_finalized") and owner.get("profile_home") == profile_home


def _session_owner(rid, owner):
    sid = owner.session_id
    _, session = _current_session_steer_authority(sid)
    origin = _connector_rpc_origin.get()
    if (session is None or session.get("_finalized")
            or origin is not None and (origin[0] is not session or origin[1] != session.get("profile_home"))):
        return None, _connector_rpc_error(rid, 4001, _reason("not_owner"), "session not found or not owned by this transport")
    if _session_uses_compute_host(session):
        return None, _connector_rpc_error(
            rid, 5033, _reason("unsupported_runtime"), "Connectors must be managed on the session's compute host."
        )
    return session, None


def _account_owner(rid, _owner):
    from tools.connectors import connectors_available

    if not connectors_available():
        return None, _connector_rpc_error(rid, 4031, _reason("connectors_unavailable"), "Connectors are not available.")
    return None, None


_OWNER_HANDLERS = {"session": _session_owner, "account": _account_owner}


def _parse_params(rid, params, model):
    from pydantic import ValidationError

    try:
        return model.model_validate(params), None
    except ValidationError:
        return None, _connector_rpc_error(rid, 4000, _reason("invalid_params"), "Connector parameters are invalid.")


def _connector_params(rid, params, model):
    request, error = _parse_params(rid, params, model)
    if error:
        return None, None, error
    handler = _OWNER_HANDLERS[request.owner.type]
    try:
        if request.owner.type == "account":
            with _account_scope(request):
                session, error = handler(rid, request.owner)
        else:
            session, error = handler(rid, request.owner)
    except Exception:
        return None, None, _connector_rpc_error(
            rid, 5034, _reason("connector_request_failed"), "Connector request failed. Try again explicitly."
        )
    return request, session, error


def _session_connector_gate(rid, session, action):
    import model_tools
    from tools.connectors import connectors_available

    agent = session.get("agent")
    enabled = agent.enabled_toolsets if agent is not None else _load_enabled_toolsets(_resolve_agent_platform(_session_source(session)))
    disabled = agent.disabled_toolsets if agent is not None else None
    if "manage_connections" not in model_tools._select_tool_names(enabled, disabled, quiet_mode=True) or not connectors_available():
        if action == "status":
            return enabled, disabled, _ok(rid, {"available": False, "connectors": []})
        return enabled, disabled, _connector_rpc_error(
            rid, 4031, _reason("connectors_unavailable"), "Connectors are not available in this session."
        )
    return enabled, disabled, None


def _session_connector_rpc(rid, request, session, action):
    import json
    import model_tools
    import uuid

    from tools.connectors import live
    from tui_gateway.connector_payload import connector_ui_payload

    sid = request.owner.session_id
    enabled, disabled, error = _session_connector_gate(rid, session, action)
    if error:
        return error
    if not _connector_owner_matches(sid, session, session.get("profile_home")):
        return _connector_rpc_error(rid, 4001, _reason("not_owner"), "session ownership changed")
    args = {"action": "reconnect" if action == "connect" and request.reconnect else action}
    if action == "connect":
        args["connectors"] = request.connectors
    if action == "connect" and (
            operation := live.current(session["session_key"], profile_home=session.get("profile_home"))) is not None:
        return _reissue(rid, operation, args)
    raw = model_tools.handle_function_call(
        "manage_connections",
        args,
        task_id=session["session_key"],
        session_id=getattr(session.get("agent"), "session_id", None) or session["session_key"],
        tool_call_id=f"connector-ui-{uuid.uuid4().hex}",
        enabled_toolsets=enabled,
        disabled_toolsets=disabled,
    )
    data = json.loads(raw) if isinstance(raw, str) else raw
    if not isinstance(data, dict) or "error" in data:
        return _connector_rpc_error(rid, 5034, _reason("connector_request_failed"), "Connector request failed. Try again explicitly.")
    if action == "status":
        if not isinstance(data.get("connectors"), list) or any(not isinstance(row, dict) for row in data["connectors"]):
            return _connector_rpc_error(rid, 5034, _reason("invalid_connector_response"), "Connector service returned an invalid response.")
        return _ok(rid, {"available": True, "connectors": connector_ui_payload(data["connectors"])})
    if not isinstance(data.get("targets"), list):
        return _connector_rpc_error(rid, 5034, _reason("invalid_connector_response"), "Connector service returned no authorization results.")
    return _ok(rid, connector_ui_payload(data))


def _account_connector_list(rid):
    from tools.connectors.managed import managed_client
    from tui_gateway.connector_payload import connector_ui_payload

    try:
        return _ok(rid, {"available": True, "connectors": connector_ui_payload(managed_client().list_connectors())})
    except Exception:
        return _connector_rpc_error(rid, 5034, _reason("connector_request_failed"), "Connector request failed. Try again explicitly.")


def _account_connector_connect(rid, request):
    from tools.connectors import account
    from tui_gateway.connector_payload import connector_ui_payload

    profile_home = _profile_home(request.profile)
    action = "reconnect" if request.reconnect else "connect"
    try:
        start = account.find_or_start_operation(
            request.connectors, action=action, profile_home=str(profile_home) if profile_home else None
        )
        if not start.started:
            from tools.connectors.contract import TargetState

            targets = [start.operation.target(name) for name in request.connectors]
            if any(target.state in (TargetState.failed, TargetState.expired) for target in targets):
                return _reissue(rid, start.operation, {"connectors": request.connectors})
            return _ok(rid, connector_ui_payload(_operation_view(start.operation)))
        if not account.wait_for_prepare(start) or start.failed.is_set():
            return _connector_rpc_error(rid, 5034, _reason("connector_request_failed"), "Connector request failed. Try again explicitly.")
        return _ok(rid, connector_ui_payload(_operation_view(start.operation)))
    except ValueError:
        return _connector_rpc_error(rid, 4000, _reason("invalid_params"), "Connector parameters are invalid.")
    except Exception:
        return _connector_rpc_error(rid, 5034, _reason("connector_request_failed"), "Connector request failed. Try again explicitly.")


def _connector_rpc(rid, params, action):
    from tui_gateway.contracts.connectors import ConnectorsConnectParams, ConnectorsListParams

    model = ConnectorsListParams if action == "status" else ConnectorsConnectParams
    request, session, error = _connector_params(rid, params, model)
    if error:
        return error
    if request.owner.type == "account":
        try:
            with _account_scope(request):
                return _account_connector_list(rid) if action == "status" else _account_connector_connect(rid, request)
        except Exception:
            return _connector_rpc_error(rid, 5034, _reason("connector_request_failed"), "Connector request failed. Try again explicitly.")

    runtime_token = _current_runtime_session_record.set(session)
    try:
        profile_home = session.get("profile_home")
        with _session_profile_runtime_scope({"profile_home": profile_home or str(_hermes_home)}):
            tokens = _set_session_context(session["session_key"], cwd=_session_cwd(session), ui_session_id=request.owner.session_id)
            try:
                result = _session_connector_rpc(rid, request, session, action)
            finally:
                _clear_session_context(tokens)
        if not _connector_owner_matches(request.owner.session_id, session, profile_home):
            return _connector_rpc_error(rid, 4001, _reason("not_owner"), "session ownership changed")
        return result
    except Exception:
        return _connector_rpc_error(rid, 5034, _reason("connector_request_failed"), "Connector request failed. Try again explicitly.")
    finally:
        _current_runtime_session_record.reset(runtime_token)


def _reissue(rid, operation, args):
    from tools.connectors.contract import TargetState, allowed
    from tui_gateway.connector_payload import connector_ui_payload

    targets = [operation.target(name) for name in args["connectors"]]
    if any(target is None for target in targets):
        return _connector_rpc_error(rid, 4004, _reason("unknown_target"), "No such target on the open operation.")
    if len({target.kind for target in targets}) != 1:
        return _connector_rpc_error(rid, 4000, _reason("invalid_params"), "One target kind per request.")
    stale = [target.name for target in targets if target.state in (TargetState.failed, TargetState.expired)]
    if len(stale) != len(targets):
        return _connector_rpc_error(rid, 4002, _reason("link_still_valid"), "Reopen the stored link.")
    if operation.settled:
        return _connector_rpc_error(rid, 4002, _reason("reissue_refused"), "The operation has settled.")
    if any(allowed(target.kind, target.state, TargetState.initiated) is None for target in targets):
        return _connector_rpc_error(rid, 4002, _reason("reissue_refused"), "This target cannot be run again.")
    error = _REISSUE_BY_KIND[targets[0].kind](operation, stale)
    if error:
        return _connector_rpc_error(rid, 4002, _reason("reissue_refused"), "The target cannot be run again.")
    return _ok(rid, connector_ui_payload(_operation_view(operation)))


def _remint_managed(operation, names):
    from tools.connectors.contract import Actor
    from tools.connectors.managed import managed_client, mint

    mint(managed_client(), operation, names, reinitiate=True, actor=Actor.user)
    return None


def _rerun_mcp(operation, names):
    from tools.connectors.mcp import retry

    return retry(operation, names)


_REISSUE_BY_KIND = {"connector": _remint_managed, "mcp": _rerun_mcp}


def _live_operation(rid, request, session):
    from tools.connectors import live

    if request.owner.type == "session":
        operation = live.get(session["session_key"], request.op_id, profile_home=session.get("profile_home"))
    else:
        profile_home = _profile_home(request.profile)
        operation = live.get_by_op_id(request.op_id, profile_home=str(profile_home) if profile_home else None)
    if operation is None:
        return None, _connector_rpc_error(rid, 4004, _reason("unknown_operation"), "No open operation with that op_id.")
    return operation, None


def _operation_params(rid, params):
    from tui_gateway.contracts.connectors_operation import ConnectionOperationParams

    return _connector_params(rid, params, ConnectionOperationParams)


@method("connectors.list")
def _(rid, params):
    try:
        return _connector_rpc(rid, params, "status")
    except Exception:
        return _connector_rpc_error(rid, 5034, _reason("connector_request_failed"), "Connector request failed. Try again explicitly.")


@method("connectors.connect")
def _(rid, params):
    try:
        return _connector_rpc(rid, params, "connect")
    except Exception:
        return _connector_rpc_error(rid, 5034, _reason("connector_request_failed"), "Connector request failed. Try again explicitly.")


def _account_scope(request):
    profile_home = _profile_home(request.profile)
    return _session_profile_runtime_scope({"profile_home": str(profile_home) if profile_home else None})


def _operation_for_request(rid, request, session):
    if request.owner.type != "account":
        return _live_operation(rid, request, session)
    try:
        with _account_scope(request):
            return _live_operation(rid, request, None)
    except Exception:
        return None, _connector_rpc_error(rid, 5034, _reason("connector_request_failed"), "Connector request failed. Try again explicitly.")


@method("connectors.operation.status")
def _(rid, params):
    from tui_gateway.connector_payload import connector_ui_payload

    try:
        request, session, error = _operation_params(rid, params)
        if error:
            return error
        operation, error = _operation_for_request(rid, request, session)
        return error or _ok(rid, connector_ui_payload(_operation_view(operation)))
    except Exception:
        return _connector_rpc_error(rid, 5034, _reason("connector_request_failed"), "Connector request failed. Try again explicitly.")


@method("connectors.operation.wake")
def _(rid, params):
    try:
        request, session, error = _operation_params(rid, params)
        if error:
            return error
        operation, error = _operation_for_request(rid, request, session)
        if error:
            return error
        operation.wake.set()
        return _ok(rid, {"status": "ok"})
    except Exception:
        return _connector_rpc_error(rid, 5034, _reason("connector_request_failed"), "Connector request failed. Try again explicitly.")


@method("connection.respond")
def _(rid, params):
    from tui_gateway.contracts.connectors_operation import ConnectionRespondParams

    try:
        request, session, error = _connector_params(rid, params, ConnectionRespondParams)
        if error:
            return error
        if request.owner.type == "account":
            with _account_scope(request):
                operation, error = _live_operation(rid, request, None)
                return error or _apply_connection_answer(rid, request, operation)
        operation, error = _live_operation(rid, request, session)
        return error or _apply_connection_answer(rid, request, operation)
    except Exception:
        return _connector_rpc_error(rid, 5034, _reason("connector_request_failed"), "Connector request failed. Try again explicitly.")


def _apply_connection_answer(rid, request, operation):
    from tools.connectors import live
    from tools.connectors.contract import SettleReason
    from tools.connectors.mcp import apply_answer
    from tools.connectors.operation import IllegalTransition

    try:
        apply_answer(operation, request.result.model_dump_json(exclude_none=True))
    except IllegalTransition:
        return _connector_rpc_error(rid, 4002, _reason("invalid_answer"), "Connection answer is invalid.")
    if not operation.settled and operation.all_resolved:
        operation.settle(SettleReason.all_resolved)
    if operation.settled:
        live.close(operation)
    return _ok(rid, {"status": "ok", "settled": operation.settled})


def _snapshot_view(snapshot):
    return {**snapshot, "settled": snapshot.get("settled_at") is not None}


def _operation_view(operation):
    return _snapshot_view(operation.result())


def _connection_update(operation, change, snapshot):
    """Emit a session update to its owner, or broadcast an account-operation update globally."""
    from tui_gateway import server

    payload = _snapshot_view(snapshot)
    if change:
        payload.update(change)
    if operation.session_key.startswith("account:"):
        payload["owner"] = {"type": "account"}
        server._broadcast_global_event("connection.update", payload)
        return
    with server._sessions_lock:
        sid = next((sid for sid, session in server._sessions.items() if session.get("session_key") == operation.session_key), None)
    if sid is not None:
        payload["owner"] = {"type": "session", "session_id": sid}
        server._emit("connection.update", sid, payload)


def _install_update_hook():
    from tools.connectors import operation as op_module

    if getattr(op_module.ConnectionOperation, "_update_hook_installed", False):
        return
    op_module.ConnectionOperation._update_hook_installed = True
    op_module.ConnectionOperation.on_change = staticmethod(_connection_update)


def register(server):
    bind_module(globals(), server, skip=("_",))
    server._LONG_HANDLERS = server._LONG_HANDLERS | _CONNECTOR_RPC_METHODS
    _install_update_hook()
