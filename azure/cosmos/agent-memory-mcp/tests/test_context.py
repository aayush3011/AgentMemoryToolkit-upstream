import pytest
from azure.cosmos.agent_memory.exceptions import ValidationError

from agent_memory_mcp.config import Settings
from agent_memory_mcp.context import (
    AppContext,
    Deps,
    get_app,
    get_client,
    resolve_thread_id,
    resolve_user_id,
)


class Session:
    pass


def _deps(settings):
    return Deps(settings=settings)


def test_resolve_user_id_prefers_explicit_then_default():
    deps = _deps(Settings(auth_enabled=False, default_user_id="default-user"))

    assert resolve_user_id(deps, "explicit-user") == "explicit-user"
    assert resolve_user_id(deps, None) == "default-user"


def test_resolve_user_id_raises_validation_error_when_unavailable():
    deps = _deps(Settings(auth_enabled=False, default_user_id=None))

    with pytest.raises(ValidationError, match="user_id is required"):
        resolve_user_id(deps, None)


def test_resolve_user_id_enforces_auth_identity(monkeypatch):
    monkeypatch.setattr("agent_memory_mcp.context.current_subject", lambda settings: "token-user")
    deps = _deps(Settings(auth_enabled=True, default_user_id="default-user", allow_user_id_arg=False))

    assert resolve_user_id(deps, None) == "token-user"
    assert resolve_user_id(deps, "token-user") == "token-user"
    with pytest.raises(ValidationError, match="does not match"):
        resolve_user_id(deps, "other-user")


def test_resolve_user_id_allows_auth_override_when_configured(monkeypatch):
    monkeypatch.setattr("agent_memory_mcp.context.current_subject", lambda settings: "token-user")
    deps = _deps(Settings(auth_enabled=True, allow_user_id_arg=True))

    assert resolve_user_id(deps, "other-user") == "other-user"


def test_resolve_thread_id_handles_explicit_and_required():
    assert resolve_thread_id("explicit-thread") == "explicit-thread"
    assert resolve_thread_id(None, required=False) is None
    with pytest.raises(ValidationError, match="thread_id is required"):
        resolve_thread_id(None)


def test_get_app_and_get_client_return_lifespan_context(make_ctx, mock_client, settings):
    ctx = make_ctx(mock_client, session=Session())

    assert isinstance(get_app(ctx), AppContext)
    assert get_app(ctx).settings is settings
    assert get_client(ctx) is mock_client
