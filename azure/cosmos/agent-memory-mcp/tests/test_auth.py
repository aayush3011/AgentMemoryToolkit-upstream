from mcp.server.auth.provider import AccessToken
from mcp.server.auth.settings import AuthSettings

from agent_memory_mcp.auth import EntraTokenVerifier, build_auth, current_access_token, current_subject
from agent_memory_mcp.config import Settings


def test_build_auth_returns_none_pair_when_disabled():
    verifier, auth_settings = build_auth(Settings(auth_enabled=False))

    assert verifier is None
    assert auth_settings is None


def test_build_auth_constructs_verifier_and_auth_settings_without_network():
    settings = Settings(
        auth_enabled=True,
        entra_tenant_id="tenant-id",
        entra_audience="api://agent-memory",
        required_scopes="memory.read,memory.write",
        resource_server_url="https://mcp.example.com",
    )

    verifier, auth_settings = build_auth(settings)

    assert isinstance(verifier, EntraTokenVerifier)
    assert verifier._jwk_client is None
    assert isinstance(auth_settings, AuthSettings)
    assert str(auth_settings.issuer_url) == "https://login.microsoftonline.com/tenant-id/v2.0"
    assert str(auth_settings.resource_server_url).rstrip("/") == "https://mcp.example.com"
    assert auth_settings.required_scopes == ["memory.read", "memory.write"]


def test_current_access_token_handles_no_auth_context():
    assert current_access_token() is None


def test_current_subject_uses_configured_claim_then_subject(monkeypatch):
    token = AccessToken(
        token="raw",
        client_id="client",
        scopes=["memory.read"],
        expires_at=None,
        subject="subject-user",
        claims={"preferred_username": "claim-user"},
    )
    monkeypatch.setattr("agent_memory_mcp.auth.current_access_token", lambda: token)

    assert current_subject(Settings(user_id_claim="preferred_username")) == "claim-user"
    assert current_subject(Settings(user_id_claim="missing")) == "subject-user"


def test_current_subject_returns_none_without_token(monkeypatch):
    monkeypatch.setattr("agent_memory_mcp.auth.current_access_token", lambda: None)

    assert current_subject(Settings()) is None
