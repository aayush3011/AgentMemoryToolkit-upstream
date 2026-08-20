"""Tests for the gateway auth dependency (APIM subscription-key validation).

The dependency is env-gated by APIM_VALIDATE_KEY_URL. When unset it is a no-op
(local/dev); when set it POSTs to the validate-key URL and maps 204/403/other to
pass / 403 / 401. httpx is stubbed so no network call is made.
"""

import app.core.auth as auth_module


class _FakeResponse:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code


class _FakeAsyncClient:
    """Minimal async-context-manager stand-in for httpx.AsyncClient."""

    def __init__(self, status_code: int) -> None:
        self._status_code = status_code

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def post(self, *args, **kwargs):
        return _FakeResponse(self._status_code)


def _patch_validator(monkeypatch, status_code: int) -> None:
    monkeypatch.setenv("APIM_VALIDATE_KEY_URL", "https://apim.example/validateKey")
    monkeypatch.setattr(auth_module.httpx, "AsyncClient", lambda *a, **k: _FakeAsyncClient(status_code))


def test_noop_when_url_unset(api, mock_memory, monkeypatch):
    monkeypatch.delenv("APIM_VALIDATE_KEY_URL", raising=False)
    mock_memory.search_cosmos.return_value = []

    # No auth headers, yet the request succeeds because auth is not configured.
    response = api.post("/users/u1/search", json={"query": "hi"})

    assert response.status_code == 200


def test_missing_headers_returns_401(api, monkeypatch):
    monkeypatch.setenv("APIM_VALIDATE_KEY_URL", "https://apim.example/validateKey")

    response = api.post("/users/u1/search", json={"query": "hi"})

    assert response.status_code == 401
    assert response.headers["content-type"].startswith("application/problem+json")


def test_valid_key_204_passes(api, mock_memory, monkeypatch):
    _patch_validator(monkeypatch, 204)
    mock_memory.search_cosmos.return_value = []

    response = api.post(
        "/users/u1/search",
        json={"query": "hi"},
        headers={"Ocp-Apim-Subscription-Key": "k", "x-requested-account": "u1"},
    )

    assert response.status_code == 200
    mock_memory.search_cosmos.assert_awaited_once()


def test_forbidden_key_403(api, monkeypatch):
    _patch_validator(monkeypatch, 403)

    response = api.post(
        "/users/u1/search",
        json={"query": "hi"},
        headers={"Ocp-Apim-Subscription-Key": "k", "x-requested-account": "other"},
    )

    assert response.status_code == 403
    assert response.headers["content-type"].startswith("application/problem+json")


def test_invalid_key_maps_to_401(api, monkeypatch):
    _patch_validator(monkeypatch, 401)

    response = api.post(
        "/users/u1/search",
        json={"query": "hi"},
        headers={"Ocp-Apim-Subscription-Key": "bad", "x-requested-account": "u1"},
    )

    assert response.status_code == 401


def test_validator_unreachable_returns_503(api, monkeypatch):
    monkeypatch.setenv("APIM_VALIDATE_KEY_URL", "https://apim.example/validateKey")

    class _RaisingClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def post(self, *args, **kwargs):
            raise auth_module.httpx.ConnectError("boom")

    monkeypatch.setattr(auth_module.httpx, "AsyncClient", lambda *a, **k: _RaisingClient())

    response = api.post(
        "/users/u1/search",
        json={"query": "hi"},
        headers={"Ocp-Apim-Subscription-Key": "k", "x-requested-account": "u1"},
    )

    assert response.status_code == 503


def test_health_is_open_even_when_auth_configured(api, monkeypatch):
    monkeypatch.setenv("APIM_VALIDATE_KEY_URL", "https://apim.example/validateKey")

    response = api.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
