from agent_memory_mcp.config import Settings, get_settings


def test_settings_env_aliases_and_get_settings_cache(monkeypatch):
    get_settings.cache_clear()
    monkeypatch.setenv("COSMOS_DB_ENDPOINT", "https://env.documents.azure.com:443/")
    monkeypatch.setenv("COSMOS_DB_DATABASE", "envdb")
    monkeypatch.setenv("AI_FOUNDRY_ENDPOINT", "https://env.openai.azure.com/")
    monkeypatch.setenv("AGENT_MEMORY_MAX_TOP_K", "17")
    monkeypatch.setenv("AGENT_MEMORY_EXPOSE_GRANULAR", "true")
    monkeypatch.setenv("AGENT_MEMORY_ALLOW_USER_ID_ARG", "true")
    monkeypatch.setenv("AGENT_MEMORY_AUTO_CREATE", "true")

    settings = Settings(_env_file=None)

    assert settings.cosmos_endpoint == "https://env.documents.azure.com:443/"
    assert settings.cosmos_database == "envdb"
    assert settings.ai_foundry_endpoint == "https://env.openai.azure.com/"
    assert settings.max_top_k == 17
    assert settings.expose_granular is True
    assert settings.allow_user_id_arg is True
    assert settings.auto_create is True

    first = get_settings()
    second = get_settings()
    assert first is second
    assert first.cosmos_database == "envdb"
    get_settings.cache_clear()


def test_required_scope_list_parses_commas_and_spaces():
    settings = Settings(required_scopes="memory.read memory.write,admin")

    assert settings.required_scope_list() == ["memory.read", "memory.write", "admin"]


def test_required_scope_list_empty_when_unset():
    assert Settings(required_scopes=None).required_scope_list() == []
    assert Settings(required_scopes="  ,  ").required_scope_list() == []


def test_default_values_for_foundation_knobs():
    settings = Settings(_env_file=None)

    assert settings.max_top_k == 50
    assert settings.expose_granular is False
    assert settings.allow_user_id_arg is False
    assert settings.auto_create is False
