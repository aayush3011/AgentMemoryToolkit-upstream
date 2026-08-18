"""Configuration for the Agent Memory MCP server.

All settings are read from environment variables (or a local ``.env`` file). The
Cosmos DB / AI Foundry variable names deliberately match the ``azure-cosmos-agent-memory``
SDK's ``.env.template`` so a single ``.env`` configures both the SDK and this server.
MCP-specific knobs use the ``AGENT_MEMORY_MCP_*`` / ``AGENT_MEMORY_*`` prefixes.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal, Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Typed configuration for the MCP server, sourced from the environment."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
        populate_by_name=True,
    )

    # ---- Cosmos DB (shared with the SDK) ----
    cosmos_endpoint: Optional[str] = Field(default=None, alias="COSMOS_DB_ENDPOINT")
    cosmos_key: Optional[str] = Field(default=None, alias="COSMOS_DB_KEY")
    cosmos_database: str = Field(default="ai_memory", alias="COSMOS_DB_DATABASE")
    cosmos_memories_container: str = Field(default="memories", alias="COSMOS_DB_MEMORIES_CONTAINER")
    cosmos_summaries_container: str = Field(default="memories_summaries", alias="COSMOS_DB_SUMMARIES_CONTAINER")
    cosmos_turns_container: str = Field(default="memories_turns", alias="COSMOS_DB_TURNS_CONTAINER")
    cosmos_counters_container: str = Field(default="counter", alias="COSMOS_DB_COUNTERS_CONTAINER")
    cosmos_lease_container: str = Field(default="leases", alias="COSMOS_DB_LEASE_CONTAINER")

    # ---- AI Foundry / Azure OpenAI (shared with the SDK) ----
    ai_foundry_endpoint: Optional[str] = Field(default=None, alias="AI_FOUNDRY_ENDPOINT")
    ai_foundry_api_key: Optional[str] = Field(default=None, alias="AI_FOUNDRY_API_KEY")
    embedding_deployment_name: str = Field(
        default="text-embedding-3-large", alias="AI_FOUNDRY_EMBEDDING_DEPLOYMENT_NAME"
    )
    embedding_dimensions: Optional[int] = Field(default=None, alias="AI_FOUNDRY_EMBEDDING_DIMENSIONS")
    chat_deployment_name: str = Field(default="gpt-4o-mini", alias="AI_FOUNDRY_CHAT_DEPLOYMENT_NAME")
    enable_turn_embeddings: bool = Field(default=False, alias="ENABLE_TURN_EMBEDDINGS")

    # ---- Processor ownership (see SDK README "Backend exclusivity") ----
    # "durable"  -> defer processing to the sibling Function app (thin-writer mode);
    #               process_thread becomes a no-op here.
    # otherwise  -> run the in-process pipeline; process_thread runs inline.
    processor_owner: Optional[str] = Field(default=None, alias="MEMORY_PROCESSOR_OWNER")

    # ---- MCP transport / server ----
    transport: Literal["stdio", "streamable-http"] = Field(default="stdio", alias="AGENT_MEMORY_MCP_TRANSPORT")
    host: str = Field(default="127.0.0.1", alias="AGENT_MEMORY_MCP_HOST")
    port: int = Field(default=8080, alias="AGENT_MEMORY_MCP_PORT")
    streamable_http_path: str = Field(default="/mcp", alias="AGENT_MEMORY_MCP_HTTP_PATH")
    stateless_http: bool = Field(default=True, alias="AGENT_MEMORY_MCP_STATELESS_HTTP")
    log_level: str = Field(default="INFO", alias="AGENT_MEMORY_MCP_LOG_LEVEL")

    # ---- Scoping / multi-tenancy ----
    default_user_id: Optional[str] = Field(default=None, alias="AGENT_MEMORY_DEFAULT_USER_ID")
    # When auth is ENABLED, honor a client-supplied user_id that differs from the
    # token identity only if this is true (trusted server-to-server). When auth is
    # DISABLED (local/stdio) a client-supplied user_id is always accepted.
    allow_user_id_arg: bool = Field(default=False, alias="AGENT_MEMORY_ALLOW_USER_ID_ARG")

    # ---- Tool surface ----
    expose_granular: bool = Field(default=False, alias="AGENT_MEMORY_EXPOSE_GRANULAR")
    max_top_k: int = Field(default=50, alias="AGENT_MEMORY_MAX_TOP_K")

    # ---- Provisioning ----
    # False: assume containers are pre-provisioned (recommended; `azd`/infra owns them).
    # True: create database + containers on startup if missing (local convenience).
    auto_create: bool = Field(default=False, alias="AGENT_MEMORY_AUTO_CREATE")

    # ---- Auth (hosted / Streamable HTTP) ----
    auth_enabled: bool = Field(default=False, alias="AGENT_MEMORY_AUTH_ENABLED")
    entra_tenant_id: Optional[str] = Field(default=None, alias="ENTRA_TENANT_ID")
    entra_audience: Optional[str] = Field(default=None, alias="ENTRA_AUDIENCE")
    resource_server_url: Optional[str] = Field(default=None, alias="AGENT_MEMORY_RESOURCE_SERVER_URL")
    # JWT claim that identifies the memory owner (Entra: "oid" is the stable object id).
    user_id_claim: str = Field(default="oid", alias="AGENT_MEMORY_USER_ID_CLAIM")
    # Space- or comma-separated scopes required on the access token.
    required_scopes: Optional[str] = Field(default=None, alias="AGENT_MEMORY_REQUIRED_SCOPES")

    def required_scope_list(self) -> list[str]:
        """Parse ``required_scopes`` into a list (comma or whitespace separated)."""
        raw = self.required_scopes or ""
        return [s for s in raw.replace(",", " ").split() if s]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide :class:`Settings`, loaded once and cached."""
    return Settings()
