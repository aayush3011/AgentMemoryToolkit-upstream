# Azure Cosmos DB Agent Memory MCP Server

`azure-cosmos-agent-memory-mcp` is a Model Context Protocol (MCP) server that exposes the [Azure Cosmos DB Agent Memory Toolkit](../../../README.md) to MCP-capable agents over local stdio or hosted Streamable HTTP. It is for agent hosts and orchestrators that need durable user/thread memory, semantic recall, and explicit consolidation without calling the Python SDK directly.

## Architecture snapshot

The server is a thin FastMCP wrapper over the async SDK (`AsyncCosmosMemoryClient`). One shared client is created at process startup, tools resolve the effective `(user_id, thread_id)`, and the SDK writes to the Agent Memory Toolkit Cosmos topology:

```text
MCP client ── stdio or Streamable HTTP (+ Entra JWT) ── agent-memory-mcp
                                                            │
                                                            ▼
                                            AsyncCosmosMemoryClient
                                                            │
                         memories_turns │ memories │ memories_summaries
                                                            │
                                  optional Durable Functions consolidation
```

The topology uses three primary containers for raw turns, durable memories, and summaries; the SDK also uses counter/lease containers for background processing. Hosted deployments should run in thin-writer mode (`MEMORY_PROCESSOR_OWNER=durable`) so Durable Functions owns change-feed consolidation. See [DESIGN.md](DESIGN.md) for the full rationale.

## Prerequisites

- Python 3.11+ (the repo development venv uses Python 3.13).
- An Azure Cosmos DB for NoSQL account with the Agent Memory Toolkit containers provisioned. See the parent [SDK README](../../../README.md) and [Docs](../../../Docs/README.md).
- Azure credentials available through `DefaultAzureCredential` or managed identity. Account keys/API keys are optional local-development fallbacks.
- Azure AI Foundry / Azure OpenAI endpoint plus embedding and chat deployments required by the SDK processing/search pipeline.

## Install

From this package directory:

```bash
pip install .
# Include Entra JWT verification dependencies for remote HTTP auth:
pip install '.[auth]'
```

This installs the `agent-memory-mcp` console script. You can also run the module directly:

```bash
python -m agent_memory_mcp
```

## Configuration

Settings are read from environment variables and an optional local `.env` file. Use [.env.template](.env.template) for the complete list.

| Variable | Default | Purpose |
|----------|---------|---------|
| `COSMOS_DB_ENDPOINT` | unset | Cosmos DB account endpoint. |
| `COSMOS_DB_KEY` | unset | Optional local key; omit for `DefaultAzureCredential`/managed identity. |
| `COSMOS_DB_DATABASE` | `ai_memory` | Memory database. |
| `COSMOS_DB_MEMORIES_CONTAINER` | `memories` | Durable fact/procedural/episodic container. |
| `COSMOS_DB_SUMMARIES_CONTAINER` | `memories_summaries` | Thread/user summary container. |
| `COSMOS_DB_TURNS_CONTAINER` | `memories_turns` | Raw conversation turns container. |
| `COSMOS_DB_COUNTERS_CONTAINER` | `counter` | Processing counter container. |
| `COSMOS_DB_LEASE_CONTAINER` | `leases` | Change-feed lease container. |
| `AI_FOUNDRY_ENDPOINT` | unset | Azure AI Foundry / Azure OpenAI endpoint. |
| `AI_FOUNDRY_API_KEY` | unset | Optional local API key; omit for managed identity where supported. |
| `AI_FOUNDRY_EMBEDDING_DEPLOYMENT_NAME` | `text-embedding-3-large` | Embedding deployment. |
| `AI_FOUNDRY_EMBEDDING_DIMENSIONS` | unset | Optional embedding dimensions override. |
| `AI_FOUNDRY_CHAT_DEPLOYMENT_NAME` | `gpt-4o-mini` | Chat deployment for processing. |
| `ENABLE_TURN_EMBEDDINGS` | `false` | Enables embeddings for raw turns and `search_turns`. |
| `MEMORY_PROCESSOR_OWNER` | unset | Set `durable` for hosted thin-writer mode; otherwise SDK in-process behavior applies. |
| `AGENT_MEMORY_MCP_TRANSPORT` | `stdio` | `stdio` or `streamable-http`. |
| `AGENT_MEMORY_MCP_HOST` | `127.0.0.1` | HTTP bind host. |
| `AGENT_MEMORY_MCP_PORT` | `8080` | HTTP bind port. |
| `AGENT_MEMORY_MCP_HTTP_PATH` | `/mcp` | Streamable HTTP path. |
| `AGENT_MEMORY_MCP_STATELESS_HTTP` | `true` | Run HTTP sessions statelessly. |
| `AGENT_MEMORY_MCP_LOG_LEVEL` | `INFO` | Server log level. |
| `AGENT_MEMORY_DEFAULT_USER_ID` | unset | Local fallback owner when auth is disabled. |
| `AGENT_MEMORY_ALLOW_USER_ID_ARG` | `false` | Trusted override for client-supplied `user_id` under auth. |
| `AGENT_MEMORY_EXPOSE_GRANULAR` | `false` | Exposes advanced granular processing tools. |
| `AGENT_MEMORY_MAX_TOP_K` | `50` | Server cap for `top_k`/`recent_k` style limits. |
| `AGENT_MEMORY_AUTO_CREATE` | `false` | Create database/containers at startup when missing. |
| `AGENT_MEMORY_AUTH_ENABLED` | `false` | Enable Entra ID JWT auth for HTTP mode. |
| `ENTRA_TENANT_ID` | unset | Entra tenant used to validate JWT issuer/JWKS. |
| `ENTRA_AUDIENCE` | unset | Expected JWT audience. |
| `AGENT_MEMORY_RESOURCE_SERVER_URL` | unset | Optional OAuth resource metadata URL. |
| `AGENT_MEMORY_USER_ID_CLAIM` | `oid` | JWT claim used as memory owner id. |
| `AGENT_MEMORY_REQUIRED_SCOPES` | unset | Space/comma-separated scopes or app roles required on tokens. |

## Running locally

### stdio for local MCP clients

```bash
cd azure/cosmos/agent-memory-mcp
export AGENT_MEMORY_MCP_TRANSPORT=stdio
export AGENT_MEMORY_DEFAULT_USER_ID=local-user
agent-memory-mcp
# or: python -m agent_memory_mcp
```

In stdio/local mode, set `AGENT_MEMORY_DEFAULT_USER_ID` (or pass `user_id` explicitly) and pass `thread_id` per call.

### Streamable HTTP for hosted/remote clients

```bash
cd azure/cosmos/agent-memory-mcp
export AGENT_MEMORY_MCP_TRANSPORT=streamable-http
export AGENT_MEMORY_MCP_HOST=0.0.0.0
export AGENT_MEMORY_MCP_PORT=8080
export AGENT_MEMORY_AUTH_ENABLED=true
export ENTRA_TENANT_ID=<tenant-id>
export ENTRA_AUDIENCE=<application-id-uri-or-client-id>
export AGENT_MEMORY_REQUIRED_SCOPES='Memory.ReadWrite'
export MEMORY_PROCESSOR_OWNER=durable
agent-memory-mcp
```

The server listens on `http://<host>:<port><AGENT_MEMORY_MCP_HTTP_PATH>`; default path is `/mcp`.

## Tool catalog

Identity is normally resolved from the authenticated token in HTTP mode, or from `AGENT_MEMORY_DEFAULT_USER_ID` in local mode, so most calls can omit `user_id`. Pass `thread_id` explicitly for turn-scoped operations.

### Session

| Tool | Purpose | Key parameters |
|------|---------|----------------|
| `whoami` | Return the effective `user_id` and whether auth is enabled. | none |

### Capture

| Tool | Purpose | Key parameters |
|------|---------|----------------|
| `add_memory` | Store any memory: a raw `turn` (needs `role` + `thread_id`) or a durable `fact`, `episodic`, or `procedural` memory. | `content`, `memory_type`, `role?`, `thread_id?`, `metadata?`, `tags?`, `salience?` |

### Retrieval and search

| Tool | Purpose | Key parameters |
|------|---------|----------------|
| `search_memories` | Primary semantic/hybrid recall over durable memories; can include turns. | `query`, `memory_types?`, `top_k`, `min_confidence?`, `tags_any?`, `tags_all?`, `exclude_tags?`, `include_turns` |
| `get_memories` | Deterministic filtered fetch without a query. | `memory_types?`, `recent_k?`, `min_confidence?`, `min_salience?`, tag filters |
| `recall_thread` | Return raw turns for one thread, oldest-first. | `thread_id?`, `recent_k?` |
| `search_turns` | Semantic search over raw turns; requires `ENABLE_TURN_EMBEDDINGS=true`. | `query`, `thread_id?`, `top_k` |

### Profiles and durable context

| Tool | Purpose | Key parameters |
|------|---------|----------------|
| `get_user_summary` | Fetch the cross-thread user profile. | `user_id?` |
| `get_thread_summary` | Fetch the latest summary for a thread. | `thread_id?`, `recent_k?` |

### Lifecycle and correction

| Tool | Purpose | Key parameters |
|------|---------|----------------|
| `update_memory` | Correct or revise an existing memory. | `memory_id`, `memory_type`, `content?`, `role?`, `metadata?`, `thread_id?` |
| `delete_memory` | Permanently delete a memory. | `memory_id`, `memory_type`, `thread_id?` |
| `get_memory_history` | Inspect the version lineage of a memory. | `memory_id`, `thread_id?`, `max_depth` |

### Consolidation

| Tool | Purpose | Key parameters |
|------|---------|----------------|
| `process_thread` | Force summary, extraction, reconciliation, procedural, and user-profile processing for a thread. In durable thin-writer deployments, change-feed processing is the owner. | `thread_id?` |
| `summarize_thread` | Advanced: regenerate a thread summary. | `thread_id?`, `recent_k?` |
| `extract_memories` | Advanced: extract durable memories from a thread. | `thread_id?`, `recent_k?` |
| `update_user_profile` | Advanced: regenerate the user profile. | `thread_ids?`, `recent_k?` |
| `reconcile_memories` | Advanced: deduplicate/supersede recent durable memories. | `n?` |

The four granular tools are registered only when `AGENT_MEMORY_EXPOSE_GRANULAR=true`; they are off by default.

## Deployment

Use the package [infra README](infra/README.md) for hosted deployment assets: Container Apps ingress for Streamable HTTP, managed identity, Cosmos DB data-plane RBAC, and environment wiring. Keep the MCP app keyless where possible and grant least-privilege Cosmos roles to the managed identity. The broader SDK provisioning guidance is in the parent [infra README](../../../infra/README.md) and [Azure testing docs](../../../Docs/azure_testing.md).

## Development

From this package directory, using the repo venv:

```bash
/Users/aayushkataria/Microsoft/git/AgentMemoryToolkit-v2/.venv313/bin/python -m pytest -q
/Users/aayushkataria/Microsoft/git/AgentMemoryToolkit-v2/.venv313/bin/python -m ruff check .
```

Unit tests register tools against a fake FastMCP catcher and mocked async memory client, so no live Cosmos DB account is needed for the default test run.

## Security notes

- In hosted HTTP mode, Entra JWT signature, issuer, audience, expiry, and optional scopes/roles are validated before tools run.
- The memory owner is derived from `AGENT_MEMORY_USER_ID_CLAIM` (default `oid`) or the token subject. Client-supplied `user_id` cannot cross that boundary unless `AGENT_MEMORY_ALLOW_USER_ID_ARG=true`.
- Enable `AGENT_MEMORY_ALLOW_USER_ID_ARG` only for trusted service-to-service callers that enforce tenant isolation upstream.
- Prefer managed identity / `DefaultAzureCredential`; do not bake Cosmos keys or AI keys into images.
- Use least-privilege Cosmos RBAC and treat stored memory as user data. Use `delete_memory` for deletion requests.
