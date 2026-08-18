# Agent Memory MCP Server - Design

> **Status:** Draft / RFC · **Component path:** `azure/cosmos/agent-memory-mcp/`
> **Depends on:** `azure-cosmos-agent-memory` SDK (this repo)

An **MCP (Model Context Protocol) server** that exposes the Agent Memory Toolkit as a
set of tools any MCP-capable agent host (Claude Desktop, VS Code / GitHub Copilot,
Foundry Agents, custom orchestrators) can call to **remember**, **recall**, and
**consolidate** long-term memory backed by Azure Cosmos DB.

---

## 1. Goals & non-goals

### Goals
- Wrap the existing `AsyncCosmosMemoryClient` behind a small, well-described,
  **agent-facing** tool surface - not a 1:1 mirror of every SDK method.
- Ship a **single codebase** that runs as **stdio** (local dev, single user) and
  **Streamable HTTP** (remote, multi-tenant, hosted) - selectable by config.
- Be **secure by default** in hosted mode: derive the memory owner (`user_id`)
  from the authenticated identity so an agent cannot read another tenant's memory.
- Be a **thin, stateless** service so hosted replicas scale horizontally; defer
  heavy LLM processing to the sibling Durable Function app where available.
- Align conventions with the existing [Azure Cosmos DB MCP Toolkit](https://github.com/AzureCosmosDB/MCPToolKit)
  (Container Apps hosting, Entra ID auth, `azd up`, Bicep) so the two feel like a family.

### Non-goals
- Not a generic Cosmos DB data-plane server (that is what the existing MCP Toolkit
  is for). This server speaks **memory**: turns, facts, summaries, procedural &
  episodic memory - not `list_databases` / raw SQL.
- Not a replacement for the SDK or the Durable Function app; it is a **transport
  adapter** over them.
- No new persistence model, no new memory types. It reuses the 3-container topology.

---

## 2. Positioning vs. the generic Cosmos DB MCP Toolkit

| | **Cosmos DB MCP Toolkit** (existing) | **Agent Memory MCP** (this) |
|---|---|---|
| Layer | Raw data plane | Memory domain (turns/facts/summaries/procedural/episodic) |
| Tools | `list_databases`, `vector_search`, `hybrid_search`, `find_document_by_id`… | `search_memories`, `add_memory`, `get_user_summary`, `process_thread`… |
| Impl | .NET 9 | **Python** (reuses `AsyncCosmosMemoryClient`) |
| Semantics | "query my database" | "remember / recall / consolidate for this user" |
| Hosting | Container Apps + Entra ID | **Same** (family alignment) |

They are complementary: the memory server offers the higher-level "give my agent a
brain" experience; the data-plane server offers general-purpose querying.

---

## 3. Architecture

```
        MCP client (Copilot / Claude / Foundry agent / custom host)
                 │  JSON-RPC over stdio  OR  Streamable HTTP (+ Entra JWT)
                 ▼
        ┌───────────────────────────────────────────┐
        │  agent-memory-mcp  (Python, FastMCP)        │
        │  • tool registry (§6)                       │
        │  • auth middleware → resolves user_id (§5)  │
        │  • one shared AsyncCosmosMemoryClient       │
        │  • lifespan: connect on startup / close     │
        └───────────────┬─────────────────────────────┘
                        │ async SDK calls
                        ▼
        ┌───────────────────────┐     ┌──────────────────────────┐
        │ Azure Cosmos DB        │◄───►│ Azure Durable Functions  │
        │ memories / turns /     │     │ (optional) change-feed   │
        │ summaries (+counter)   │     │ processing pipeline      │
        └───────────────────────┘     └──────────────────────────┘
                        ▲
                        │ embeddings + LLM
                        ▼
                Azure AI Foundry / Azure OpenAI
```

- **Framework:** the official Python **MCP SDK** (`mcp`, `FastMCP`). One set of
  `@mcp.tool()` definitions is served over `stdio` or `streamable-http` by flipping
  a transport flag - no duplicate code.
- **Client reuse:** exactly **one** `AsyncCosmosMemoryClient` per process, created
  in the FastMCP **lifespan** (connect on startup, `close()` on shutdown). Cosmos and
  AI Foundry clients are connection-pooled and shared across tool calls.
- **Async-native:** every tool is `async` and awaits the async SDK - no thread pool.

### 3.1 Processing ownership (important)
Hosted MCP replicas are stateless and may be many. Running the in-process LLM
pipeline inside every replica would multiply cost and risk double-processing.

- **Hosted / production:** deploy the MCP server as a **thin writer** -
  `processor=DurableFunctionProcessor()` and `MEMORY_PROCESSOR_OWNER=durable`. The
  server writes turns; the sibling **Durable Function app** consolidates off the
  change feed. `process_*` tools become explicit "nudge/no-op" hooks (§6.5).
- **Local / stdio / no function app:** `processor=InProcessProcessor()` and
  `MEMORY_PROCESSOR_OWNER=inprocess`. `process_thread` runs the full pipeline inline.

This mirrors the SDK's existing backend-exclusivity contract - see the README
"Backend exclusivity (`MEMORY_PROCESSOR_OWNER`)" section.

---

## 4. Package layout

```
azure/cosmos/agent-memory-mcp/
├── DESIGN.md                     # this document
├── README.md                     # quickstart (stdio + hosted)
├── pyproject.toml                # dist: azure-cosmos-agent-memory-mcp
├── src/agent_memory_mcp/
│   ├── __init__.py
│   ├── server.py                 # FastMCP app + lifespan (shared client)
│   ├── config.py                 # env → settings (pydantic-settings)
│   ├── auth.py                   # JWT validation + user_id resolution (§5)
│   ├── context.py                # per-request scope (user_id/thread_id) helpers
│   ├── serialization.py          # SDK dict → compact tool payload (§7)
│   ├── errors.py                 # SDK exception → MCP error mapping (§8)
│   └── tools/
│       ├── session.py            # whoami
│       ├── capture.py            # add_memory
│       ├── retrieval.py          # search_memories, get_memories, recall_thread, search_turns
│       ├── profile.py            # get_user_summary, get_thread_summary
│       ├── lifecycle.py          # update_memory, delete_memory, get_memory_history
│       └── processing.py         # process_thread (+ granular optional)
├── Dockerfile                    # Container Apps image (hosted mode)
├── infra/                        # Bicep (Container App, ingress, identity, RBAC)
└── tests/
```

- Standalone **deployable app** (like `function_app/`), not an importable submodule
  of `azure.cosmos.*` (the folder is hyphenated on purpose). It depends on the
  published `azure-cosmos-agent-memory` SDK.
- Distribution name: `azure-cosmos-agent-memory-mcp`; import package
  `agent_memory_mcp`.

---

## 5. Identity & scoping model (the key security decision)

Memory is partitioned by hierarchical **`(user_id, thread_id)`**. How those are
resolved differs by transport:

| Field | Hosted (Streamable HTTP) | Local (stdio) |
|-------|--------------------------|----------------|
| `user_id` | **Derived from the Entra JWT** (`oid`/`sub` claim). Never accepted as a tool arg → prevents cross-tenant reads. | From `AGENT_MEMORY_DEFAULT_USER_ID` env, or accepted as a tool arg (single-user trust). |
| `thread_id` | Supplied by the agent per call (maps to the conversation). | Same. |

- A configurable **`AGENT_MEMORY_ALLOW_USER_ID_ARG=false`** guard enforces the
  "never trust client-supplied user_id" rule in hosted mode; set `true` only for
  trusted server-to-server callers.

This is the single most important design point: **in multi-tenant mode the owner is
the token, not a parameter.**

---

## 6. Tool surface

Design principles:
- **Curated, not exhaustive.** 12 core tools grouped by intent. Fewer, well-named,
  richly-described tools ⇒ better model tool-selection than 30 thin wrappers.
- **Verbs the agent thinks in:** *add, recall, search, summarize, delete.*
- **Scope args:** pass `thread_id` explicitly for turn-scoped ops. `user_id` per §5.
- Omitted on purpose: local-buffer methods (`add_local`/`push_to_cosmos`),
  `create_memory_store`/`connect_cosmos`/`validate_topology` (ops/admin, not agent
  actions), raw `add_cosmos(memory_type="turn")` embedding knobs.

### 6.1 Capture (write)

| Tool | Wraps | Purpose | Key args |
|------|-------|---------|----------|
| `add_memory` | `add_cosmos` | Store any memory. `memory_type="turn"` records a raw conversation message (needs `role` + `thread_id`); `fact`/`episodic`/`procedural` assert a durable memory the agent already knows. | `content`, `memory_type="fact"`, `role?`, `thread_id?`, `tags?`, `salience?`, `metadata?` |

### 6.2 Retrieval & search (read)

| Tool | Wraps | Purpose | Key args |
|------|-------|---------|----------|
| `search_memories` | `search_cosmos` | **Primary recall.** Hybrid vector + full-text over facts/episodic/procedural; optional blend of raw turns. | `query`, `memory_types?`, `top_k=5`, `min_confidence?`, `tags_any?/all?`, `include_turns?` |
| `get_memories` | `get_memories` | Deterministic filtered fetch (no query) - by type, tags, confidence, salience, time window, `recent_k`. | `memory_types?`, `recent_k?`, `min_confidence?`, `created_after/before?` |
| `recall_thread` | `get_thread` | Raw turns of a thread, oldest-first, for rehydrating context. | `thread_id`, `recent_k?` |
| `search_turns` | `search_turns` | Semantic search over **raw turns** (requires turn embeddings). | `query`, `thread_id?`, `top_k=5` |

### 6.3 Profiles & durable context (read)

| Tool | Wraps | Purpose |
|------|-------|---------|
| `get_user_summary` | `get_user_summary` | Cross-thread profile of the user - call at session start. |
| `get_thread_summary` | `get_thread_summary` | Latest rollup(s) for one thread. |

### 6.4 Lifecycle & correction (write)

| Tool | Wraps | Purpose |
|------|-------|---------|
| `update_memory` | `update_cosmos` | Correct/annotate an existing memory's content or metadata. |
| `delete_memory` | `delete_cosmos` | Hard-delete a memory (needs `memory_id` + `memory_type`; `thread_id` for turns). |
| `get_memory_history` | `get_memory_history` | Walk the supersede chain - "what did this used to be / what changed?" |

### 6.5 Consolidation / processing (write, heavy)

| Tool | Wraps | Behavior by mode |
|------|-------|------------------|
| `process_thread` | `process_now` | **In-process:** runs summary→extract→reconcile→procedural→user-summary now. **Durable:** documented no-op (change feed drives it). |
| *(optional, advanced)* `summarize_thread` / `extract_memories` / `update_user_profile` / `reconcile_memories` | `generate_thread_summary` / `extract_memories` / `generate_user_summary` / `reconcile` | Granular pipeline steps; gated behind `AGENT_MEMORY_EXPOSE_GRANULAR=false` to keep the default surface small. |

### 6.6 Session utility

| Tool | Purpose |
|------|---------|
| `whoami` | Report the resolved `user_id` and whether auth is enabled - confirm identity before reading/writing. |

**Core surface = §6.1–6.4 + `process_thread` + `whoami` = 12 tools.** Granular
processing tools are opt-in.

---

## 7. Response shaping

- SDK returns verbose Cosmos dicts (embeddings, `_etag`, `_rid`, internal fields).
  `serialization.py` projects each record to a **compact, token-lean** payload:
  `{id, memory_type, content, role?, tags?, confidence?, salience?, created_at,
  thread_id, score?}` - **embeddings and Cosmos system fields stripped**.
- Lists return `{items: [...], count, truncated}` with a server-enforced `top_k`
  ceiling (e.g. 50) to protect the model's context window.
- Timestamps normalized to ISO-8601 UTC.

---

## 8. Error handling

Map SDK exceptions → structured MCP tool errors (never leak stack traces / endpoints):

| SDK exception | MCP result |
|---------------|------------|
| `ValidationError` | `invalid_params` with a fix hint (e.g. "thread_id required for turns"). |
| `MemoryNotFoundError` | Empty result / `not_found`, not a hard error. |
| `CosmosNotConnectedError`, `CosmosOperationError` | `unavailable` - retryable; surfaced as transient. |
| `LLMError` | `unavailable` for `process_*`/`search` embedding paths. |
| auth failure (hosted) | `401/403` before the tool runs (middleware). |

Cosmos 429s: rely on the SDK/Cosmos SDK retry policy; expose remaining failures as
transient with a short backoff hint.

---

## 9. Configuration (env)

Reuses the SDK's env contract (`.env.template`) plus MCP-specific keys:

| Var | Purpose |
|-----|---------|
| `COSMOS_DB_ENDPOINT`, `COSMOS_DB_DATABASE`, `COSMOS_DB_*_CONTAINER` | Cosmos topology (as SDK). |
| `AI_FOUNDRY_ENDPOINT`, `AI_FOUNDRY_*_DEPLOYMENT_NAME` | Embeddings + chat. |
| `MEMORY_PROCESSOR_OWNER` | `durable` (hosted) / `inprocess` (local) - §3.1. |
| `AGENT_MEMORY_MCP_TRANSPORT` | `stdio` \| `streamable-http`. |
| `AGENT_MEMORY_MCP_HOST/PORT` | HTTP bind (hosted). |
| `AGENT_MEMORY_DEFAULT_USER_ID` | Local single-user owner. |
| `AGENT_MEMORY_ALLOW_USER_ID_ARG` | Trust client-supplied `user_id` (default `false`). |
| `AGENT_MEMORY_EXPOSE_GRANULAR` | Expose §6.5 granular tools (default `false`). |
| `AGENT_MEMORY_MAX_TOP_K` | Server-side result ceiling (default 50). |
| `ENTRA_TENANT_ID`, `ENTRA_AUDIENCE` | JWT validation (hosted). |

Auth to Azure uses `DefaultAzureCredential` / Managed Identity (same as SDK) - no
keys in image.

---

## 10. Auth & security (hosted mode)

- **Streamable HTTP behind Entra ID.** Validate the bearer JWT (issuer/audience/
  signature) in ASGI middleware before dispatch; extract `oid`/`sub` → `user_id`.
- **Managed Identity** for Cosmos + AI Foundry (data-plane RBAC), no secrets baked in.
- **Tenant isolation** enforced at the tool layer (§5): `user_id` is never a
  client-controlled partition key in multi-tenant mode.
- **PII note:** memory content is user data. Document retention/TTL (episodic = 90d)
  and provide `delete_memory` for deletion requests.

---

## 11. Deployment

Mirror the sibling toolkit and this repo's `infra/`:
- **Local:** `python -m agent_memory_mcp` (stdio) or `--http`; `.env` per template.
  MCP client config snippet in README for Copilot/Claude/VS Code.
- **Hosted:** Docker image → **Azure Container Apps** (HTTP ingress, min-replicas 0/1,
  autoscale), Managed Identity, Bicep in `infra/`, wired into the repo's `azd up`
  alongside Cosmos + AI Foundry (+ optional Function app).

---

## 12. Testing

- **Unit:** tool arg validation, scope/auth resolution, serialization, error mapping -
  with a mocked `AsyncCosmosMemoryClient`.
- **Contract:** MCP protocol handshake + `tools/list` + `tools/call` over stdio via
  an in-memory client.
- **Integration (`@integration`):** live Cosmos + AI Foundry, end-to-end
  record→search→process→profile, gated like the SDK's existing integration marks.

---

## 13. Phased delivery

1. **M1 - Read/write core (stdio):** `add_memory`, `search_memories`, `get_memories`,
   `recall_thread`, `get_user_summary`, `get_thread_summary`, `whoami` + lifespan
   client + serialization + errors.
2. **M2 - Consolidation & lifecycle:** `process_thread`, `update_memory`,
   `delete_memory`, `get_memory_history`, `search_turns`.
3. **M3 - Hosted:** Streamable HTTP transport, Entra JWT middleware, Dockerfile,
   Bicep/`azd`, Managed Identity, `MEMORY_PROCESSOR_OWNER=durable` thin-writer mode.
4. **M4 - Polish:** granular processing tools (opt-in), MCP resources/prompts (§14),
   docs, samples.

---

## 14. Optional MCP extras (later)
- **Resources:** expose the user profile / active procedural prompt as readable MCP
  *resources* (`memory://user/{id}/profile`) so hosts can attach them as context
  without a tool round-trip.
- **Prompts:** ship reusable MCP *prompt templates* (e.g. "start-of-session recall")
  that chain `get_user_summary` + `search_memories`.

---

## 15. Open questions
1. **Naming/placement:** confirm `azure/cosmos/agent-memory-mcp/` (standalone app,
   hyphenated) vs. an importable `azure.cosmos.agent_memory_mcp` submodule.
2. **`user_id` binding:** JWT-claim only, or also allow a signed server-to-server
   header for trusted multi-agent backends?
3. **Granular processing tools:** expose by default, or keep behind the flag?
4. **Turn embeddings:** default `search_turns` on (needs `ENABLE_TURN_EMBEDDINGS`)
   or advertise only when embeddings are enabled?
5. Should the MCP server ever **auto-create** containers (`create_memory_store`), or
   strictly assume infra is pre-provisioned by `azd`?
