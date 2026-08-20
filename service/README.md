# AMT Memory REST API - Service (Phase 1)

A thin FastAPI service that exposes the Agent Memory Toolkit over REST. Every
endpoint is a small async adapter over the existing `AsyncCosmosMemoryClient`;
the service adds no memory logic of its own.

See `../Docs/rest_api_spec_phase1.md` for the full design.

## Endpoints (Phase 1)

| Method + path | What it does |
| --- | --- |
| `GET /health` | Liveness check (no Cosmos needed) |
| `POST /users/{uid}/threads/{tid}/memory` | Append one conversational turn; drives the background lifecycle on cadence |
| `POST /users/{uid}/search` | Hybrid recall (facts + episodes by default; `include_episodes:false` for facts-only) |
| `POST /users/{uid}/search/episodes` | Episode-only vector search |
| `POST /users/{uid}/reconcile` | Resolve contradictions / dedup (sync inline) |

Turns are the only memory a caller creates directly. Facts, episodes, and
summaries are produced automatically by the background lifecycle on cadence -
see "Note on episodes" below.

## Prerequisites

- Python 3.11.
- An Azure Cosmos DB account and an Azure AI Foundry (or Azure OpenAI) resource
  with an embedding deployment and a chat deployment.
- `az login` (the service authenticates with `DefaultAzureCredential` by
  default), or Cosmos / Foundry keys if you prefer.

## 1. Install

```bash
cd service
python -m pip install -e ".[dev]"
```

## 2. Configure

Copy the template and fill in your endpoints:

```bash
cp .env.example .env
# edit .env - Cosmos endpoint/database/containers + Foundry endpoint/deployments
```

The app does not auto-load `.env`; export the values before running (below).
For local testing, leave `MEMORY_PROCESSOR_OWNER` unset so the memory pipeline
runs in-process on cadence when you post turns. Set it to `durable` only when a
sibling Durable Function app owns processing (the service then becomes a thin
writer and does not run the pipeline itself).

## 3. Provision the memory store (first time only)

Create the container set the service points at (idempotent):

```bash
set -a; source .env; set +a
python - <<'PY'
import asyncio, os
from azure.cosmos.agent_memory.aio import AsyncCosmosMemoryClient

async def main():
    c = AsyncCosmosMemoryClient(
        cosmos_endpoint=os.environ["COSMOS_DB_ENDPOINT"],
        cosmos_database=os.environ["COSMOS_DB_DATABASE"],
        cosmos_container=os.environ["COSMOS_DB_MEMORIES_CONTAINER"],
        cosmos_turns_container=os.environ["COSMOS_DB_TURNS_CONTAINER"],
        cosmos_summaries_container=os.environ["COSMOS_DB_SUMMARIES_CONTAINER"],
        cosmos_counter_container=os.environ["COSMOS_DB_COUNTERS_CONTAINER"],
        cosmos_lease_container=os.environ["COSMOS_DB_LEASE_CONTAINER"],
        ai_foundry_endpoint=os.environ["AI_FOUNDRY_ENDPOINT"],
        embedding_deployment_name=os.environ["AI_FOUNDRY_EMBEDDING_DEPLOYMENT_NAME"],
        embedding_dimensions=int(os.environ["AI_FOUNDRY_EMBEDDING_DIMENSIONS"]),
        chat_deployment_name=os.environ["AI_FOUNDRY_CHAT_DEPLOYMENT_NAME"],
        use_default_credential=True,
    )
    await c.create_memory_store(embedding_dimensions=int(os.environ["AI_FOUNDRY_EMBEDDING_DIMENSIONS"]))
    await c.validate_topology()
    await c.close()
    print("store ready")

asyncio.run(main())
PY
```

## 4. Run the server

```bash
set -a; source .env; set +a
python -m uvicorn app.main:app --reload --port 8000
```

Confirm it is up:

```bash
curl http://localhost:8000/health   # -> {"status":"ok"}
```

If Cosmos / Foundry config is missing or `az login` has expired, data-plane
endpoints return `503` (problem+json) instead of crashing; `/health` still
returns `200`.

## 5. Test the API

Pick whichever you like - all hit the same running server.

- **Swagger UI (zero setup):** open `http://localhost:8000/docs` and use
  "Try it out" on each endpoint. `http://localhost:8000/redoc` is the read-only
  view; `http://localhost:8000/openapi.json` is the raw spec.
- **`.http` file (Microsoft-native):** open `service/api.http` in Visual Studio
  2022 (built-in) or VS Code with the "REST Client" extension, then click
  "Send Request". Adjust the `@base_url` / `@user_id` / `@thread_id` variables
  at the top.
- **Postman:** import `service/postman_collection.json` and set the collection
  variables.
- **curl:** e.g.

  ```bash
  # append a turn
  curl -X POST http://localhost:8000/users/u1/threads/t1/memory \
    -H "Content-Type: application/json" \
    -d '{"role":"user","content":"I prefer window seats and a budget of $1200."}'

  # search (facts + episodes)
  curl -X POST http://localhost:8000/users/u1/search \
    -H "Content-Type: application/json" \
    -d '{"query":"What are the user preferences?","top_k":5}'

  # reconcile
  curl -X POST http://localhost:8000/users/u1/reconcile \
    -H "Content-Type: application/json" -d '{}'
  ```

Post several turns before searching: fact extraction fires every
`FACT_EXTRACTION_EVERY_N` (default 2) turns and runs as a background task, so
give it a few seconds after posting before you expect results.

## Note on episodes (why a search can return zero)

Facts are **count-based** (extracted every N turns), but episodes are
**boundary-based**. The episode cadence (`EPISODE_EVAL_EVERY_N`) only
*evaluates* the open turn stream for a boundary; an episode is written only when
a segment actually **closes** on one of:

- an **idle time-gap** greater than `EPISODE_IDLE_GAP_SECONDS` (default 1800s =
  30 min) between two consecutive turns - detected on the next turn after the
  gap;
- a **topic-drift** shift (only when `EPISODE_TOPIC_DRIFT > 0`; default `0.0`
  disables it);
- the **max-size cap** `EPISODE_MAX_TURNS` (default 40 turns).

So a short, rapid, single-topic burst of turns (a still-open session) produces
facts but not yet an episode - that is by design, not a failure. In a real
conversation these boundaries occur naturally (the user returns after a break,
or a long session crosses the cap) and episodes form automatically with no
manual step. The Phase 1 REST API does not expose a session-close/flush hook, so
episodes only form via those natural boundaries.

## Run the tests

```bash
cd service
python -m pytest tests/ -q
```

The unit tests mock the SDK client, so they need no Azure resources.

## Container image

```bash
cd service
docker build -t amt-rest-service .
docker run -p 8000:8000 --env-file .env amt-rest-service
```

## Redeploy to Azure Container Apps

The service runs on Azure Container Apps from an image in ACR. To ship a code
change, rebuild the image and roll a new revision. Build the image server-side
with `az acr build` rather than a local `docker build`: the build installs
dependencies from PyPI, and a server-side build avoids local network/proxy
egress issues.

```bash
cd service

# names for your environment (example values are the shared demo deployment)
ACR=<acr-name>          # your Azure Container Registry name
APP=<container-app>     # your Container App name
RG=<resource-group>     # your resource group
TAG=amt-service:0.3.0b2

# 1) rebuild the image from Dockerfile + pyproject.toml + app/ (server-side)
az acr build -r "$ACR" -t "$TAG" .

# 2) roll a new revision (a fresh suffix forces one even if the tag is unchanged)
az containerapp update -n "$APP" -g "$RG" \
  --image "$ACR.azurecr.io/$TAG" \
  --revision-suffix "r$(date +%Y%m%d%H%M%S)"
```

A new revision takes about 90-120 seconds to become ready. Verify the rollout:

```bash
FQDN=$(az containerapp show -n "$APP" -g "$RG" \
  --query properties.configuration.ingress.fqdn -o tsv)
curl -s "https://$FQDN/health"    # -> {"status":"ok","processor_owner":"durable"}
```

Only `Dockerfile`, `pyproject.toml`, and `app/` are used for the build; no Bicep
or gateway files are involved in a code redeploy.
