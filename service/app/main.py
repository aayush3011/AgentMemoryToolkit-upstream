from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI

try:
    from azure.cosmos.agent_memory.aio import CosmosMemoryClient as AsyncCosmosMemoryClient
except ImportError:
    from azure.cosmos.agent_memory.aio import AsyncCosmosMemoryClient
from app.core.auth import require_apim_key
from app.core.errors import register_exception_handlers
from app.routers import ingestion, lifecycle, retrieval, summaries
from azure.cosmos.agent_memory.aio.processors import AsyncDurableFunctionProcessor

logger = logging.getLogger(__name__)


def _get_bool(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _get_int(name: str) -> int | None:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return None
    try:
        return int(raw)
    except ValueError:
        logger.warning("Ignoring invalid integer env var %s=%r", name, raw)
        return None


def _build_cadence_thresholds() -> dict[str, int] | None:
    names = (
        "FACT_EXTRACTION_EVERY_N",
        "EPISODE_EVAL_EVERY_N",
        "THREAD_SUMMARY_EVERY_N",
        "USER_SUMMARY_EVERY_N",
        "DEDUP_EVERY_N",
    )
    values = {name: value for name in names if (value := _get_int(name)) is not None}
    return values or None


def _build_processor() -> Any | None:
    owner = (os.getenv("MEMORY_PROCESSOR_OWNER") or "").strip().lower()
    if owner == "durable":
        return AsyncDurableFunctionProcessor()
    return None


def _build_client() -> AsyncCosmosMemoryClient:
    return AsyncCosmosMemoryClient(
        cosmos_endpoint=os.getenv("COSMOS_DB_ENDPOINT") or os.getenv("COSMOS_DB__accountEndpoint"),
        cosmos_key=os.getenv("COSMOS_DB_KEY"),
        cosmos_database=os.getenv("COSMOS_DB_DATABASE"),
        cosmos_container=os.getenv("COSMOS_DB_MEMORIES_CONTAINER") or os.getenv("COSMOS_DB_CONTAINER"),
        cosmos_turns_container=os.getenv("COSMOS_DB_TURNS_CONTAINER", "memories_turns"),
        cosmos_summaries_container=os.getenv("COSMOS_DB_SUMMARIES_CONTAINER", "memories_summaries"),
        cosmos_counter_container=os.getenv("COSMOS_DB_COUNTERS_CONTAINER"),
        cosmos_lease_container=os.getenv("COSMOS_DB_LEASE_CONTAINER"),
        ai_foundry_endpoint=os.getenv("AI_FOUNDRY_ENDPOINT"),
        ai_foundry_api_key=os.getenv("AI_FOUNDRY_API_KEY"),
        embedding_deployment_name=os.getenv("AI_FOUNDRY_EMBEDDING_DEPLOYMENT_NAME", "text-embedding-3-large"),
        embedding_dimensions=_get_int("AI_FOUNDRY_EMBEDDING_DIMENSIONS"),
        chat_deployment_name=os.getenv("AI_FOUNDRY_CHAT_DEPLOYMENT_NAME", "gpt-4o-mini"),
        use_default_credential=_get_bool("AZURE_USE_DEFAULT_CREDENTIAL", True),
        processor=_build_processor(),
        cadence_thresholds=_build_cadence_thresholds(),
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    client: AsyncCosmosMemoryClient | None = None
    try:
        client = _build_client()
    except Exception:
        logger.exception("Failed to construct AsyncCosmosMemoryClient")
    app.state.memory_client = client

    if client is not None:
        try:
            await client.connect_cosmos()
        except Exception as exc:
            logger.warning("Cosmos connection failed during startup: %r", exc)

    try:
        yield
    finally:
        if client is not None:
            try:
                await client.close()
            except Exception:
                logger.exception("Failed to close AsyncCosmosMemoryClient")


app = FastAPI(title="AMT Memory REST API", version="0.1.0", lifespan=lifespan)
register_exception_handlers(app)

# Behind the InferencePlatform Front Door the AMT app is mounted under a path
# prefix (default /memory); Front Door forwards the full path without stripping,
# matching how their .NET app serves under /inference. API_PREFIX is empty for
# local/dev so routes stay at the root. /health stays unprefixed and unauthed so
# the container and Front Door origin health probes always reach it.
API_PREFIX = os.getenv("API_PREFIX", "")

app.include_router(ingestion.router, prefix=API_PREFIX, dependencies=[Depends(require_apim_key)])
app.include_router(retrieval.router, prefix=API_PREFIX, dependencies=[Depends(require_apim_key)])
app.include_router(lifecycle.router, prefix=API_PREFIX, dependencies=[Depends(require_apim_key)])
app.include_router(summaries.router, prefix=API_PREFIX, dependencies=[Depends(require_apim_key)])


@app.get("/health")
async def health() -> dict[str, str]:
    owner = (os.getenv("MEMORY_PROCESSOR_OWNER") or "").strip().lower() or "inprocess"
    return {"status": "ok", "processor_owner": owner}
