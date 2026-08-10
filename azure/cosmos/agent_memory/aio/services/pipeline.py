"""Async pipeline service for LLM-driven memory extraction, summaries, and reconciliation.

This module is the asynchronous sibling of
:class:`azure.cosmos.agent_memory.services.pipeline.PipelineService`. The two
share all pure helpers via
:mod:`azure.cosmos.agent_memory.services._pipeline_helpers`; only the IO call
sites differ - every Cosmos query, chat completion, and embedding call is
``await``-ed against the async clients/store.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Iterable, Literal, Optional

from azure.cosmos.exceptions import (
    CosmosResourceExistsError,
    CosmosResourceNotFoundError,
)

from azure.cosmos.agent_memory._container_routing import ContainerKey
from azure.cosmos.agent_memory._utils import (
    DEFAULT_TTL_BY_TYPE,
    compute_content_hash,
    distance_function_from_container_properties,
    vector_autodrop_supported,
    vector_order_direction,
    vector_similarity_at_least,
)
from azure.cosmos.agent_memory.aio.store import AsyncMemoryStore
from azure.cosmos.agent_memory.exceptions import (
    LLMError,
    MemoryConflictError,
    ValidationError,
)
from azure.cosmos.agent_memory.logging import get_logger
from azure.cosmos.agent_memory.models import (
    EpisodicRecord,
    FactRecord,
    ProceduralRecord,
    ThreadSummaryRecord,
    UserSummaryRecord,
    construct_internal,
)
from azure.cosmos.agent_memory.prompts._schemas import response_format_for
from azure.cosmos.agent_memory.services._pipeline_helpers import (
    ID_SEED_SEP as _ID_SEED_SEP,
)
from azure.cosmos.agent_memory.services._pipeline_helpers import (
    PromptyLoader,
    _normalize_metadata_keys,
    batch_turns_by_tokens,
    build_topic_tags,
    build_transcript,
    cap_structured_summary,
    chat_text,
    clamp_unit_interval,
    created_at_sort_key,
    deterministic_episode_id,
    extract_memories_prompt_file,
    find_episode_boundary,
    is_retryable_llm_error,
    is_valid_time_pair,
    parse_llm_json,
    segment_time_bounds,
    turn_gap_seconds,
)
from azure.cosmos.agent_memory.services._pipeline_helpers import (
    is_real_number as _is_real_number,
)
from azure.cosmos.agent_memory.services._pipeline_helpers import (
    max_or_none as _max_or_none,
)
from azure.cosmos.agent_memory.store._search_helpers import top_literal
from azure.cosmos.agent_memory.thresholds import (
    get_dedup_sim_high,
    get_dedup_vector_enabled,
    get_episode_idle_gap_seconds,
    get_episode_max_turns,
    get_episode_min_turns,
    get_episode_topic_drift,
    get_extraction_batch_max_tokens,
)

logger = get_logger("azure.cosmos.agent_memory.pipeline.aio")


_cap_structured_summary = cap_structured_summary

_ACTIVE_DOC_FILTER = "(NOT IS_DEFINED(c.superseded_by) OR IS_NULL(c.superseded_by))"
_PROCEDURAL_MAX_CREATE_ATTEMPTS = 5

# Safety cap on how many episodes one extract_episodes call may close in a
# single pass (mirror of the sync pipeline). Boundary evaluation runs on a small
# turn cadence, so this only bounds a pathological drain and never fans out into
# an unbounded burst of LLM extractions.
_EPISODE_MAX_SEGMENTS_PER_RUN = 50


class _AsyncStoreContainerAdapter:
    """Expose one split ``AsyncMemoryStore`` container via Cosmos method shapes."""

    def __init__(self, store: AsyncMemoryStore, container_key: ContainerKey) -> None:
        self._store = store
        self._container_key = container_key

    def _target_container(self) -> Any | None:
        containers = getattr(self._store, "_containers", None)
        if isinstance(containers, dict):
            return containers.get(self._container_key)
        if self._container_key is ContainerKey.MEMORIES:
            return getattr(self._store, "container", None)
        return None

    async def _collect_query(self, result: Any) -> list[dict[str, Any]]:
        if inspect.isawaitable(result):
            result = await result
        if hasattr(result, "__aiter__"):
            return [item async for item in result]
        return list(result)

    async def query_items(self, **kwargs: Any) -> list[dict[str, Any]]:
        container = self._target_container()
        if container is not None and hasattr(container, "query_items"):
            kwargs.pop("enable_cross_partition_query", None)
            return await self._collect_query(container.query_items(**kwargs))
        try:
            return await self._store.query(
                kwargs["query"],
                parameters=kwargs.get("parameters"),
                container_key=self._container_key,
                partition_key=kwargs.get("partition_key"),
            )
        except TypeError:
            return await self._store.query(
                kwargs["query"],
                parameters=kwargs.get("parameters"),
                partition_key=kwargs.get("partition_key"),
            )

    async def read_item(self, *, item: str, partition_key: Any) -> dict[str, Any]:
        container = self._target_container()
        if container is not None and hasattr(container, "read_item"):
            response = container.read_item(item=item, partition_key=partition_key)
            if inspect.isawaitable(response):
                response = await response
            return response
        try:
            return await self._store.read_item(item, partition_key, container_key=self._container_key)
        except TypeError:
            return await self._store.read_item(item, partition_key)

    async def upsert_item(self, *, body: dict[str, Any]) -> dict[str, Any]:
        container = self._target_container()
        if container is not None and hasattr(container, "upsert_item"):
            response = container.upsert_item(body=body)
            if inspect.isawaitable(response):
                response = await response
            return response if isinstance(response, dict) else body
        upsert = getattr(self._store, "upsert_item", None)
        if upsert is not None:
            response = upsert(body=body)
            if inspect.isawaitable(response):
                response = await response
            return response if isinstance(response, dict) else body
        response = await self._store.add_cosmos(body)
        return response if isinstance(response, dict) else body

    async def create_item(self, *, body: dict[str, Any]) -> dict[str, Any]:
        container = self._target_container()
        if container is not None and hasattr(container, "create_item"):
            response = container.create_item(body=body)
            if inspect.isawaitable(response):
                response = await response
            return response if isinstance(response, dict) else body
        create = getattr(self._store, "create_item", None)
        if create is not None:
            response = create(body=body)
            if inspect.isawaitable(response):
                response = await response
            return response if isinstance(response, dict) else body
        response = await self._store.add_cosmos(body)
        return response if isinstance(response, dict) else body

    async def replace_item(self, **kwargs: Any) -> Any:
        container = self._target_container()
        if container is not None and hasattr(container, "replace_item"):
            response = container.replace_item(**kwargs)
            if inspect.isawaitable(response):
                response = await response
            return response
        return await self.upsert_item(body=kwargs["body"])


class AsyncPipelineService:
    """Async LLM orchestration service backed by an async typed memory store."""

    def __init__(
        self,
        store: AsyncMemoryStore,
        chat_client: Any,
        embeddings_client: Any,
        prompts_dir: str | None = None,
        *,
        containers: dict[ContainerKey, Any],
        transcript_metadata_keys: Optional[Iterable[str]] = None,
    ) -> None:
        self._store = store
        self._containers = containers
        self._memories_container = containers[ContainerKey.MEMORIES]
        self._turns_container = containers[ContainerKey.TURNS]
        self._summaries_container = containers[ContainerKey.SUMMARIES]
        self._container = self._memories_container
        self._chat_client = chat_client
        self._embeddings = embeddings_client
        self._prompty = PromptyLoader(prompts_dir)
        self._transcript_metadata_keys: Optional[tuple[str, ...]] = _normalize_metadata_keys(transcript_metadata_keys)

    async def _query_items(self, container: Any, **kwargs: Any) -> list[dict[str, Any]]:
        result = container.query_items(**kwargs)
        if inspect.isawaitable(result):
            result = await result
        if hasattr(result, "__aiter__"):
            return [item async for item in result]
        return list(result)

    async def _read_item(self, container: Any, *, item: str, partition_key: Any) -> dict[str, Any]:
        result = container.read_item(item=item, partition_key=partition_key)
        if inspect.isawaitable(result):
            result = await result
        return result

    async def _upsert_item(self, container: Any, *, body: dict[str, Any]) -> dict[str, Any]:
        result = container.upsert_item(body=body)
        if inspect.isawaitable(result):
            result = await result
        return result if isinstance(result, dict) else body

    async def _create_item(self, container: Any, *, body: dict[str, Any]) -> dict[str, Any]:
        result = container.create_item(body=body)
        if inspect.isawaitable(result):
            result = await result
        return result if isinstance(result, dict) else body

    async def _replace_item(self, container: Any, **kwargs: Any) -> Any:
        result = container.replace_item(**kwargs)
        if inspect.isawaitable(result):
            result = await result
        return result

    async def _run_prompty(
        self,
        filename: str,
        inputs: dict[str, Any],
    ) -> str:
        """Render a prompty template, run the LLM async, and return the response text."""
        messages, params = self._prompty.prepare(filename, inputs)
        schema_format = response_format_for(filename)
        if schema_format is not None:
            params["response_format"] = schema_format
        response = await self._chat_client.generate(messages, **params)
        return chat_text(response)

    async def _embed_one(self, text: str) -> list[float]:
        return await self._embeddings.generate(text)

    async def _embed_batch(self, texts: list[str]) -> list[list[float]]:
        return await self._embeddings.generate_batch(texts)

    async def _vector_distance_function(self) -> str:
        """Return the container's configured Cosmos ``distanceFunction`` (cached).

        Read from the container's vector embedding policy (``await container.read()``)
        - the authoritative, immutable source set when the container was created.
        Drives the ORDER BY direction and similarity-threshold comparisons so dedup
        never silently assumes cosine. Falls back to cosine when the policy can't be
        read (e.g. ``__new__``-built test instances with mocked containers).
        """
        fn = getattr(self, "_distance_function_cache", None)
        if fn is not None:
            return fn
        try:
            props = await self._memories_container.read()
        except Exception:
            # See sync pipeline: don't cache a defaulted cosine, and flag the
            # failure so the destructive in-place fold path skips this run rather
            # than mis-applying cosine bands to (possibly euclidean) distances.
            self._distance_function_read_failed = True
            logger.debug(
                "vector dedup: could not read container vector policy; defaulting to cosine (not cached)",
                exc_info=True,
            )
            return "cosine"
        fn = distance_function_from_container_properties(props)
        self._distance_function_cache = fn
        self._distance_function_read_failed = False
        return fn

    def _warn_distance_policy_unavailable_once(self) -> None:
        """One-shot WARN that in-place folding was skipped (policy unreadable)."""
        if getattr(self, "_warned_distance_policy_unavailable", False):
            return
        self._warned_distance_policy_unavailable = True
        logger.warning(
            "vector dedup: container vector policy could not be read; skipping in-place "
            "near-duplicate folding this run to avoid mis-calibrated folds. Memories are "
            "written as-is and deduped on a later run once the policy is readable."
        )

    def _warn_euclidean_autodrop_once(self, distance_function: str) -> None:
        """One-shot WARN that the near-exact vector auto-drop is disabled.

        The ``DEDUP_SIM_HIGH`` thresholds are cosine-calibrated; on euclidean
        the destructive auto-drop is skipped (borderline tagging + LLM reconcile
        still run). Logged once per pipeline instance to avoid hot-path spam.
        """
        if getattr(self, "_warned_euclidean_autodrop", False):
            return
        self._warned_euclidean_autodrop = True
        logger.warning(
            "Container distanceFunction=%r: near-exact vector auto-drop is "
            "cosine-calibrated and has been DISABLED for this distance function. "
            "Duplicate detection falls back to borderline tagging + LLM reconcile. "
            "Use cosine/dotproduct embeddings for vector-floor auto-dedup.",
            distance_function,
        )

    async def _vector_candidates(
        self,
        *,
        user_id: str,
        embedding,
        memory_type,
        top_k,
        exclude_ids,
    ) -> list[dict]:
        """Return active same-user vector candidates from Cosmos."""
        if not user_id or not embedding or not top_k or int(top_k) < 1:
            return []
        excluded = set(exclude_ids or [])
        capped_top = top_literal(int(top_k), name="_vector_candidates.top_k")
        distance_function = await self._vector_distance_function()
        order_direction = vector_order_direction(distance_function)
        field = "embedding"
        query = (
            f"SELECT TOP {capped_top} c.id, c.content, c.type, "
            f"VectorDistance(c.{field}, @vec) AS score "
            "FROM c WHERE c.user_id = @user_id "
            "AND c.type = @memory_type "
            f"AND {_ACTIVE_DOC_FILTER} "
            f"AND IS_DEFINED(c.{field}) "
            # Cosmos orders ORDER BY VectorDistance() most-similar-first per the
            # container's distanceFunction; an explicit ASC/DESC is rejected (BadRequest).
            f"ORDER BY VectorDistance(c.{field}, @vec)"
        )
        rows = await self._query_items(
            self._memories_container,
            query=query,
            parameters=[
                {"name": "@user_id", "value": user_id},
                {"name": "@memory_type", "value": memory_type},
                {"name": "@vec", "value": embedding},
            ],
        )
        candidates = [
            {
                "id": row.get("id"),
                "content": row.get("content"),
                "type": row.get("type"),
                "score": float(row.get("score") or 0.0),
            }
            for row in rows
            if row.get("id") and row.get("id") not in excluded
        ]
        # Most-similar-first: descending score for cosine/dotproduct, ascending for euclidean.
        candidates.sort(
            key=lambda item: item.get("score", 0.0),
            reverse=order_direction == "DESC",
        )
        return candidates

    def _prompt_lineage(self, filename: str) -> dict[str, str]:
        """Return ``{prompt_id, prompt_version}`` for stamping a doc.

        Safe no-op fallback (``prompt_version="v1"``) when the loader was
        never initialized - happens in unit tests that build the service
        via ``__new__`` to bypass real LLM/embedding clients.
        """
        loader = getattr(self, "_prompty", None)
        version = loader.prompt_version(filename) if loader is not None else "v1"
        return {"prompt_id": filename, "prompt_version": version}

    def _validate_extracted_doc(self, doc: dict[str, Any]) -> dict[str, Any]:
        """Run an extracted fact/episodic doc through its typed model."""
        if doc.get("type") == "fact":
            return construct_internal(FactRecord, doc).to_doc()
        if doc.get("type") == "episodic":
            return construct_internal(EpisodicRecord, doc).to_doc()
        return doc

    @staticmethod
    def _chat_text(response: Any) -> str:
        return chat_text(response)

    def _build_transcript(
        self,
        items: list[dict[str, Any]],
        *,
        group_by_thread: bool = False,
        include_timestamp: bool = False,
    ) -> str:
        # getattr fallback covers unit tests that build AsyncPipelineService
        # via __new__ to bypass __init__ (and therefore the metadata-keys stash).
        return build_transcript(
            items,
            group_by_thread=group_by_thread,
            metadata_keys=getattr(self, "_transcript_metadata_keys", None),
            include_timestamp=include_timestamp,
        )

    async def _load_existing_memories(
        self,
        user_id: str,
        memory_types: list[str],
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Query active (non-superseded) memories for reconciliation context.

        Results are ordered by ``c._ts DESC`` so the most recently written
        memories survive the cap - without ORDER BY, Cosmos returns rows
        in implementation-defined order and the dedup comparison set is
        non-deterministic.
        """
        type_placeholders = ", ".join(f"@mtype{i}" for i in range(len(memory_types)))
        capped_limit = top_literal(limit, name="_load_existing_memories.limit")
        query = (
            f"SELECT TOP {capped_limit} * FROM c "
            f"WHERE c.user_id = @user_id "
            f"AND c.type IN ({type_placeholders}) "
            f"AND {_ACTIVE_DOC_FILTER} "
            f"ORDER BY c._ts DESC"
        )
        parameters: list[dict[str, Any]] = [
            {"name": "@user_id", "value": user_id},
        ]
        for i, mt in enumerate(memory_types):
            parameters.append({"name": f"@mtype{i}", "value": mt})

        return await self._query_items(
            self._memories_container,
            query=query,
            parameters=parameters,
        )

    async def _upsert_memory(self, doc: dict[str, Any]) -> dict[str, Any]:
        """Upsert a fact, episodic, or procedural document to the memories container."""
        return await self._upsert_item(self._memories_container, body=doc)

    async def _upsert_summary(self, doc: dict[str, Any]) -> dict[str, Any]:
        """Upsert a thread/user summary document to the summaries container."""
        return await self._upsert_item(self._summaries_container, body=doc)

    async def _create_memory(self, doc: dict[str, Any]) -> dict[str, Any]:
        """Create a memory document and let Cosmos raise 409 for duplicates."""
        return await self._create_item(self._memories_container, body=doc)

    @staticmethod
    def _empty_extract_counts() -> dict[str, int]:
        return {
            "fact_count": 0,
            "episodic_count": 0,
            "updated_count": 0,
            "contradicted_count": 0,
            "exact_dedup_skipped": 0,
            "dropped_episodic_count": 0,
            "deferred_turn_count": 0,
            "quarantined_turn_count": 0,
        }

    @staticmethod
    def _stable_source_timestamp(items: list[dict[str, Any]]) -> str:
        timestamps = [str(item.get("created_at")) for item in items if item.get("created_at")]
        if timestamps:
            return max(timestamps)
        return datetime.now(timezone.utc).isoformat()

    async def _mark_superseded(
        self,
        old_doc: dict[str, Any],
        superseder_id: str,
        *,
        reason: Literal["duplicate", "contradict", "update"],
    ) -> bool:
        """Atomically set ``superseded_by`` on ``old_doc`` via the async memory store."""
        return await self._store.mark_superseded(old_doc, superseder_id, reason=reason)

    @staticmethod
    def _parse_llm_json(text: str | None) -> dict[str, Any]:
        return parse_llm_json(text)

    async def extract_memories_durable(
        self,
        user_id: str,
        thread_id: str,
        recent_k: int | None = None,
        *,
        turns: Optional[list[dict[str, Any]]] = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """Load turns, call the LLM, and return memory docs without embeddings or writes."""
        if not user_id:
            raise ValidationError("user_id is required")
        if not thread_id:
            raise ValidationError("thread_id is required")

        logger.info("extract_memories_durable started user_id=%s thread_id=%s", user_id, thread_id)

        if turns is None:
            query = (
                "SELECT * FROM c WHERE c.user_id = @user_id "
                "AND c.thread_id = @thread_id AND c.type = 'turn' "
                "AND (NOT IS_DEFINED(c.extracted_at) OR IS_NULL(c.extracted_at))"
            )
            parameters: list[dict[str, Any]] = [
                {"name": "@user_id", "value": user_id},
                {"name": "@thread_id", "value": thread_id},
            ]
            items = await self._query_items(
                self._turns_container,
                query=query,
                parameters=parameters,
                partition_key=[user_id, thread_id],
            )
        else:
            items = list(turns)

        items.sort(key=lambda m: m.get("created_at", ""), reverse=True)
        if recent_k is not None:
            items = items[:recent_k]
        items.reverse()

        if not items:
            logger.warning("extract_memories_durable no memories found user_id=%s thread_id=%s", user_id, thread_id)
            return {"facts": [], "episodic": [], "updates": [], "processed_turn_docs": []}

        existing_for_hash = await self._load_existing_memories(user_id, ["fact"])
        existing_fact_hashes: set[str] = {
            m["content_hash"] for m in existing_for_hash if m.get("type") == "fact" and m.get("content_hash")
        }

        # Token-bounded, per-batch extraction. Each batch is an independent LLM
        # call, so a single poisoned turn fails only its own batch. Turns from
        # succeeded and quarantined (non-retryable, e.g. content-filter) batches
        # go into ``processed_turns`` and are stamped ``extracted_at`` by persist
        # so they are never re-processed; turns from batches that fail with a
        # *retryable* error are left un-stamped and retried on the next run.
        batches = batch_turns_by_tokens(items, get_extraction_batch_max_tokens())
        facts: list[dict[str, Any]] = []
        processed_turns: list[dict[str, Any]] = []
        deferred_turn_count = 0
        quarantined_turn_count = 0
        extract_prompt = extract_memories_prompt_file()
        for batch in batches:
            batch_transcript = self._build_transcript(batch, include_timestamp=True)
            try:
                response_text = await self._run_prompty(extract_prompt, inputs={"transcript": batch_transcript})
                parsed = self._parse_llm_json(response_text)
                facts.extend(parsed.get("facts", []))
                processed_turns.extend(batch)
            except Exception as exc:  # noqa: BLE001
                if is_retryable_llm_error(exc):
                    deferred_turn_count += len(batch)
                    logger.warning(
                        "extract_memories: deferring %d turns after retryable extraction error "
                        "(will retry next run) user_id=%s thread_id=%s err=%s",
                        len(batch),
                        user_id,
                        thread_id,
                        exc,
                    )
                else:
                    processed_turns.extend(batch)
                    quarantined_turn_count += len(batch)
                    logger.warning(
                        "extract_memories: quarantining %d turns after non-retryable extraction error "
                        "(e.g. content filter) - marking extracted so they do not re-poison future runs "
                        "user_id=%s thread_id=%s err=%s",
                        len(batch),
                        user_id,
                        thread_id,
                        exc,
                    )

        doc_timestamp = self._stable_source_timestamp(items)
        fact_docs: list[dict[str, Any]] = []
        updates: list[dict[str, Any]] = []
        exact_dedup_skipped = 0

        for fact in facts:
            text = fact.get("text")
            if not text:
                logger.warning("extract_memories: dropping malformed fact (missing 'text'): %r", fact)
                continue

            new_content_hash = compute_content_hash(text)
            if new_content_hash in existing_fact_hashes:
                logger.debug(
                    "extract_memories: skipping exact-dup fact hash=%s user_id=%s thread_id=%s",
                    new_content_hash,
                    user_id,
                    thread_id,
                )
                exact_dedup_skipped += 1
                continue

            seed = _ID_SEED_SEP.join((user_id, thread_id, new_content_hash))
            det_id = f"fact_{hashlib.sha256(seed.encode()).hexdigest()[:32]}"
            topic_tags = build_topic_tags(fact.get("tags", []))
            raw_source = fact.get("source")
            if raw_source is not None and raw_source not in ("user", "agent"):
                logger.debug(
                    "extract_memories: coercing invalid fact source=%r to 'user' user_id=%s",
                    raw_source,
                    user_id,
                )
            fact_source = raw_source if raw_source in ("user", "agent") else "user"
            source_tags = ["sys:agent-fact"] if fact_source == "agent" else []
            confidence = fact.get("confidence")
            doc: dict[str, Any] = {
                "id": det_id,
                "user_id": user_id,
                "thread_id": thread_id,
                "role": "system",
                "type": "fact",
                "content": text,
                "content_hash": new_content_hash,
                "confidence": clamp_unit_interval(confidence, 0.5),
                **self._prompt_lineage(extract_prompt),
                "metadata": {
                    "category": fact.get("category") or "other",
                    "temporal_context": fact.get("temporal_context"),
                    "source": fact_source,
                },
                "salience": clamp_unit_interval(fact.get("salience"), 0.5),
                "tags": ["sys:fact", "sys:auto-extracted"] + source_tags + topic_tags,
                "created_at": doc_timestamp,
                "updated_at": doc_timestamp,
            }

            fact_docs.append(self._validate_extracted_doc(doc))
            existing_fact_hashes.add(new_content_hash)

        if exact_dedup_skipped:
            updates.append({"op": "stats", "exact_dedup_skipped": exact_dedup_skipped})
        if deferred_turn_count or quarantined_turn_count:
            updates.append(
                {
                    "op": "stats",
                    "deferred_turn_count": deferred_turn_count,
                    "quarantined_turn_count": quarantined_turn_count,
                }
            )

        result = {
            "facts": fact_docs,
            "episodic": [],
            "updates": updates,
            "processed_turn_docs": processed_turns,
        }
        logger.info(
            "extract_memories_durable completed user_id=%s thread_id=%s fact_docs=%d episodic_docs=%d updates=%d",
            user_id,
            thread_id,
            len(fact_docs),
            0,
            len(updates),
        )
        return result

    async def dedup_extracted_memories(self, user_id: str, extracted: dict) -> dict:
        """Fold near-duplicate extracted docs into their existing canonical
        memory *in place* (async mirror of the sync in-place dedup).
        """
        if not get_dedup_vector_enabled():
            return extracted
        if not user_id:
            raise ValidationError("user_id is required")
        if not isinstance(extracted, dict):
            raise ValidationError("extracted must be a dict")

        high = get_dedup_sim_high()
        distance_function = await self._vector_distance_function()
        read_failed = getattr(self, "_distance_function_read_failed", False)
        similarity_ok = (not read_failed) and vector_autodrop_supported(distance_function)
        if read_failed:
            self._warn_distance_policy_unavailable_once()
        elif not similarity_ok:
            self._warn_euclidean_autodrop_once(distance_function)

        result = {
            "facts": [dict(doc) for doc in extracted.get("facts", [])],
            "episodic": [dict(doc) for doc in extracted.get("episodic", [])],
            "updates": [dict(op) for op in extracted.get("updates", [])],
        }
        # Carry through any non-bucket keys (e.g. ``processed_turn_docs``) so this
        # transform never silently drops caller state.
        for _carry_key, _carry_value in extracted.items():
            if _carry_key not in result:
                result[_carry_key] = _carry_value

        docs = [doc for doc in result["facts"] + result["episodic"] if doc.get("content")]
        # Similarity comparison is only meaningful for cosine/dotproduct; on a
        # euclidean container we skip in-place folding and let everything ADD.
        if not docs or not similarity_ok:
            return result

        missing_embeddings = [doc for doc in docs if not doc.get("embedding")]
        if missing_embeddings:
            embeddings = await self._embed_batch([str(doc["content"]) for doc in missing_embeddings])
            for doc, embedding in zip(missing_embeddings, embeddings):
                doc["embedding"] = embedding

        inplace_updated = 0
        folded_ids: set[str] = set()
        updated_target_ids: set[str] = set()
        for doc in docs:
            doc_id = str(doc.get("id") or "")
            memory_type = str(doc.get("type") or "")
            embedding = doc.get("embedding") or []
            if not doc_id or memory_type not in {"fact", "episodic"} or not embedding:
                continue

            neighbor, score = await self._nearest_active_full(
                user_id=user_id,
                embedding=embedding,
                memory_type=memory_type,
                exclude_ids={doc_id} | set(doc.get("supersedes_ids") or []),
            )
            if not neighbor or not vector_similarity_at_least(score, high, distance_function):
                continue  # novel - leave in result for persist to ADD

            neighbor_id = str(neighbor.get("id") or "")
            if not neighbor_id:
                continue
            if neighbor_id in updated_target_ids:
                folded_ids.add(doc_id)
                continue
            if await self._apply_inplace_update(neighbor, doc):
                updated_target_ids.add(neighbor_id)
                inplace_updated += 1
                folded_ids.add(doc_id)

        if folded_ids:
            for bucket in ("facts", "episodic"):
                result[bucket] = [d for d in result[bucket] if str(d.get("id") or "") not in folded_ids]
        if inplace_updated:
            result["updates"].append({"op": "stats", "inplace_updated": inplace_updated})
        return result

    async def _nearest_active_full(
        self,
        *,
        user_id: str,
        embedding: list[float],
        memory_type: str,
        exclude_ids: set[str],
    ) -> tuple[Optional[dict[str, Any]], float]:
        """Async mirror: nearest active same-type memory returned as a *full* doc."""
        if not user_id or not embedding:
            return None, 0.0
        query = (
            "SELECT TOP 5 c AS doc, VectorDistance(c.embedding, @vec) AS score "
            "FROM c WHERE c.user_id = @user_id "
            "AND c.type = @memory_type "
            f"AND {_ACTIVE_DOC_FILTER} "
            "AND IS_DEFINED(c.embedding) "
            "ORDER BY VectorDistance(c.embedding, @vec)"
        )
        try:
            rows = await self._query_items(
                self._memories_container,
                query=query,
                parameters=[
                    {"name": "@user_id", "value": user_id},
                    {"name": "@memory_type", "value": memory_type},
                    {"name": "@vec", "value": embedding},
                ],
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("_nearest_active_full query failed user_id=%s err=%s", user_id, exc)
            return None, 0.0
        for row in rows:
            doc = row.get("doc") or {}
            rid = str(doc.get("id") or "")
            if rid and rid not in exclude_ids:
                return doc, float(row.get("score") or 0.0)
        return None, 0.0

    async def _apply_inplace_update(self, neighbor: dict[str, Any], new_doc: dict[str, Any]) -> bool:
        """Async mirror of the sync in-place refresh (recency-wins content+embedding).

        Folds only within the same ``metadata.source`` (user vs agent); a
        cross-source pair returns False so the caller keeps it as a novel ADD,
        preventing tag/source desync.
        """
        from azure.core import MatchConditions
        from azure.cosmos.exceptions import CosmosAccessConditionFailedError

        neighbor_source = (neighbor.get("metadata") or {}).get("source") or "user"
        new_source = (new_doc.get("metadata") or {}).get("source") or "user"
        if neighbor_source != new_source:
            logger.info(
                "in-place dedup update skipped (source mismatch neighbor=%s new=%s) "
                "target_id=%s; keeping new doc as novel",
                neighbor_source,
                new_source,
                neighbor.get("id"),
            )
            return False

        try:
            old_etag = neighbor.get("_etag")
            updated = dict(neighbor)
            for sys_prop in ("_rid", "_self", "_etag", "_attachments", "_ts"):
                updated.pop(sys_prop, None)
            new_content = str(new_doc.get("content") or "")
            old_content = str(neighbor.get("content") or "")
            if len(new_content) >= len(old_content):
                updated["content"] = new_content
                updated["content_hash"] = compute_content_hash(new_content)
                if new_doc.get("embedding"):
                    updated["embedding"] = new_doc["embedding"]
            updated["updated_at"] = datetime.now(timezone.utc).isoformat()

            new_sal = _max_or_none([neighbor.get("salience"), new_doc.get("salience")])
            if new_sal is not None:
                updated["salience"] = new_sal
            new_conf = _max_or_none([neighbor.get("confidence"), new_doc.get("confidence")])
            if new_conf is not None:
                updated["confidence"] = new_conf

            merged_tags: list[str] = []
            for t in list(neighbor.get("tags") or []) + list(new_doc.get("tags") or []):
                if t and t != "sys:dup-candidate" and t not in merged_tags:
                    merged_tags.append(t)
            if merged_tags:
                updated["tags"] = merged_tags

            if old_etag and hasattr(self._memories_container, "replace_item"):
                await self._replace_item(
                    self._memories_container,
                    item=updated["id"],
                    body=updated,
                    match_condition=MatchConditions.IfNotModified,
                    etag=old_etag,
                )
            else:
                await self._upsert_item(self._memories_container, body=updated)
            return True
        except CosmosAccessConditionFailedError:
            logger.info(
                "in-place dedup update skipped (concurrent writer won) target_id=%s; keeping new doc as novel",
                neighbor.get("id"),
            )
            return False
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "in-place dedup update failed target_id=%s err=%s (keeping new doc as novel)",
                neighbor.get("id"),
                exc,
            )
            return False

    async def persist_extracted_memories(
        self,
        user_id: str,
        extracted: dict[str, list[dict[str, Any]]],
    ) -> dict[str, int]:
        """Embed and create extracted memories, skipping deterministic-ID conflicts."""
        if not user_id:
            raise ValidationError("user_id is required")
        if not isinstance(extracted, dict):
            raise ValidationError("extracted must be a dict")

        result = self._empty_extract_counts()
        fact_docs = [dict(doc) for doc in extracted.get("facts", [])]
        episodic_docs = [dict(doc) for doc in extracted.get("episodic", [])]
        update_ops = [dict(op) for op in extracted.get("updates", [])]
        docs_to_create = fact_docs + episodic_docs

        docs_needing_embeddings = [doc for doc in docs_to_create if doc.get("content") and not doc.get("embedding")]
        if docs_needing_embeddings:
            embeddings = await self._embed_batch([str(doc["content"]) for doc in docs_needing_embeddings])
            for doc, embedding in zip(docs_needing_embeddings, embeddings):
                doc["embedding"] = embedding

        for doc in docs_to_create:
            validated = self._validate_extracted_doc(doc)
            doc_type = validated.get("type")
            try:
                await self._create_memory(validated)
            except CosmosResourceExistsError:
                logger.info("persist_extracted_memories skipped existing id=%s", validated.get("id"))
                continue

            if doc_type == "episodic":
                result["episodic_count"] += 1
            elif doc_type == "fact":
                result["fact_count"] += 1

        for op in update_ops:
            if op.get("op") == "stats":
                result["exact_dedup_skipped"] += int(op.get("exact_dedup_skipped") or 0)
                result["dropped_episodic_count"] += int(op.get("dropped_episodic_count") or 0)
                for key in ("inplace_updated", "deferred_turn_count", "quarantined_turn_count"):
                    if key in op:
                        result[key] = result.get(key, 0) + int(op.get(key) or 0)

        logger.info("persist_extracted_memories completed user_id=%s counts=%s", user_id, result)

        return result

    async def _mark_turns_extracted(self, turn_docs: list[dict[str, Any]], *, field: str = "extracted_at") -> int:
        """Stamp a processed-watermark field on each turn doc and upsert. Mirror of
        the sync helper - ``field`` selects the independent watermark
        (``extracted_at`` for facts, ``episode_extracted_at`` for episodic
        segmentation). Per-turn failures are logged but never raise.
        """
        if not turn_docs:
            return 0
        now_iso = datetime.now(tz=timezone.utc).isoformat()
        marked = 0
        for turn in turn_docs:
            turn_id = turn.get("id")
            if not turn_id:
                continue
            try:
                doc_to_write = dict(turn)
                doc_to_write[field] = now_iso
                await self._upsert_item(self._turns_container, body=doc_to_write)
                marked += 1
            except Exception as exc:
                logger.warning(
                    "_mark_turns_extracted(%s) failed for turn_id=%s err=%s (turn may be re-processed on next call)",
                    field,
                    turn_id,
                    exc,
                )
        return marked

    async def extract_memories(
        self,
        user_id: str,
        thread_id: str,
        recent_k: int | None = None,
        *,
        turns: Optional[list[dict[str, Any]]] = None,
    ) -> dict[str, int]:
        """Extract facts and episodic memories from a thread and persist them."""
        extracted = await self.extract_memories_durable(user_id, thread_id, recent_k, turns=turns)
        # Capture the processed turns from the compute stage as the single source of
        # truth for stamping. Stamping happens here (not inside persist) so no
        # intermediate transform (e.g. dedup) can drop ``processed_turn_docs``
        # and cause the same turns to be re-extracted forever.
        processed_turns = extracted.get("processed_turn_docs") or []
        if get_dedup_vector_enabled():
            extracted = await self.dedup_extracted_memories(user_id, extracted)
        counts = await self.persist_extracted_memories(user_id, extracted)
        if processed_turns:
            marked = await self._mark_turns_extracted(processed_turns)
            if marked < len(processed_turns):
                logger.warning(
                    "extract_memories stamped only %d/%d processed turns as extracted user_id=%s "
                    "thread_id=%s (unstamped turns will be re-extracted next run)",
                    marked,
                    len(processed_turns),
                    user_id,
                    thread_id,
                )
            else:
                logger.info(
                    "extract_memories stamped %d processed turns as extracted user_id=%s thread_id=%s",
                    marked,
                    user_id,
                    thread_id,
                )
        return counts

    async def _load_turn_window(
        self,
        user_id: str,
        thread_id: str,
        recent_k: int | None = None,
    ) -> list[dict[str, Any]]:
        """Load the newest turns for a thread, returned in chronological order."""
        query = "SELECT * FROM c WHERE c.user_id = @user_id AND c.thread_id = @thread_id AND c.type = 'turn'"
        items = await self._query_items(
            self._turns_container,
            query=query,
            parameters=[
                {"name": "@user_id", "value": user_id},
                {"name": "@thread_id", "value": thread_id},
            ],
            partition_key=[user_id, thread_id],
        )
        items.sort(key=lambda m: m.get("created_at", ""), reverse=True)
        if recent_k is not None:
            items = items[:recent_k]
        items.reverse()
        return items

    @staticmethod
    def _ground_episode_events(
        events: Any,
        *,
        turn_ids: list[str],
    ) -> tuple[list[dict[str, Any]], list[str]]:
        """Normalize event source ids to real ids from the current turn window."""
        valid_turn_ids = set(turn_ids)
        label_to_id = {f"turn-{i}": turn_id for i, turn_id in enumerate(turn_ids, start=1)}
        grounded_events: list[dict[str, Any]] = []
        source_turn_ids: list[str] = []
        for event in events if isinstance(events, list) else []:
            if not isinstance(event, dict):
                continue
            grounded = dict(event)
            grounded_sources: list[str] = []
            for turn_id in event.get("source_turn_ids") or []:
                source = str(turn_id).strip()
                mapped = source if source in valid_turn_ids else label_to_id.get(source.lower())
                if mapped and mapped not in grounded_sources:
                    grounded_sources.append(mapped)
                if mapped and mapped not in source_turn_ids:
                    source_turn_ids.append(mapped)
            grounded["source_turn_ids"] = grounded_sources
            grounded_events.append(grounded)
        return grounded_events, source_turn_ids

    async def _build_episode_docs(
        self,
        user_id: str,
        thread_id: str,
        items: list[dict[str, Any]],
        *,
        segment_key: str,
    ) -> list[dict[str, Any]]:
        """Run the episode-extraction prompt over one bounded, already-closed turn
        segment and return episode docs (no embeddings, no writes).

        Episode ids are DETERMINISTIC from the segment identity (its turn range)
        plus each episode's ordinal - not the summary text - so a re-run over the
        same segment collides on id and is skipped rather than duplicated. Mirror
        of the sync helper.
        """
        if not items:
            return []

        transcript_items: list[dict[str, Any]] = []
        for index, item in enumerate(items, start=1):
            turn_id = str(item.get("id") or f"turn-{index}")
            copied = dict(item)
            copied["content"] = f"Turn {turn_id}: {item.get('content', '')}"
            transcript_items.append(copied)
        transcript = self._build_transcript(transcript_items, include_timestamp=True)
        response_text = await self._run_prompty("extract_episode.prompty", inputs={"transcript": transcript})
        parsed = self._parse_llm_json(response_text)
        episodes = parsed.get("episodes", [])
        if not isinstance(episodes, list):
            logger.warning(
                "_build_episode_docs dropping malformed response user_id=%s thread_id=%s payload=%r",
                user_id,
                thread_id,
                parsed,
            )
            return []

        doc_timestamp = self._stable_source_timestamp(items)
        turn_ids = [str(item.get("id")) for item in items if item.get("id")]
        segment_started, segment_ended = segment_time_bounds(items)

        episode_docs: list[dict[str, Any]] = []
        for index, episode in enumerate(episodes):
            if not isinstance(episode, dict):
                logger.warning(
                    "_build_episode_docs dropping malformed episode user_id=%s thread_id=%s payload=%r",
                    user_id,
                    thread_id,
                    episode,
                )
                continue

            summary = episode.get("summary")
            if not isinstance(summary, str) or not summary.strip():
                logger.warning(
                    "_build_episode_docs dropping malformed episode (missing summary) "
                    "user_id=%s thread_id=%s payload=%r",
                    user_id,
                    thread_id,
                    episode,
                )
                continue

            title = episode.get("title")
            if not isinstance(title, str) or not title.strip():
                logger.warning(
                    "_build_episode_docs dropping malformed episode (missing title) user_id=%s thread_id=%s payload=%r",
                    user_id,
                    thread_id,
                    episode,
                )
                continue

            if not isinstance(episode.get("events"), list):
                logger.warning(
                    "_build_episode_docs dropping malformed episode (missing events) "
                    "user_id=%s thread_id=%s payload=%r",
                    user_id,
                    thread_id,
                    episode,
                )
                continue

            content = summary
            events, source_turn_ids = self._ground_episode_events(episode.get("events"), turn_ids=turn_ids)
            content_hash = compute_content_hash(content)
            llm_started, llm_ended = episode.get("started_at"), episode.get("ended_at")
            # Trust model-supplied times only when they form a valid, self-consistent
            # ISO pair; otherwise fall back to the grounded segment bounds rather than
            # dropping the whole episode (malformed or mixed-tz strings are common).
            if is_valid_time_pair(llm_started, llm_ended):
                started_at, ended_at = str(llm_started).strip(), str(llm_ended).strip()
            else:
                started_at, ended_at = segment_started, segment_ended
            try:
                doc = construct_internal(
                    EpisodicRecord,
                    {
                        "id": self._deterministic_episode_id(segment_key, index),
                        "user_id": user_id,
                        "thread_id": thread_id,
                        "role": "system",
                        "type": "episodic",
                        "content": content,
                        "title": title,
                        "started_at": started_at,
                        "ended_at": ended_at,
                        "participants": episode.get("participants") or [],
                        "events": events,
                        "outcome": episode.get("outcome"),
                        "lessons": episode.get("lessons") or [],
                        "source_turn_ids": source_turn_ids,
                        "content_hash": content_hash,
                        "salience": clamp_unit_interval(episode.get("salience"), 0.5),
                        "confidence": clamp_unit_interval(episode.get("confidence"), 0.5),
                        "ttl": DEFAULT_TTL_BY_TYPE.get("episodic", 7_776_000),
                        "tags": ["sys:episodic", "sys:auto-extracted"],
                        "created_at": doc_timestamp,
                        "updated_at": doc_timestamp,
                        **self._prompt_lineage("extract_episode.prompty"),
                    },
                ).to_doc()
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "_build_episode_docs dropping malformed episode user_id=%s thread_id=%s err=%s payload=%r",
                    user_id,
                    thread_id,
                    exc,
                    episode,
                )
                continue
            episode_docs.append(doc)

        return episode_docs

    @staticmethod
    def _deterministic_episode_id(segment_key: str, index: int) -> str:
        return deterministic_episode_id(segment_key, index)

    async def _load_open_episode_segment(self, user_id: str, thread_id: str) -> list[dict[str, Any]]:
        """Return the open episode segment: turns not yet folded into an episode,
        oldest first. Independent ``episode_extracted_at`` watermark (mirror of
        the sync helper)."""
        items = await self._query_items(
            self._turns_container,
            query=(
                "SELECT * FROM c WHERE c.user_id = @user_id "
                "AND c.thread_id = @thread_id AND c.type = 'turn' "
                "AND (NOT IS_DEFINED(c.episode_extracted_at) OR IS_NULL(c.episode_extracted_at))"
            ),
            parameters=[
                {"name": "@user_id", "value": user_id},
                {"name": "@thread_id", "value": thread_id},
            ],
            partition_key=[user_id, thread_id],
        )
        items.sort(key=created_at_sort_key)
        return items

    async def _episode_segment_embeddings(self, segment: list[dict[str, Any]]) -> list[list[float]]:
        """One embedding per segment turn for drift detection, or ``[]`` when drift
        is disabled or embeddings are unavailable (mirror of the sync helper)."""
        if get_episode_topic_drift() <= 0:
            return []
        embeddings: list[Optional[list[float]]] = [
            turn.get("embedding") if isinstance(turn.get("embedding"), list) else None for turn in segment
        ]
        missing = [i for i, emb in enumerate(embeddings) if emb is None]
        if missing:
            try:
                fresh = await self._embed_batch([str(segment[i].get("content") or "") for i in missing])
            except Exception as exc:  # noqa: BLE001
                logger.warning("episode drift embedding failed (%s); skipping drift this evaluation", exc)
                return []
            for pos, i in enumerate(missing):
                embeddings[i] = fresh[pos] if pos < len(fresh) else None
        if any(emb is None for emb in embeddings):
            return []
        return [emb for emb in embeddings if emb is not None]

    @staticmethod
    def _turn_gap_seconds(prev_turn: dict[str, Any], cur_turn: dict[str, Any]) -> Optional[float]:
        return turn_gap_seconds(prev_turn, cur_turn)

    def _find_episode_boundary(
        self,
        segment: list[dict[str, Any]],
        embeddings: list[list[float]],
    ) -> Optional[int]:
        return find_episode_boundary(
            segment,
            embeddings,
            max_turns=get_episode_max_turns(),
            idle_gap=get_episode_idle_gap_seconds(),
            drift=get_episode_topic_drift(),
            min_turns=get_episode_min_turns(),
        )

    async def extract_episodes(
        self,
        user_id: str,
        thread_id: str,
        *,
        flush: bool = False,
    ) -> dict[str, int]:
        """Segment the open turn stream into episodes at detected boundaries.

        Mirror of the sync pipeline: the open segment is every turn without an
        ``episode_extracted_at`` stamp; at each boundary (idle time-gap, topic
        drift, or max-size cap) the closed segment is extracted, embedded, and
        persisted, then its turns are stamped. ``flush=True`` drains the trailing
        open segment. The caller never signals "session end".

        Idempotency is best-effort (not absolute): each episode's id is
        deterministic in its segment key and ordinal - not the LLM summary text -
        so re-running the same still-open segment skips the duplicate write (409)
        while the segment's turn set is stable; a partial watermark-stamp failure
        can still admit a duplicate, which episodic reconciliation does not fold.
        """
        if not user_id:
            raise ValidationError("user_id is required")
        if not thread_id:
            raise ValidationError("thread_id is required")

        segment = await self._load_open_episode_segment(user_id, thread_id)
        total = 0
        guard = 0
        while segment and guard < _EPISODE_MAX_SEGMENTS_PER_RUN:
            guard += 1
            embeddings = await self._episode_segment_embeddings(segment)
            boundary = self._find_episode_boundary(segment, embeddings)
            if boundary is None:
                if not flush:
                    break
                boundary = len(segment)
            closing = segment[:boundary]
            if not closing:
                break
            first_id = str(closing[0].get("id") or "")
            last_id = str(closing[-1].get("id") or "")
            segment_key = _ID_SEED_SEP.join((user_id, thread_id, first_id, last_id))
            try:
                docs = await self._build_episode_docs(user_id, thread_id, closing, segment_key=segment_key)
                embeddings_for_docs = await self._embed_batch([str(doc["content"]) for doc in docs]) if docs else []
            except Exception as exc:  # noqa: BLE001
                if is_retryable_llm_error(exc):
                    # Transient provider error: leave the segment un-stamped and stop
                    # this run so it is retried intact next time (mirror of the fact path).
                    logger.warning(
                        "extract_episodes: deferring %d turns after retryable extraction error "
                        "(will retry next run) user_id=%s thread_id=%s err=%s",
                        len(closing),
                        user_id,
                        thread_id,
                        exc,
                    )
                    break
                # Non-retryable (e.g. content filter, context-length): quarantine the
                # poison segment - stamp it so it never re-poisons future runs and the
                # open segment cannot grow without bound - then advance to the next.
                logger.warning(
                    "extract_episodes: quarantining %d turns after non-retryable extraction error "
                    "(marking episode_extracted_at so they do not re-poison future runs) "
                    "user_id=%s thread_id=%s err=%s",
                    len(closing),
                    user_id,
                    thread_id,
                    exc,
                )
                await self._mark_turns_extracted(closing, field="episode_extracted_at")
                segment = segment[boundary:]
                continue
            for doc, embedding in zip(docs, embeddings_for_docs):
                doc["embedding"] = embedding
                validated = self._validate_extracted_doc(doc)
                try:
                    await self._create_memory(validated)
                    total += 1
                except CosmosResourceExistsError:
                    logger.info("extract_episodes idempotent skip duplicate episode id=%s", validated.get("id"))
            await self._mark_turns_extracted(closing, field="episode_extracted_at")
            segment = segment[boundary:]
        return {"episodes": total}

    async def synthesize_procedural(
        self,
        user_id: str,
        *,
        force: bool = False,
    ) -> dict[str, Any]:
        """Synthesize the active procedural prompt for a user."""
        if not user_id:
            raise ValidationError("user_id is required")

        logger.info("synthesize_procedural started user_id=%s force=%s", user_id, force)

        async def _read_latest_procedural() -> Optional[dict[str, Any]]:
            docs = await self._query_items(
                self._memories_container,
                query=(
                    "SELECT * FROM c WHERE c.user_id = @uid "
                    "AND c.thread_id = @thread_id "
                    "AND c.type = @type "
                    f"AND {_ACTIVE_DOC_FILTER}"
                ),
                parameters=[
                    {"name": "@uid", "value": user_id},
                    {"name": "@thread_id", "value": "__procedural__"},
                    {"name": "@type", "value": "procedural"},
                ],
            )
            docs.sort(
                key=lambda doc: (int(doc.get("version") or 0), int(doc.get("_ts") or 0)),
                reverse=True,
            )
            if len(docs) > 1:
                logger.warning(
                    "synthesize_procedural found multiple active docs user_id=%s count=%d",
                    user_id,
                    len(docs),
                )
            return docs[0] if docs else None

        prior_doc = await _read_latest_procedural()

        behavioral_fact_docs = await self._query_items(
            self._memories_container,
            query=(
                "SELECT TOP 50 * FROM c WHERE c.user_id = @uid "
                "AND c.type = @type "
                f"AND {_ACTIVE_DOC_FILTER} "
                "AND ((IS_DEFINED(c.metadata.category) "
                "AND c.metadata.category IN ('preference', 'requirement')) "
                "OR (IS_DEFINED(c.salience) AND c.salience >= @min_salience)) "
                "ORDER BY c.salience DESC, c.created_at ASC, c.id ASC"
            ),
            parameters=[
                {"name": "@uid", "value": user_id},
                {"name": "@type", "value": "fact"},
                {"name": "@min_salience", "value": 0.8},
            ],
        )
        behavioral_fact_docs = [
            doc
            for doc in behavioral_fact_docs
            if isinstance(doc.get("content"), str) and doc.get("content", "").strip()
        ]
        behavioral_fact_ids = [doc["id"] for doc in behavioral_fact_docs]

        episodic_docs = await self._query_items(
            self._memories_container,
            query=(
                "SELECT TOP 50 * FROM c WHERE c.user_id = @uid "
                "AND c.type = @type "
                f"AND {_ACTIVE_DOC_FILTER} "
                "AND IS_DEFINED(c.lessons) "
                "AND ARRAY_LENGTH(c.lessons) > 0 "
                "ORDER BY c.salience DESC, c.created_at ASC, c.id ASC"
            ),
            parameters=[
                {"name": "@uid", "value": user_id},
                {"name": "@type", "value": "episodic"},
            ],
        )
        episodic_with_lessons = [
            doc
            for doc in episodic_docs
            if isinstance(doc.get("lessons"), list)
            and any(isinstance(lesson, str) and lesson.strip() for lesson in doc.get("lessons", []))
        ]
        source_episodic_ids = [doc["id"] for doc in episodic_with_lessons]

        current_source_ids = set(behavioral_fact_ids) | set(source_episodic_ids)

        def _covered_by(prior: Optional[dict[str, Any]]) -> bool:
            if prior is None:
                return False
            covered = set(prior.get("source_fact_ids") or []) | set(prior.get("source_episodic_ids") or [])
            return current_source_ids.issubset(covered)

        if prior_doc and not force and _covered_by(prior_doc):
            logger.info(
                "synthesize_procedural unchanged user_id=%s fact_count=%d episodic_count=%d",
                user_id,
                len(behavioral_fact_ids),
                len(source_episodic_ids),
            )
            return {"status": "unchanged", "procedural": prior_doc}

        if not current_source_ids:
            logger.info(
                "synthesize_procedural skipping LLM user_id=%s - no behavioral facts or episodic lessons",
                user_id,
            )
            return {"status": "unchanged", "procedural": prior_doc}

        user_name = "the user"

        def _render_bullets(values: list[str]) -> str:
            cleaned = [value.strip() for value in values if isinstance(value, str) and value.strip()]
            if not cleaned:
                return "(none)"
            return "\n".join(f"- {value}" for value in cleaned)

        static_prompty_inputs = {
            "behavioral_facts": _render_bullets([doc.get("content", "") for doc in behavioral_fact_docs]),
            "episodic_lessons": _render_bullets(
                [
                    lesson
                    for doc in episodic_with_lessons
                    for lesson in doc.get("lessons", [])
                    if isinstance(lesson, str) and lesson.strip()
                ]
            ),
            "user_name": user_name,
        }

        # Retry loop: LLM call lives inside so that on a race-induced 409
        # we (a) check whether the winner already covers our source set and
        # short-circuit if so, and (b) re-call the LLM with the winner as
        # the new prior if not - keeping synthesized content monotonic in
        # source coverage, not just version number.
        written_doc: Optional[dict[str, Any]] = None
        for attempt in range(1, _PROCEDURAL_MAX_CREATE_ATTEMPTS + 1):
            response_text = await self._run_prompty(
                "synthesize_procedural.prompty",
                inputs={
                    "prior_prompt": (prior_doc.get("content") or "") if prior_doc else "",
                    **static_prompty_inputs,
                },
            )

            parsed = self._parse_llm_json(response_text)
            system_prompt = parsed.get("system_prompt") if isinstance(parsed, dict) else None
            if not isinstance(system_prompt, str) or not system_prompt.strip():
                raise LLMError("synthesize_procedural returned JSON without a non-empty 'system_prompt' string")
            system_prompt = system_prompt.strip()

            new_seq = (int(prior_doc.get("version") or 0) + 1) if prior_doc else 1
            new_doc: dict[str, Any] = {
                "id": f"proc_{user_id}_{new_seq}",
                "user_id": user_id,
                "thread_id": "__procedural__",
                "type": "procedural",
                "version": new_seq,
                "content": system_prompt,
                "source_fact_ids": behavioral_fact_ids,
                "source_episodic_ids": source_episodic_ids,
                "supersedes_ids": [prior_doc["id"]] if prior_doc else [],
                "created_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "role": "system",
                "tags": ["sys:procedural", "sys:synthesized"],
                **self._prompt_lineage("synthesize_procedural.prompty"),
                "metadata": {},
            }
            validated = construct_internal(ProceduralRecord, new_doc).to_doc()
            try:
                await self._create_item(self._memories_container, body=dict(validated))
                written_doc = validated
                break
            except CosmosResourceExistsError:
                logger.info(
                    "synthesize_procedural id collision user_id=%s seq=%d attempt=%d/%d - re-reading",
                    user_id,
                    new_seq,
                    attempt,
                    _PROCEDURAL_MAX_CREATE_ATTEMPTS,
                )
                latest = await _read_latest_procedural()
                if latest is None:
                    continue
                prior_doc = latest
                if _covered_by(prior_doc):
                    logger.info(
                        "synthesize_procedural race resolved by coverage user_id=%s winner=%s",
                        user_id,
                        prior_doc["id"],
                    )
                    return {"status": "unchanged", "procedural": prior_doc}
        if written_doc is None:
            raise MemoryConflictError(
                "synthesize_procedural failed after "
                f"{_PROCEDURAL_MAX_CREATE_ATTEMPTS} attempts due to id collisions "
                f"user_id={user_id!r}"
            )

        new_id = written_doc["id"]
        if prior_doc:
            await self._mark_superseded(prior_doc, new_id, reason="update")

        logger.info(
            "synthesize_procedural synthesized user_id=%s version=%d fact_count=%d episodic_count=%d",
            user_id,
            written_doc["version"],
            len(behavioral_fact_ids),
            len(source_episodic_ids),
        )
        return {"status": "synthesized", "procedural": written_doc}

    async def generate_thread_summary_durable(
        self,
        user_id: str,
        thread_id: str,
        recent_k: int | None = None,
    ) -> dict[str, Any]:
        """Generate or update a thread summary document without embedding or writing it."""
        if not user_id:
            raise ValidationError("user_id is required")
        if not thread_id:
            raise ValidationError("thread_id is required")

        logger.info("generate_thread_summary_durable started user_id=%s thread_id=%s", user_id, thread_id)

        summary_id = f"summary_{user_id}_{thread_id}"
        existing_summary: Optional[dict[str, Any]] = None
        try:
            existing_summary = await self._read_item(
                self._summaries_container,
                item=summary_id,
                partition_key=[user_id, thread_id],
            )
        except CosmosResourceNotFoundError:
            pass

        query = "SELECT * FROM c WHERE c.user_id = @user_id AND c.thread_id = @thread_id AND c.type = 'turn'"
        parameters: list[dict[str, Any]] = [
            {"name": "@user_id", "value": user_id},
            {"name": "@thread_id", "value": thread_id},
        ]
        if existing_summary:
            since = existing_summary["updated_at"]
            query += " AND c.created_at > @since"
            parameters.append({"name": "@since", "value": since})

        items = await self._query_items(
            self._turns_container,
            query=query,
            parameters=parameters,
            partition_key=[user_id, thread_id],
        )

        if existing_summary and not items:
            logger.info("generate_thread_summary_durable no new memories, returning existing")
            summary_doc = dict(existing_summary)
            summary_doc.pop("embedding", None)
            return summary_doc
        if not existing_summary and not items:
            raise ValidationError(f"No memories found for user_id={user_id!r}, thread_id={thread_id!r}")

        items.sort(key=lambda m: m.get("created_at", ""), reverse=True)
        if recent_k is not None:
            items = items[:recent_k]
        items.reverse()

        transcript = self._build_transcript(items)
        if existing_summary:
            prior_json = existing_summary.get("metadata", {}).get("structured_summary")
            prior_text = json.dumps(prior_json, indent=2) if prior_json else existing_summary.get("content", "")
            response_text = await self._run_prompty(
                "summarize_update.prompty",
                inputs={"prior_summary": prior_text, "transcript": transcript},
            )
            summary_prompt_filename = "summarize_update.prompty"
        else:
            response_text = await self._run_prompty("summarize.prompty", inputs={"transcript": transcript})
            summary_prompt_filename = "summarize.prompty"

        parsed = self._parse_llm_json(response_text)
        parsed = _cap_structured_summary(parsed)
        overview = parsed.get("overview", response_text)
        topics = parsed.get("topics", [])
        total_source_count = (
            existing_summary.get("metadata", {}).get("source_count", 0) if existing_summary else 0
        ) + len(items)
        topic_tags = build_topic_tags(topics)
        doc_timestamp = self._stable_source_timestamp(items)
        summary_doc: dict[str, Any] = {
            "id": summary_id,
            "user_id": user_id,
            "thread_id": thread_id,
            "role": "system",
            "type": "thread_summary",
            "content": overview,
            "salience": 1.0,
            "tags": ["sys:summary"] + topic_tags,
            **self._prompt_lineage(summary_prompt_filename),
            "metadata": {
                "structured_summary": parsed,
                "source_count": total_source_count,
                "recent_k": recent_k,
                "incremental_update": existing_summary is not None,
            },
            "created_at": existing_summary["created_at"] if existing_summary else doc_timestamp,
            "updated_at": doc_timestamp,
        }
        return construct_internal(ThreadSummaryRecord, summary_doc).to_doc()

    async def persist_thread_summary(
        self,
        user_id: str,
        thread_id: str,
        summary_doc: dict[str, Any],
    ) -> dict[str, Any]:
        """Compute the summary embedding and upsert the deterministic summary doc."""
        if not user_id:
            raise ValidationError("user_id is required")
        if not thread_id:
            raise ValidationError("thread_id is required")
        if not isinstance(summary_doc, dict):
            raise ValidationError("summary_doc must be a dict")

        doc = dict(summary_doc)
        doc["id"] = doc.get("id") or f"summary_{user_id}_{thread_id}"
        doc["user_id"] = user_id
        doc["thread_id"] = thread_id
        doc.setdefault("prompt_id", "summarize.prompty")
        doc.setdefault("prompt_version", "v1")
        if doc.get("content") and not doc.get("embedding"):
            doc["embedding"] = await self._embed_one(doc["content"])
        validated = construct_internal(ThreadSummaryRecord, doc).to_doc()
        stored = await self._upsert_summary(validated)
        logger.info("persist_thread_summary completed id=%s", validated.get("id"))
        return stored

    async def generate_thread_summary(
        self,
        user_id: str,
        thread_id: str,
        recent_k: int | None = None,
    ) -> dict[str, Any]:
        """Generate or incrementally update a thread summary and persist it."""
        summary_doc = await self.generate_thread_summary_durable(user_id, thread_id, recent_k=recent_k)
        return await self.persist_thread_summary(user_id, thread_id, summary_doc)

    async def generate_user_summary_durable(
        self,
        user_id: str,
        thread_ids: list[str] | None = None,
        recent_k: int | None = None,
    ) -> dict[str, Any]:
        """Generate a user summary document without embedding or writing it."""
        if not user_id:
            raise ValidationError("user_id is required")

        logger.info(
            "generate_user_summary_durable started user_id=%s observed_thread_ids=%s",
            user_id,
            len(thread_ids) if thread_ids else 0,
        )

        user_summary_id = f"user_summary_{user_id}"
        existing_summary: Optional[dict[str, Any]] = None
        try:
            existing_summary = await self._read_item(
                self._summaries_container,
                item=user_summary_id,
                partition_key=[user_id, "__user_summary__"],
            )
        except CosmosResourceNotFoundError:
            pass

        query_predicate = "c.user_id = @user_id"
        parameters: list[dict[str, Any]] = [{"name": "@user_id", "value": user_id}]
        if existing_summary:
            since = existing_summary["updated_at"]
            query_predicate += " AND c.created_at > @since"
            parameters.append({"name": "@since", "value": since})

        memories_query = f"SELECT * FROM c WHERE {query_predicate} AND c.type IN ('fact', 'episodic', 'procedural')"
        summaries_query = f"SELECT * FROM c WHERE {query_predicate} AND c.type = 'thread_summary'"

        items = await self._query_items(
            self._memories_container,
            query=memories_query,
            parameters=parameters,
        )
        items.extend(
            await self._query_items(
                self._summaries_container,
                query=summaries_query,
                parameters=parameters,
            )
        )

        if existing_summary and not items:
            logger.info("generate_user_summary_durable no new memories, returning existing")
            user_doc = dict(existing_summary)
            user_doc.pop("embedding", None)
            return user_doc
        if not existing_summary and not items:
            raise ValidationError(f"No memories found for user_id={user_id!r}")

        items.sort(key=lambda m: m.get("created_at", ""), reverse=True)
        if recent_k is not None:
            by_thread: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for m in items:
                by_thread[m.get("thread_id", "")].append(m)
            trimmed: list[dict[str, Any]] = []
            for thread_items in by_thread.values():
                trimmed.extend(thread_items[:recent_k])
            trimmed.sort(key=lambda m: m.get("created_at", ""))
            items = trimmed
        else:
            items.reverse()

        transcript = self._build_transcript(items, group_by_thread=True)
        new_thread_ids = {m.get("thread_id", "") for m in items}
        if existing_summary:
            prior_json = existing_summary.get("metadata", {}).get("structured_summary")
            prior_text = json.dumps(prior_json, indent=2) if prior_json else existing_summary.get("content", "")
            response_text = await self._run_prompty(
                "user_summary_update.prompty",
                inputs={"prior_summary": prior_text, "transcript": transcript},
            )
            prompt_filename = "user_summary_update.prompty"
        else:
            response_text = await self._run_prompty("user_summary.prompty", inputs={"transcript": transcript})
            prompt_filename = "user_summary.prompty"

        parsed = self._parse_llm_json(response_text)
        parsed = _cap_structured_summary(parsed)
        key_facts = parsed.get("key_facts", [])
        overview = "; ".join(key_facts) if key_facts else response_text
        if existing_summary:
            old_thread_ids = set(existing_summary.get("metadata", {}).get("thread_ids", []))
            all_thread_ids = sorted(old_thread_ids | new_thread_ids)
            old_memory_count = existing_summary.get("metadata", {}).get("source_memory_count", 0)
            total_memory_count = old_memory_count + len(items)
        else:
            all_thread_ids = sorted(new_thread_ids)
            total_memory_count = len(items)

        topic_tags = build_topic_tags(parsed.get("topics", []))
        doc_timestamp = self._stable_source_timestamp(items)
        summary_doc: dict[str, Any] = {
            "id": user_summary_id,
            "user_id": user_id,
            "thread_id": "__user_summary__",
            "role": "system",
            "type": "user_summary",
            "content": overview,
            "salience": 1.0,
            "tags": ["sys:user-summary"] + topic_tags,
            **self._prompt_lineage(prompt_filename),
            "metadata": {
                "structured_summary": parsed,
                "source_thread_count": len(all_thread_ids),
                "source_memory_count": total_memory_count,
                "thread_ids": all_thread_ids,
                "recent_k": recent_k,
                "incremental_update": existing_summary is not None,
            },
            "created_at": existing_summary["created_at"] if existing_summary else doc_timestamp,
            "updated_at": doc_timestamp,
        }
        return construct_internal(UserSummaryRecord, summary_doc).to_doc()

    async def persist_user_summary(
        self,
        user_id: str,
        user_summary_doc: dict[str, Any],
    ) -> dict[str, Any]:
        """Compute the user-summary embedding and upsert the deterministic doc."""
        if not user_id:
            raise ValidationError("user_id is required")
        if not isinstance(user_summary_doc, dict):
            raise ValidationError("user_summary_doc must be a dict")

        doc = dict(user_summary_doc)
        doc["id"] = doc.get("id") or f"user_summary_{user_id}"
        doc["user_id"] = user_id
        doc["thread_id"] = "__user_summary__"
        doc.setdefault("prompt_id", "user_summary.prompty")
        doc.setdefault("prompt_version", "v1")
        structured_summary = doc.get("metadata", {}).get("structured_summary")
        topics = structured_summary.get("topics", []) if isinstance(structured_summary, dict) else []
        doc["tags"] = sorted({*(doc.get("tags") or []), "sys:user-summary", *build_topic_tags(topics)})
        if doc.get("content") and not doc.get("embedding"):
            doc["embedding"] = await self._embed_one(doc["content"])
        validated = construct_internal(UserSummaryRecord, doc).to_doc()
        stored = await self._upsert_summary(validated)
        logger.info("persist_user_summary completed id=%s", validated.get("id"))
        return stored

    async def generate_user_summary(
        self,
        user_id: str,
        thread_ids: list[str] | None = None,
        recent_k: int | None = None,
    ) -> dict[str, Any]:
        """Generate or incrementally update a user summary and persist it."""
        summary_doc = await self.generate_user_summary_durable(user_id, thread_ids=thread_ids, recent_k=recent_k)
        return await self.persist_user_summary(user_id, summary_doc)

    def _emit_reconcile_outcome(
        self,
        *,
        started_at: float,
        user_id: str,
        candidates: int,
        result: dict[str, int],
    ) -> None:
        duration_ms = (time.monotonic() - started_at) * 1000.0
        logger.info(
            "reconcile.outcome",
            extra={
                "operation": "reconcile_memories",
                "user_id": user_id,
                "candidates_considered": candidates,
                "kept": result["kept"],
                "merged": result["merged"],
                "contradicted": result["contradicted"],
                "duration_ms": duration_ms,
                "prompt_id": "dedup.prompty",
                "prompt_version": "v1",
            },
        )

    async def _active_memories_for_reconcile(self, user_id: str, memory_type: str, n: int) -> list[dict[str, Any]]:
        capped_n = top_literal(n, name="reconcile_memories.n")
        # Agent-sourced facts (sys:agent-fact) are excluded: they record what the
        # agent did/recommended (historical events), not mutable user state, so
        # they must never be contradiction-superseded by a later user statement.
        query = (
            f"SELECT TOP {capped_n} * FROM c "
            "WHERE c.user_id = @user_id "
            "AND c.type = @memory_type "
            f"AND {_ACTIVE_DOC_FILTER} "
            "AND NOT ARRAY_CONTAINS(c.tags, 'sys:agent-fact') "
            "ORDER BY c.created_at DESC"
        )
        return await self._query_items(
            self._memories_container,
            query=query,
            parameters=[
                {"name": "@user_id", "value": user_id},
                {"name": "@memory_type", "value": memory_type},
            ],
        )

    async def _load_memories_by_ids(
        self,
        user_id: str,
        memory_type: str,
        ids: Iterable[str],
    ) -> list[dict[str, Any]]:
        id_list = [mid for mid in dict.fromkeys(ids) if mid]
        if not id_list:
            return []
        placeholders = ", ".join(f"@id{i}" for i in range(len(id_list)))
        query = (
            "SELECT * FROM c WHERE c.user_id = @user_id "
            "AND c.type = @memory_type "
            f"AND c.id IN ({placeholders}) "
            f"AND {_ACTIVE_DOC_FILTER}"
        )
        parameters = [
            {"name": "@user_id", "value": user_id},
            {"name": "@memory_type", "value": memory_type},
        ]
        parameters.extend({"name": f"@id{i}", "value": mid} for i, mid in enumerate(id_list))
        return await self._query_items(self._memories_container, query=query, parameters=parameters)

    async def reconcile_memories(self, user_id: str, n: int = 50, *, memory_type: str = "fact") -> dict[str, int]:
        """Resolve contradictions among a user's most-recent active memories.

        Async mirror of the sync contradiction-only reconcile. Near-duplicate
        paraphrases are folded in place at write time
        (:meth:`dedup_extracted_memories`); this pass only supersedes the loser
        of each ``contradicted_pairs`` entry - no clustering, no merged
        documents, no re-merge churn. Episodic and procedural types are no-ops.
        Returns ``{"kept", "merged", "contradicted"}`` with ``merged`` always 0.
        """
        if not user_id:
            raise ValidationError("user_id is required")
        if not isinstance(n, int) or isinstance(n, bool) or n < 1:
            raise ValidationError(f"n must be a positive integer, got {n!r}")
        if n > 500:
            raise ValidationError(f"n must be <= 500 to bound prompt size and LLM cost, got {n}")
        if memory_type not in {"fact", "episodic", "procedural"}:
            raise ValidationError(f"memory_type must be one of fact, episodic, procedural, got {memory_type!r}")
        if memory_type in {"episodic", "procedural"}:
            result = {"kept": 0, "merged": 0, "contradicted": 0}
            logger.info("reconcile_memories %s no-op user_id=%s result=%s", memory_type, user_id, result)
            return result

        started_at = time.monotonic()
        logger.info("reconcile_memories started user_id=%s n=%d memory_type=%s", user_id, n, memory_type)

        facts = await self._active_memories_for_reconcile(user_id, memory_type, n)
        result = await self._reconcile_contradictions(user_id, memory_type, facts)
        self._emit_reconcile_outcome(
            started_at=started_at,
            user_id=user_id,
            candidates=len(facts),
            result=result,
        )
        return result

    async def _reconcile_contradictions(
        self, user_id: str, memory_type: str, facts: list[dict[str, Any]]
    ) -> dict[str, int]:
        """Async mirror: resolve only ``contradicted_pairs`` within the pool.

        Paraphrases are not merged here; the dedup prompt/schema no longer emits
        duplicate groups because write-time in-place dedup already folds them, so
        no merged docs are minted and the pass is convergent. Returns
        ``{"kept", "merged": 0, "contradicted"}``.
        """
        if len(facts) <= 1:
            return {"kept": len(facts), "merged": 0, "contradicted": 0}

        lines: list[str] = []
        for i, cf in enumerate(facts, 1):
            content_quoted = json.dumps(cf.get("content", ""), ensure_ascii=False)
            conf_raw = cf.get("confidence")
            sal_raw = cf.get("salience")
            conf_str = conf_raw if _is_real_number(conf_raw) else "N/A"
            sal_str = sal_raw if _is_real_number(sal_raw) else "N/A"
            created_raw = cf.get("created_at")
            created_str = created_raw if created_raw else "N/A"
            lines.append(
                f"{i}. ID: {cf['id']} | Content: {content_quoted} | "
                f"Confidence: {conf_str} | Salience: {sal_str} | Created: {created_str}"
            )
        facts_text = "\n".join(lines)

        response_text = await self._run_prompty("dedup.prompty", inputs={"facts_text": facts_text})
        parsed = self._parse_llm_json(response_text)
        contradicted_pairs = parsed.get("contradicted_pairs", []) or []

        facts_by_id: dict[str, dict[str, Any]] = {f["id"]: f for f in facts}
        contradicted = 0
        consumed_loser_ids: set[str] = set()
        for pair in contradicted_pairs:
            winner_id = pair.get("winner_id")
            loser_id = pair.get("loser_id")
            if not winner_id or not loser_id or winner_id == loser_id:
                continue
            if winner_id not in facts_by_id:
                logger.warning(
                    "reconcile_memories: hallucinated winner_id=%s not in pool; skipping pair %r",
                    winner_id,
                    pair,
                )
                continue
            # Guard chained contradictions (A>B then B>C) and re-supersession.
            if winner_id in consumed_loser_ids or loser_id in consumed_loser_ids:
                logger.info(
                    "reconcile_memories: skipping chained/duplicate contradiction pair %r "
                    "(winner or loser already superseded this pass)",
                    pair,
                )
                continue
            loser_doc = facts_by_id.get(loser_id)
            if loser_doc is None:
                continue
            if await self._mark_superseded(loser_doc, winner_id, reason="contradict"):
                contradicted += 1
                consumed_loser_ids.add(loser_id)

        kept = len([fid for fid in facts_by_id if fid not in consumed_loser_ids])
        result = {"kept": kept, "merged": 0, "contradicted": contradicted}
        logger.info(
            "reconcile_memories contradiction pass user_id=%s memory_type=%s result=%s",
            user_id,
            memory_type,
            result,
        )
        return result

    async def build_procedural_context(self, user_id: str) -> str:
        """Return the active synthesized procedural prompt for system injection."""
        if not user_id:
            raise ValidationError("user_id is required")
        query = (
            "SELECT TOP 1 c.content, c.version FROM c WHERE c.user_id = @user_id "
            "AND c.thread_id = @thread_id AND c.type = @type "
            f"AND {_ACTIVE_DOC_FILTER} "
            "ORDER BY c.version DESC"
        )
        items = await self._query_items(
            self._memories_container,
            query=query,
            parameters=[
                {"name": "@user_id", "value": user_id},
                {"name": "@thread_id", "value": "__procedural__"},
                {"name": "@type", "value": "procedural"},
            ],
        )
        if not items:
            return ""
        content = items[0].get("content")
        return content if isinstance(content, str) else ""


__all__ = ["AsyncPipelineService"]
