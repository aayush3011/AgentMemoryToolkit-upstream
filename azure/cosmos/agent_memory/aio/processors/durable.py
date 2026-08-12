"""Async marker :class:`AsyncMemoryProcessor` for the Durable Function backend."""

from __future__ import annotations

from typing import Any, Optional

from azure.cosmos.agent_memory.logging import get_logger
from azure.cosmos.agent_memory.processors.base import (
    ProcessThreadResult,
    UserSummaryResult,
)

logger = get_logger(__name__)


class AsyncDurableFunctionProcessor:
    """Async mirror of :class:`DurableFunctionProcessor`.

    All ``process_*`` coroutines short-circuit and return empty results.
    """

    async def process_thread(
        self,
        *,
        user_id: str,
        thread_id: str,
        turns: list[dict[str, Any]],
        existing_memories: Optional[list[dict[str, Any]]] = None,
    ) -> ProcessThreadResult:
        logger.debug(
            "AsyncDurableFunctionProcessor.process_thread no-op user_id=%s thread_id=%s n_turns=%d",
            user_id,
            thread_id,
            len(turns) if turns else 0,
        )
        return ProcessThreadResult(thread_summary=None, extracted_counts={}, reconciled_count=0, elapsed_ms=0)

    async def process_extract_memories(
        self,
        *,
        user_id: str,
        thread_id: str,
        recent_k: Optional[int] = None,
    ) -> dict[str, int]:
        logger.debug(
            "AsyncDurableFunctionProcessor.process_extract_memories no-op user_id=%s thread_id=%s",
            user_id,
            thread_id,
        )
        return {}

    async def process_extract_episodes(
        self,
        *,
        user_id: str,
        thread_id: str,
    ) -> dict[str, int]:
        # The Durable Function app owns episodic extraction via the Cosmos DB
        # Change Feed trigger: ExtractEpisodesOrchestrator -> ee_ExtractEpisodes
        # -> pipeline.extract_episodes. This hook mirrors synthesize_procedural
        # by leaving Durable-owned work to the orchestrator.
        logger.debug(
            "AsyncDurableFunctionProcessor.process_extract_episodes no-op user_id=%s thread_id=%s",
            user_id,
            thread_id,
        )
        return {}

    async def process_thread_summary(
        self,
        *,
        user_id: str,
        thread_id: str,
    ) -> Optional[dict[str, Any]]:
        logger.debug(
            "AsyncDurableFunctionProcessor.process_thread_summary no-op user_id=%s thread_id=%s",
            user_id,
            thread_id,
        )
        return None

    async def process_user_summary(
        self,
        *,
        user_id: str,
        thread_ids: Optional[list[str]] = None,
    ) -> UserSummaryResult:
        logger.debug(
            "AsyncDurableFunctionProcessor.process_user_summary no-op user_id=%s",
            user_id,
        )
        return UserSummaryResult(summary=None)

    async def process_reconcile(self, *, user_id: str) -> int:
        logger.debug(
            "AsyncDurableFunctionProcessor.process_reconcile no-op user_id=%s",
            user_id,
        )
        return 0

    async def generate_user_summary(
        self,
        *,
        user_id: str,
        thread_summaries: list[dict[str, Any]],
    ) -> UserSummaryResult:
        logger.debug(
            "AsyncDurableFunctionProcessor.generate_user_summary no-op user_id=%s n_summaries=%d",
            user_id,
            len(thread_summaries) if thread_summaries else 0,
        )
        return UserSummaryResult(summary=None)

    async def synthesize_procedural(
        self,
        *,
        user_id: str,
        force: bool = False,
    ) -> dict[str, Any]:
        # No-op, like the other durable hooks (mirror of the sync processor):
        # procedural synthesis runs in the Durable Function app after reconcile,
        # so returning instead of raising keeps the in-process auto-trigger from
        # stamping a spurious failure each cadence.
        del force
        logger.debug("DurableFunctionProcessor.synthesize_procedural no-op user_id=%s", user_id)
        return {"status": "skipped", "procedures_created": 0}

    async def close(self) -> None:
        logger.debug("AsyncDurableFunctionProcessor.close no-op")
        return None


__all__ = ["AsyncDurableFunctionProcessor"]
