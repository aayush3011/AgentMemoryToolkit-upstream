"""Tests for AsyncDurableFunctionProcessor - verifies all methods are no-ops."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from azure.cosmos.agent_memory.aio.cosmos_memory_client import AsyncCosmosMemoryClient
from azure.cosmos.agent_memory.aio.processors import (
    AsyncDurableFunctionProcessor,
    ProcessThreadResult,
    UserSummaryResult,
)

OLD_EPISODIC_DURABLE_WARNING = "Episodic memory is not available under the Durable Functions backend"


@pytest.mark.asyncio
async def test_process_thread_returns_empty_result():
    proc = AsyncDurableFunctionProcessor()
    result = await proc.process_thread(user_id="u", thread_id="t", turns=[{"role": "user"}])
    assert isinstance(result, ProcessThreadResult)
    assert result.thread_summary is None
    assert result.extracted_counts == {}
    assert result.reconciled_count == 0
    assert result.elapsed_ms == 0


@pytest.mark.asyncio
async def test_generate_user_summary_returns_empty_result():
    proc = AsyncDurableFunctionProcessor()
    result = await proc.generate_user_summary(user_id="u", thread_summaries=[{"thread_id": "t"}])
    assert isinstance(result, UserSummaryResult)
    assert result.summary is None


@pytest.mark.asyncio
async def test_process_extract_episodes_returns_empty_result_without_old_warning(caplog):
    proc = AsyncDurableFunctionProcessor()
    caplog.set_level(logging.WARNING)

    result = await proc.process_extract_episodes(user_id="u1", thread_id="t1")

    assert result == {}
    assert OLD_EPISODIC_DURABLE_WARNING not in caplog.text


@pytest.mark.asyncio
async def test_synthesize_procedural_is_noop():
    # Mirror of the sync processor: no-op (not raise) so the auto-trigger does
    # not stamp a spurious failure each cadence.
    proc = AsyncDurableFunctionProcessor()
    result = await proc.synthesize_procedural(user_id="u1")
    assert result == {"status": "skipped", "procedures_created": 0}


@pytest.mark.asyncio
async def test_client_extract_episodes_raises_for_durable_processor():
    client = AsyncCosmosMemoryClient(use_default_credential=False, processor=AsyncDurableFunctionProcessor())
    client._pipeline = MagicMock()

    with pytest.raises(NotImplementedError, match="Durable Function app"):
        await client.extract_episodes("u1", "t1", flush=True)

    client._pipeline.extract_episodes.assert_not_called()


@pytest.mark.asyncio
async def test_close_is_noop():
    assert await AsyncDurableFunctionProcessor().close() is None


def test_does_not_carry_pipeline():
    proc = AsyncDurableFunctionProcessor()
    assert not hasattr(proc, "_pipeline")
