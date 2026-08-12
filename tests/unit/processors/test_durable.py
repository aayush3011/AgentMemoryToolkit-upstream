"""Tests for DurableFunctionProcessor - verifies all methods are no-ops."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from azure.cosmos.agent_memory.cosmos_memory_client import CosmosMemoryClient
from azure.cosmos.agent_memory.processors import (
    DurableFunctionProcessor,
    ProcessThreadResult,
    UserSummaryResult,
)

OLD_EPISODIC_DURABLE_WARNING = "Episodic memory is not available under the Durable Functions backend"


def test_process_thread_returns_empty_result():
    proc = DurableFunctionProcessor()
    result = proc.process_thread(
        user_id="u1",
        thread_id="t1",
        turns=[{"role": "user", "content": "hi"}],
    )
    assert isinstance(result, ProcessThreadResult)
    assert result.thread_summary is None
    assert result.extracted_counts == {}
    assert result.reconciled_count == 0
    assert result.elapsed_ms == 0


def test_generate_user_summary_returns_empty_result():
    proc = DurableFunctionProcessor()
    result = proc.generate_user_summary(user_id="u1", thread_summaries=[{"thread_id": "t1"}])
    assert isinstance(result, UserSummaryResult)
    assert result.summary is None


def test_process_extract_episodes_returns_empty_result_without_old_warning(caplog):
    proc = DurableFunctionProcessor()
    caplog.set_level(logging.WARNING)

    result = proc.process_extract_episodes(user_id="u1", thread_id="t1")

    assert result == {}
    assert OLD_EPISODIC_DURABLE_WARNING not in caplog.text


def test_synthesize_procedural_is_noop():
    # Durable procedural synthesis runs in the Function app after reconcile; the
    # processor no-ops (rather than raising) so the in-process auto-trigger does
    # not stamp a spurious failure each cadence.
    proc = DurableFunctionProcessor()
    result = proc.synthesize_procedural(user_id="u1")
    assert result == {"status": "skipped", "procedures_created": 0}


def test_client_extract_episodes_raises_for_durable_processor():
    client = CosmosMemoryClient(use_default_credential=False, processor=DurableFunctionProcessor())
    client._pipeline = MagicMock()

    with pytest.raises(NotImplementedError, match="Durable Function app"):
        client.extract_episodes("u1", "t1", flush=True)

    client._pipeline.extract_episodes.assert_not_called()


def test_close_is_noop():
    assert DurableFunctionProcessor().close() is None


def test_does_not_invoke_pipeline():
    """Sanity check: instantiating + calling never imports/uses ProcessingPipeline."""
    proc = DurableFunctionProcessor()
    # No pipeline attribute should exist
    assert not hasattr(proc, "_pipeline")
    proc.process_thread(user_id="u", thread_id="t", turns=[])
    proc.generate_user_summary(user_id="u", thread_summaries=[])
