"""Project verbose Cosmos documents into compact, token-lean tool payloads.

The SDK returns raw Cosmos documents that carry embeddings and Cosmos system
fields (``_rid``, ``_etag`` …). Those waste the model's context window and never
help the agent, so we strip them and normalize a few field names before handing
records back through MCP.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Optional

# Fields that must never reach the agent (embeddings + Cosmos internals).
_DENY_KEYS = frozenset(
    {
        "_rid",
        "_self",
        "_etag",
        "_attachments",
        "_ts",
        "_lsn",
        "embedding",
        "embeddings",
        "content_vector",
        "contentVector",
        "vector",
        "embedding_vector",
    }
)

# Heuristic: drop any list-valued field whose name looks like a raw vector.
_VECTOR_SUFFIXES = ("vector", "embedding")


def _looks_like_vector(key: str, value: Any) -> bool:
    lowered = key.lower()
    return isinstance(value, list) and lowered.endswith(_VECTOR_SUFFIXES) and all(
        isinstance(x, (int, float)) for x in value[:4]
    )


def project_record(doc: Mapping[str, Any]) -> dict[str, Any]:
    """Return a copy of ``doc`` without embeddings / Cosmos system fields.

    Also normalizes ``type`` -> ``memory_type`` so every payload exposes the
    memory type under a single, predictable key.
    """
    if not isinstance(doc, Mapping):
        return doc  # pass through non-dicts (e.g. plain strings) unchanged
    out: dict[str, Any] = {}
    for key, value in doc.items():
        if key in _DENY_KEYS or _looks_like_vector(key, value):
            continue
        out[key] = value
    if "type" in out and "memory_type" not in out:
        out["memory_type"] = out.pop("type")
    return out


def project_records(docs: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Project a sequence of documents (see :func:`project_record`)."""
    return [project_record(d) for d in docs]


def list_payload(
    docs: Iterable[Mapping[str, Any]],
    *,
    limit: Optional[int] = None,
) -> dict[str, Any]:
    """Build a ``{items, count, truncated}`` payload from raw documents.

    When ``limit`` is given, the item list is capped and ``truncated`` reflects
    whether records were dropped to respect the cap.
    """
    projected = project_records(docs)
    truncated = False
    if limit is not None and len(projected) > limit:
        projected = projected[:limit]
        truncated = True
    return {"items": projected, "count": len(projected), "truncated": truncated}
