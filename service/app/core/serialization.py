DEFAULT_TOP_K_CEILING = 50

# Fields never returned to clients: the raw embedding vector (large, opaque) and
# any Cosmos system field (``_rid``, ``_self``, ``_etag``, ``_attachments``,
# ``_ts``, ``_lsn``, ...). Everything else on the document is passed through so
# callers get the full record and can filter client-side.
_ALWAYS_STRIP = {"embedding", "embeddings"}


def _strip_internal(doc: dict) -> dict:
    """Return a shallow copy of a Cosmos doc with embeddings and system fields
    (any key starting with an underscore) removed."""
    return {k: v for k, v in doc.items() if k not in _ALWAYS_STRIP and not k.startswith("_")}


def serialize_memory(doc: dict) -> dict:
    """Return the full memory document, minus the embedding vector and Cosmos
    system fields.

    Everything the store persisted is passed through (content, role, tags,
    confidence, salience, created_at/updated_at, thread_id, score, metadata,
    source ids, and - for episodic memories - title, started_at, ended_at,
    participants, events, outcome, lessons). ``memory_type`` is added as an alias
    of the document's ``type`` for convenience; the original ``type`` is kept
    too. Callers filter down to whatever they want to show.
    """
    payload = _strip_internal(doc)
    if payload.get("type") is not None and "memory_type" not in payload:
        payload["memory_type"] = payload["type"]
    return payload


def list_envelope(docs: list[dict], top_k: int | None = None) -> dict:
    """Return a capped list envelope of full memory payloads."""
    requested = DEFAULT_TOP_K_CEILING if top_k is None else top_k
    cap = max(0, min(requested, DEFAULT_TOP_K_CEILING))
    capped = docs[:cap]
    return {
        "items": [serialize_memory(d) for d in capped],
        "count": len(capped),
        "truncated": len(docs) > cap,
    }


def serialize_summary(doc: dict) -> dict:
    """Return the full thread/user summary document, minus the embedding vector
    and Cosmos system fields.

    Everything persisted is passed through (content, thread_id,
    created_at/updated_at, tags, metadata, ...). ``memory_type`` is added as an
    alias of ``type``, and the structured summary (nested under ``metadata``) is
    also surfaced at the top level as ``structured_summary`` for convenience.
    """
    payload = _strip_internal(doc)
    if payload.get("type") is not None and "memory_type" not in payload:
        payload["memory_type"] = payload["type"]
    metadata = doc.get("metadata")
    if isinstance(metadata, dict) and metadata.get("structured_summary") is not None:
        payload["structured_summary"] = metadata["structured_summary"]
    return payload
