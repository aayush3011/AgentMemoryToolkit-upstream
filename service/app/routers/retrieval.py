from fastapi import APIRouter, Depends, Query

from app.core.scoping import validate_scope
from app.core.serialization import list_envelope
from app.deps import get_client
from app.schemas.retrieval import EpisodeSearchRequest, SearchRequest

router = APIRouter(tags=["retrieval"])


@router.get(
    "/users/{user_id}/memories",
    summary="List / filter a user's memories (no search query needed)",
)
async def list_memories(
    user_id: str,
    thread_id: str | None = Query(default=None, description="Scope to one thread; omit for all threads."),
    memory_types: list[str] | None = Query(
        default=None, description="Filter by type, e.g. fact, episodic, thread_summary, user_summary."
    ),
    recent_k: int | None = Query(default=None, ge=1, description="Return only the most recent K."),
    tags_any: list[str] | None = Query(default=None),
    tags_all: list[str] | None = Query(default=None),
    exclude_tags: list[str] | None = Query(default=None),
    include_superseded: bool = Query(default=False),
    min_salience: float | None = Query(default=None, ge=0.0, le=1.0),
    min_confidence: float | None = Query(default=None, ge=0.0, le=1.0),
    created_after: str | None = Query(default=None),
    created_before: str | None = Query(default=None),
    client=Depends(get_client),
) -> dict:
    """List memories for a user with optional filters, no vector search.

    ``thread_id`` is optional; omit it to list across all of the user's threads.
    """
    validate_scope(user_id, thread_id)
    results = await client.get_memories(
        user_id=user_id,
        thread_id=thread_id,
        memory_types=memory_types,
        recent_k=recent_k,
        tags_any=tags_any,
        tags_all=tags_all,
        exclude_tags=exclude_tags,
        include_superseded=include_superseded,
        min_salience=min_salience,
        min_confidence=min_confidence,
        created_after=created_after,
        created_before=created_before,
    )
    return list_envelope(results, top_k=recent_k)


@router.get(
    "/users/{user_id}/episodes",
    summary="List a user's episodes, newest first (no search query needed)",
)
async def list_episodes(
    user_id: str,
    thread_id: str | None = Query(default=None, description="Scope to one thread; omit for all threads."),
    recent_k: int | None = Query(default=None, ge=1, description="Return only the most recent K."),
    client=Depends(get_client),
) -> dict:
    """List a user's episodic memories, newest first, without a search query."""
    validate_scope(user_id, thread_id)
    results = await client.get_episodes(user_id=user_id, thread_id=thread_id, recent_k=recent_k)
    return list_envelope(results, top_k=recent_k)


@router.post("/users/{user_id}/search", summary="Search user memories")
async def search_memories(
    user_id: str,
    body: SearchRequest,
    client=Depends(get_client),
) -> dict:
    """Run primary hybrid recall over user memories."""
    validate_scope(user_id)
    results = await client.search_cosmos(
        search_terms=body.query,
        user_id=user_id,
        memory_types=body.memory_types,
        top_k=body.top_k,
        min_confidence=body.min_confidence,
        min_salience=body.min_salience,
        tags_any=body.tags_any,
        tags_all=body.tags_all,
        exclude_tags=body.exclude_tags,
        created_after=body.created_after,
        created_before=body.created_before,
        include_episodes=body.include_episodes,
    )
    return list_envelope(results, top_k=body.top_k)


@router.post("/users/{user_id}/search/episodes", summary="Search user episodic memories")
async def search_episodes(
    user_id: str,
    body: EpisodeSearchRequest,
    client=Depends(get_client),
) -> dict:
    """Run episode-only recall over user memories."""
    validate_scope(user_id)
    results = await client.search_episodic_memories(
        user_id=user_id,
        search_terms=body.query,
        top_k=body.top_k,
        min_salience=body.min_salience,
    )
    return list_envelope(results, top_k=body.top_k)
