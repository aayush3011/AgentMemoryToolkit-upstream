from fastapi import APIRouter, Depends

from app.core.scoping import validate_scope
from app.core.serialization import serialize_summary
from app.deps import get_client
from app.schemas.summaries import GenerateThreadSummaryRequest, GenerateUserSummaryRequest
from azure.cosmos.agent_memory.exceptions import MemoryNotFoundError

router = APIRouter(tags=["summaries"])


@router.post(
    "/users/{user_id}/threads/{thread_id}/summary",
    status_code=201,
    summary="Generate (or refresh) a thread summary from the session's turns",
)
async def generate_thread_summary(
    user_id: str,
    thread_id: str,
    body: GenerateThreadSummaryRequest | None = None,
    client=Depends(get_client),
) -> dict:
    """Build a per-session recap from the raw turns of a thread."""
    validate_scope(user_id, thread_id)
    recent_k = body.recent_k if body is not None else None
    doc = await client.generate_thread_summary(user_id=user_id, thread_id=thread_id, recent_k=recent_k)
    return serialize_summary(doc)


@router.get(
    "/users/{user_id}/threads/{thread_id}/summary",
    summary="Get the active thread summary (newest first)",
)
async def get_thread_summary(
    user_id: str,
    thread_id: str,
    client=Depends(get_client),
) -> dict:
    """Retrieve active thread summaries for a session, newest first."""
    validate_scope(user_id, thread_id)
    docs = await client.get_thread_summary(user_id=user_id, thread_id=thread_id)
    items = [serialize_summary(d) for d in docs]
    return {"items": items, "count": len(items)}


@router.post(
    "/users/{user_id}/summary",
    status_code=201,
    summary="Generate (or refresh) the cross-thread user profile",
)
async def generate_user_summary(
    user_id: str,
    body: GenerateUserSummaryRequest | None = None,
    client=Depends(get_client),
) -> dict:
    """Roll up the user's facts + thread summaries into a holistic profile."""
    validate_scope(user_id)
    thread_ids = body.thread_ids if body is not None else None
    recent_k = body.recent_k if body is not None else None
    doc = await client.generate_user_summary(user_id=user_id, thread_ids=thread_ids, recent_k=recent_k)
    return serialize_summary(doc)


@router.get(
    "/users/{user_id}/summary",
    summary="Get the cross-thread user profile",
)
async def get_user_summary(
    user_id: str,
    client=Depends(get_client),
) -> dict:
    """Retrieve the user's holistic profile, or 404 when none exists yet."""
    validate_scope(user_id)
    doc = await client.get_user_summary(user_id=user_id)
    if not doc:
        raise MemoryNotFoundError(f"No user summary for user_id={user_id!r}")
    return serialize_summary(doc)
