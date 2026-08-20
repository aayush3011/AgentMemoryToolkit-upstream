from fastapi import APIRouter, Depends

from app.core.scoping import validate_scope
from app.deps import get_client
from app.schemas.ingestion import CreatedResponse, CreateMemoryRequest

router = APIRouter(tags=["ingestion"])


@router.post(
    "/users/{user_id}/threads/{thread_id}/memory",
    status_code=201,
    response_model=CreatedResponse,
    summary="Create a memory (append a conversational turn to a thread)",
)
async def create_memory(
    user_id: str,
    thread_id: str,
    body: CreateMemoryRequest,
    client=Depends(get_client),
) -> CreatedResponse:
    """Append a thread-scoped turn and return its memory id.

    This is the only memory a caller creates directly. Fact extraction, episode
    segmentation, and summaries run automatically on cadence off the ingested
    turns; they are not asserted through the API.
    """
    validate_scope(user_id, thread_id)
    memory_id = await client.upsert_memory(
        user_id=user_id,
        role=body.role,
        content=body.content,
        memory_type="turn",
        thread_id=thread_id,
        tags=body.tags,
        metadata=body.metadata,
        salience=body.salience,
        created_at=body.created_at,
    )
    return CreatedResponse(id=memory_id)
