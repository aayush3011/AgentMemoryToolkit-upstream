from fastapi import APIRouter, Depends

from app.core.scoping import validate_scope
from app.deps import get_client
from app.schemas.lifecycle import ReconcileRequest

router = APIRouter(tags=["lifecycle"])


@router.post(
    "/users/{user_id}/reconcile",
    response_model=None,
    summary="Reconcile user memories",
)
async def reconcile_user_memories(
    user_id: str,
    body: ReconcileRequest | None = None,
    client=Depends(get_client),
) -> dict[str, int]:
    """Run reconciliation synchronously inline for Phase 1.

    Reconciliation also runs automatically on cadence in the background
    lifecycle; this endpoint exposes an explicit inline run.
    """
    validate_scope(user_id)
    n = body.n if body is not None else None
    return await client.reconcile(user_id=user_id, n=n)
