from fastapi import HTTPException


def validate_scope(user_id: str, thread_id: str | None = None) -> None:
    if not user_id or not user_id.strip():
        raise HTTPException(status_code=422, detail="user_id must not be empty")
    if thread_id is not None and not thread_id.strip():
        raise HTTPException(status_code=422, detail="thread_id must not be empty")
