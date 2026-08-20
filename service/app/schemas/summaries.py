from pydantic import BaseModel, Field


class GenerateThreadSummaryRequest(BaseModel):
    """Optional controls for generating a thread (session) summary."""

    recent_k: int | None = Field(default=None, ge=1)


class GenerateUserSummaryRequest(BaseModel):
    """Optional controls for generating the cross-thread user profile."""

    thread_ids: list[str] | None = None
    recent_k: int | None = Field(default=None, ge=1)
