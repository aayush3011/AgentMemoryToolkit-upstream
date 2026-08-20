from typing import Any

from pydantic import BaseModel


class CreatedResponse(BaseModel):
    id: str


class CreateMemoryRequest(BaseModel):
    """A single conversational turn appended to a thread.

    Turns are the only thing callers create directly; facts, episodes, and
    summaries are derived by the background lifecycle on cadence.
    """

    role: str
    content: str
    tags: list[str] | None = None
    metadata: dict[str, Any] | None = None
    salience: float | None = None
    created_at: str | None = None
