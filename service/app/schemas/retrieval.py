from pydantic import BaseModel, Field


class SearchRequest(BaseModel):
    query: str = Field(..., min_length=1)
    memory_types: list[str] | None = None
    top_k: int = Field(default=5, ge=1, le=50)
    min_confidence: float | None = None
    min_salience: float | None = None
    tags_any: list[str] | None = None
    tags_all: list[str] | None = None
    exclude_tags: list[str] | None = None
    created_after: str | None = None
    created_before: str | None = None
    include_episodes: bool = True


class EpisodeSearchRequest(BaseModel):
    query: str = Field(..., min_length=1)
    top_k: int = Field(default=5, ge=1, le=50)
    min_salience: float | None = None
