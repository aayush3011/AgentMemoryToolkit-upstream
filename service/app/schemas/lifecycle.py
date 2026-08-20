from pydantic import BaseModel, Field


class ReconcileRequest(BaseModel):
    n: int | None = Field(default=None, ge=1)
