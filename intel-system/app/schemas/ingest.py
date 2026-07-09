from datetime import datetime
from typing import Any

from pydantic import BaseModel


class IngestItem(BaseModel):
    external_id: str | None = None
    occurred_at: datetime
    url: str | None = None
    text: str | None = None
    payload: dict[str, Any] = {}


class IngestRequest(BaseModel):
    source_identifier: str
    items: list[IngestItem]


class IngestResponse(BaseModel):
    accepted: int
    duplicates: int
