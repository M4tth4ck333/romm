from datetime import datetime
from typing import Literal

from .base import BaseModel


class PlaySessionSchema(BaseModel):
    id: int
    user_id: int
    device_id: str | None
    rom_id: int | None
    save_slot: str | None
    start_time: datetime
    end_time: datetime
    duration_ms: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class PlaySessionIngestResult(BaseModel):
    index: int
    status: Literal["created", "duplicate", "error"]
    id: int | None = None
    detail: str | None = None


class PlaySessionIngestResponse(BaseModel):
    results: list[PlaySessionIngestResult]
    created_count: int
    skipped_count: int
