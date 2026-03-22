from datetime import datetime, timedelta, timezone

from fastapi import HTTPException, Request, status
from pydantic import BaseModel, Field, model_validator
from sqlalchemy.exc import IntegrityError

from decorators.auth import protected_route
from endpoints.responses.play_session import (
    PlaySessionIngestResponse,
    PlaySessionIngestResult,
    PlaySessionSchema,
)
from handler.auth.constants import Scope
from handler.database import db_device_handler, db_play_session_handler, db_rom_handler
from logger.logger import log
from models.play_session import PlaySession
from models.rom import Rom
from utils.datetime import to_utc
from utils.router import APIRouter

router = APIRouter(
    prefix="/play-sessions",
    tags=["play-sessions"],
)

MAX_BATCH_SIZE = 100
MAX_FUTURE_TOLERANCE = timedelta(minutes=5)


class PlaySessionEntry(BaseModel):
    rom_id: int | None = None
    save_slot: str | None = None
    start_time: datetime
    end_time: datetime
    duration_ms: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_times(self) -> "PlaySessionEntry":
        self.start_time = self.start_time.replace(microsecond=0)
        self.end_time = self.end_time.replace(microsecond=0)
        if self.end_time <= self.start_time:
            raise ValueError("end_time must be after start_time")
        return self


class PlaySessionIngestPayload(BaseModel):
    device_id: str | None = None
    sessions: list[PlaySessionEntry]


@protected_route(
    router.post, "", [Scope.ROMS_USER_WRITE], status_code=status.HTTP_201_CREATED
)
def ingest_play_sessions(
    request: Request,
    payload: PlaySessionIngestPayload,
) -> PlaySessionIngestResponse:
    if not payload.sessions:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Payload must contain at least one session",
        )
    if len(payload.sessions) > MAX_BATCH_SIZE:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Batch size exceeds maximum of {MAX_BATCH_SIZE}",
        )

    now = datetime.now(timezone.utc)
    user_id = request.user.id

    resolved_device_id = None
    if payload.device_id is not None:
        device = db_device_handler.get_device(
            device_id=payload.device_id, user_id=user_id
        )
        if device is not None:
            resolved_device_id = payload.device_id

    rom_cache: dict[int, Rom | None] = {}
    results: list[PlaySessionIngestResult] = []
    created_count = 0
    skipped_count = 0
    rom_user_updates: dict[int, datetime] = {}

    resolved: list[tuple[int, int | None]] = []
    candidate_keys: list[tuple[str | None, int | None, datetime]] = []
    skipped_indices: set[int] = set()

    for idx, item in enumerate(payload.sessions):
        if item.end_time > now + MAX_FUTURE_TOLERANCE:
            results.append(
                PlaySessionIngestResult(
                    index=idx,
                    status="error",
                    detail="end_time is too far in the future",
                )
            )
            skipped_count += 1
            skipped_indices.add(idx)
            resolved.append((idx, None))
            candidate_keys.append((None, None, item.start_time))
            continue

        resolved_rom_id = None
        if item.rom_id is not None:
            if item.rom_id not in rom_cache:
                rom_cache[item.rom_id] = db_rom_handler.get_rom(id=item.rom_id)
            rom = rom_cache[item.rom_id]
            if rom is not None:
                resolved_rom_id = item.rom_id

        resolved.append((idx, resolved_rom_id))
        candidate_keys.append(
            (resolved_device_id, resolved_rom_id, to_utc(item.start_time))
        )

    existing_keys = db_play_session_handler.find_existing(
        user_id=user_id,
        keys=[k for i, k in enumerate(candidate_keys) if i not in skipped_indices],
    )

    seen_keys: set[tuple[str | None, int | None, datetime]] = set()

    for (idx, resolved_rom_id), item in zip(resolved, payload.sessions, strict=False):
        if idx in skipped_indices:
            continue

        dedup_key = (resolved_device_id, resolved_rom_id, to_utc(item.start_time))
        if dedup_key in seen_keys or dedup_key in existing_keys:
            results.append(
                PlaySessionIngestResult(
                    index=idx,
                    status="duplicate",
                )
            )
            skipped_count += 1
            continue
        seen_keys.add(dedup_key)

        play_session = PlaySession(
            user_id=user_id,
            device_id=resolved_device_id,
            rom_id=resolved_rom_id,
            save_slot=item.save_slot,
            start_time=item.start_time,
            end_time=item.end_time,
            duration_ms=item.duration_ms,
        )

        try:
            created = db_play_session_handler.add_session(play_session)
            results.append(
                PlaySessionIngestResult(
                    index=idx,
                    status="created",
                    id=created.id,
                )
            )
            created_count += 1

            if resolved_rom_id is not None:
                existing = rom_user_updates.get(resolved_rom_id)
                if existing is None or item.end_time > existing:
                    rom_user_updates[resolved_rom_id] = item.end_time

        except IntegrityError:
            results.append(
                PlaySessionIngestResult(
                    index=idx,
                    status="duplicate",
                )
            )
            skipped_count += 1

    for rom_id, latest_end_time in rom_user_updates.items():
        rom_user = db_rom_handler.get_rom_user(rom_id=rom_id, user_id=user_id)
        if not rom_user:
            rom_user = db_rom_handler.add_rom_user(rom_id=rom_id, user_id=user_id)

        update_data: dict = {}
        current_last_played = (
            to_utc(rom_user.last_played) if rom_user.last_played else None
        )
        if current_last_played is None or latest_end_time > current_last_played:
            update_data["last_played"] = latest_end_time
        if update_data:
            db_rom_handler.update_rom_user(rom_user.id, update_data)

    if resolved_device_id is not None:
        db_device_handler.update_last_seen(
            device_id=resolved_device_id, user_id=user_id
        )

    log.info(
        f"Ingested {created_count} play sessions for user {request.user.username}"
        f" ({skipped_count} skipped)"
    )

    return PlaySessionIngestResponse(
        results=results,
        created_count=created_count,
        skipped_count=skipped_count,
    )


@protected_route(router.get, "", [Scope.ROMS_USER_READ])
def get_play_sessions(
    request: Request,
    rom_id: int | None = None,
    device_id: str | None = None,
    start_after: datetime | None = None,
    end_before: datetime | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[PlaySessionSchema]:
    sessions = db_play_session_handler.get_sessions(
        user_id=request.user.id,
        rom_id=rom_id,
        device_id=device_id,
        start_after=start_after,
        end_before=end_before,
        limit=limit if start_after is None and end_before is None else None,
        offset=offset,
    )
    return [PlaySessionSchema.model_validate(s) for s in sessions]


@protected_route(
    router.delete,
    "/{session_id}",
    [Scope.ROMS_USER_WRITE],
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_play_session(request: Request, session_id: int) -> None:
    deleted = db_play_session_handler.delete_session(
        session_id=session_id, user_id=request.user.id
    )
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Play session {session_id} not found",
        )
    log.info(f"Deleted play session {session_id} for user {request.user.username}")
