from collections.abc import Sequence
from datetime import datetime, timezone

from sqlalchemy import and_, delete, func, or_, select
from sqlalchemy.orm import Session

from decorators.database import begin_session
from models.play_session import PlaySession

from .base_handler import DBBaseHandler


class DBPlaySessionsHandler(DBBaseHandler):
    @begin_session
    def add_sessions(
        self,
        play_sessions: list[PlaySession],
        session: Session = None,  # type: ignore
    ) -> list[PlaySession]:
        session.add_all(play_sessions)
        session.flush()
        return play_sessions

    @begin_session
    def find_existing_sesssion(
        self,
        user_id: int,
        keys: list[tuple[str | None, int | None, datetime]],
        session: Session = None,  # type: ignore
    ) -> set[tuple[str | None, int | None, datetime]]:
        if not keys:
            return set()

        clauses = []
        for device_id, rom_id, start_time in keys:
            parts = [PlaySession.start_time == start_time]
            if device_id is None:
                parts.append(PlaySession.device_id.is_(None))
            else:
                parts.append(PlaySession.device_id == device_id)
            if rom_id is None:
                parts.append(PlaySession.rom_id.is_(None))
            else:
                parts.append(PlaySession.rom_id == rom_id)
            clauses.append(and_(*parts))

        stmt = select(
            PlaySession.device_id,
            PlaySession.rom_id,
            PlaySession.start_time,
        ).where(PlaySession.user_id == user_id, or_(*clauses))
        rows = session.execute(stmt).all()
        return {
            (
                r.device_id,
                r.rom_id,
                (
                    r.start_time.replace(tzinfo=timezone.utc)
                    if r.start_time.tzinfo is None
                    else r.start_time
                ),
            )
            for r in rows
        }

    @begin_session
    def get_sessions(
        self,
        user_id: int,
        rom_id: int | None = None,
        device_id: str | None = None,
        start_after: datetime | None = None,
        end_before: datetime | None = None,
        limit: int | None = 50,
        offset: int = 0,
        session: Session = None,  # type: ignore
    ) -> Sequence[PlaySession]:
        stmt = select(PlaySession).filter_by(user_id=user_id)

        if rom_id is not None:
            stmt = stmt.filter_by(rom_id=rom_id)
        if device_id is not None:
            stmt = stmt.filter_by(device_id=device_id)
        if start_after is not None:
            stmt = stmt.where(PlaySession.start_time >= start_after)
        if end_before is not None:
            stmt = stmt.where(PlaySession.start_time <= end_before)

        stmt = stmt.order_by(PlaySession.start_time.desc())
        if limit is not None:
            stmt = stmt.limit(limit)
        stmt = stmt.offset(offset)
        return session.scalars(stmt).all()

    @begin_session
    def get_total_play_time(
        self,
        user_id: int,
        rom_id: int,
        session: Session = None,  # type: ignore
    ) -> int:
        result = session.scalar(
            select(func.sum(PlaySession.duration_ms)).where(
                PlaySession.user_id == user_id,
                PlaySession.rom_id == rom_id,
            )
        )
        return result or 0

    @begin_session
    def delete_session(
        self,
        session_id: int,
        user_id: int,
        session: Session = None,  # type: ignore
    ) -> bool:
        result = session.execute(
            delete(PlaySession)
            .where(PlaySession.id == session_id, PlaySession.user_id == user_id)
            .execution_options(synchronize_session="evaluate")
        )
        return result.rowcount > 0
