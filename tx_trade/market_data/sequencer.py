"""Thread-safe session-global ingest sequence allocation."""

from __future__ import annotations

from threading import Lock
from uuid import UUID

_MAX_INGEST_SEQUENCE = (1 << 63) - 1


class IngestSequencer:
    """Allocate strictly increasing signed-64-bit cursors per recording session."""

    def __init__(self) -> None:
        self._last_by_session: dict[UUID, int] = {}
        self._lock = Lock()

    @staticmethod
    def _validate_session_id(session_id: UUID) -> None:
        if type(session_id) is not UUID:
            raise TypeError("session_id must be UUID")

    def next(self, session_id: UUID) -> int:
        self._validate_session_id(session_id)
        with self._lock:
            previous = self._last_by_session.get(session_id, -1)
            if previous >= _MAX_INGEST_SEQUENCE:
                raise OverflowError("ingest_sequence exceeds signed 64-bit range")
            allocated = previous + 1
            self._last_by_session[session_id] = allocated
            return allocated

    def peek_last(self, session_id: UUID) -> int | None:
        self._validate_session_id(session_id)
        with self._lock:
            return self._last_by_session.get(session_id)
