"""Side-effect-free public contracts for deterministic market-data replay."""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID


def _require_uuid(value: object, name: str) -> None:
    if type(value) is not UUID:
        raise TypeError(f"{name} must be UUID")


def _require_nonnegative_integer(value: object, name: str) -> None:
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    if value < 0:
        raise ValueError(f"{name} must be non-negative")


def _require_nonempty_string(value: object, name: str) -> None:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    if not value.strip():
        raise ValueError(f"{name} must not be empty")


class ReplayState(StrEnum):
    """Lifecycle state of a single-use replay runtime."""

    READY = "ready"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"
    COMPLETED = "completed"
    FAILED = "failed"

    @property
    def is_terminal(self) -> bool:
        return self in {
            ReplayState.STOPPED,
            ReplayState.COMPLETED,
            ReplayState.FAILED,
        }


class ReplayMode(StrEnum):
    """How replay time is mapped to wall-clock time."""

    FASTEST = "fastest"
    PACED = "paced"


class ReplayFailureCode(StrEnum):
    """Stable, sanitized failure categories exposed by replay."""

    SESSION_NOT_FOUND = "session_not_found"
    SESSION_NOT_COMPLETE = "session_not_complete"
    SCHEMA_MISMATCH = "schema_mismatch"
    EMPTY_SESSION = "empty_session"
    INTEGRITY_FAILED = "integrity_failed"
    CURSOR_OUT_OF_RANGE = "cursor_out_of_range"
    SOURCE_FAILED = "source_failed"
    SINK_FAILED = "sink_failed"
    TIMER_FAILED = "timer_failed"
    INTERNAL_FAILED = "internal_failed"


_FAILURE_MESSAGES: dict[ReplayFailureCode, str] = {
    ReplayFailureCode.SESSION_NOT_FOUND: "replay session was not found",
    ReplayFailureCode.SESSION_NOT_COMPLETE: "replay session is not complete",
    ReplayFailureCode.SCHEMA_MISMATCH: "replay session schema is unsupported",
    ReplayFailureCode.EMPTY_SESSION: "replay session contains no events",
    ReplayFailureCode.INTEGRITY_FAILED: "replay session failed integrity validation",
    ReplayFailureCode.CURSOR_OUT_OF_RANGE: "replay cursor is outside the session",
    ReplayFailureCode.SOURCE_FAILED: "replay source failed",
    ReplayFailureCode.SINK_FAILED: "replay sink failed",
    ReplayFailureCode.TIMER_FAILED: "replay timer failed",
    ReplayFailureCode.INTERNAL_FAILED: "replay runtime failed",
}


class ReplayError(RuntimeError):
    """Replay failure whose public text never includes an underlying exception."""

    def __init__(self, code: ReplayFailureCode) -> None:
        if type(code) is not ReplayFailureCode:
            raise TypeError("code must be ReplayFailureCode")
        self.code = code
        super().__init__(_FAILURE_MESSAGES[code])


@dataclass(frozen=True, slots=True)
class ReplayOptions:
    """Playback mode, speed, and exclusive starting cursor."""

    mode: ReplayMode
    speed: float = 1.0
    after_ingest_sequence: int | None = None

    def __post_init__(self) -> None:
        if type(self.mode) is not ReplayMode:
            raise TypeError("mode must be ReplayMode")
        if isinstance(self.speed, bool) or not isinstance(self.speed, (int, float)):
            raise TypeError("speed must be a real number")
        speed = float(self.speed)
        if not math.isfinite(speed) or speed <= 0:
            raise ValueError("speed must be finite and positive")
        object.__setattr__(self, "speed", speed)
        if self.after_ingest_sequence is not None:
            _require_nonnegative_integer(self.after_ingest_sequence, "after_ingest_sequence")


@dataclass(frozen=True, slots=True)
class ReplaySessionDescriptor:
    """Validated metadata for a non-empty replay candidate."""

    session_id: UUID
    status: str
    schema_version: int
    event_count: int
    first_ingest_sequence: int
    last_ingest_sequence: int

    def __post_init__(self) -> None:
        _require_uuid(self.session_id, "session_id")
        _require_nonempty_string(self.status, "status")
        if type(self.schema_version) is not int:
            raise TypeError("schema_version must be an integer")
        if self.schema_version < 1:
            raise ValueError("schema_version must be at least 1")
        if type(self.event_count) is not int:
            raise TypeError("event_count must be an integer")
        if self.event_count < 1:
            raise ValueError("event_count must be positive")
        _require_nonnegative_integer(self.first_ingest_sequence, "first_ingest_sequence")
        _require_nonnegative_integer(self.last_ingest_sequence, "last_ingest_sequence")
        if self.first_ingest_sequence > self.last_ingest_sequence:
            raise ValueError("first_ingest_sequence must not exceed last")


@dataclass(frozen=True, slots=True)
class ReplaySnapshot:
    """Immutable observation of replay progress."""

    state: ReplayState
    session_id: UUID
    cursor: int | None
    emitted_count: int
    failure_code: ReplayFailureCode | None

    def __post_init__(self) -> None:
        if type(self.state) is not ReplayState:
            raise TypeError("state must be ReplayState")
        _require_uuid(self.session_id, "session_id")
        if self.cursor is not None:
            _require_nonnegative_integer(self.cursor, "cursor")
        _require_nonnegative_integer(self.emitted_count, "emitted_count")
        if self.failure_code is not None and type(self.failure_code) is not ReplayFailureCode:
            raise TypeError("failure_code must be ReplayFailureCode or None")
        if self.state is ReplayState.FAILED:
            if self.failure_code is None:
                raise ValueError("failed snapshots require a failure_code")
        elif self.failure_code is not None:
            raise ValueError("only failed snapshots may have a failure_code")
