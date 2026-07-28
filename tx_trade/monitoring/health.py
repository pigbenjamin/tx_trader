"""Bounded, thread-safe operational health state."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from threading import Lock
from typing import Protocol
from uuid import UUID


class Clock(Protocol):
    def now(self) -> datetime: ...


def _positive(value: int, name: str) -> None:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive integer")


def _reason(value: str) -> None:
    if type(value) is not str or not value.strip():
        raise ValueError("reason must be a non-empty string")


def _taipei_datetime(value: object) -> None:
    if type(value) is not datetime:
        raise TypeError("clock.now() must return a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("clock.now() must be timezone-aware")
    if getattr(value.tzinfo, "key", None) != "Asia/Taipei":
        raise ValueError("clock.now() must use Asia/Taipei timezone")


class HealthState(StrEnum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class PipelineHealthSnapshot:
    state: HealthState
    reasons: tuple[str, ...]
    observed_at: datetime


class PipelineHealth:
    """Monotonic health state with a bounded set of non-sensitive reasons."""

    def __init__(self, clock: Clock, max_reasons: int = 32) -> None:
        _positive(max_reasons, "max_reasons")
        if not hasattr(clock, "now"):
            raise TypeError("clock must provide now()")
        self._clock = clock
        self._max_reasons = max_reasons
        self._state = HealthState.HEALTHY
        self._reasons: list[str] = []
        self._reason_set: set[str] = set()
        self._lock = Lock()

    def degrade(self, reason: str) -> None:
        self._change(HealthState.DEGRADED, reason)

    def fail(self, reason: str) -> None:
        self._change(HealthState.FAILED, reason)

    def _change(self, state: HealthState, reason: str) -> None:
        _reason(reason)
        with self._lock:
            if state is HealthState.FAILED or self._state is HealthState.HEALTHY:
                self._state = state
            self._add_reason(reason)

    def _add_reason(self, reason: str) -> None:
        if reason in self._reason_set:
            return
        if len(self._reasons) < self._max_reasons:
            self._reasons.append(reason)
            self._reason_set.add(reason)
            return
        omitted = "additional_reasons_omitted"
        if omitted not in self._reason_set:
            # Keep the total number of stored reasons bounded by max_reasons.
            self._reason_set.remove(self._reasons[-1])
            self._reasons[-1] = omitted
            self._reason_set.add(omitted)

    def snapshot(self) -> PipelineHealthSnapshot:
        with self._lock:
            state, reasons = self._state, tuple(self._reasons)
        observed_at = self._clock.now()
        _taipei_datetime(observed_at)
        return PipelineHealthSnapshot(state, reasons, observed_at)


@dataclass(frozen=True, slots=True)
class SessionImpactSnapshot:
    session_id: UUID
    is_incomplete: bool
    reasons: tuple[str, ...]
    dropped_tick_count: int
    first_dropped_tick_sequence: int | None
    last_dropped_tick_sequence: int | None


@dataclass(slots=True)
class _SessionImpact:
    reasons: list[str]
    reason_set: set[str]
    dropped_tick_count: int = 0
    first_dropped_tick_sequence: int | None = None
    last_dropped_tick_sequence: int | None = None


class SessionImpactTracker:
    """Tracks bounded per-session recording damage until a session is finalized."""

    def __init__(self, max_sessions: int, max_reasons: int = 32) -> None:
        _positive(max_sessions, "max_sessions")
        _positive(max_reasons, "max_reasons")
        self._max_sessions = max_sessions
        self._max_reasons = max_reasons
        self._sessions: OrderedDict[UUID, _SessionImpact] = OrderedDict()
        self._capacity_exhausted = False
        self._lock = Lock()

    def _get_or_create(self, session_id: UUID) -> _SessionImpact:
        if type(session_id) is not UUID:
            raise TypeError("session_id must be UUID")
        impact = self._sessions.get(session_id)
        if impact is None:
            if len(self._sessions) >= self._max_sessions:
                self._capacity_exhausted = True
                raise RuntimeError("session impact capacity exhausted")
            impact = _SessionImpact([], set())
            self._sessions[session_id] = impact
        return impact

    def mark_incomplete(self, session_id: UUID, reason: str) -> None:
        _reason(reason)
        with self._lock:
            impact = self._get_or_create(session_id)
            self._add_reason(impact, reason)

    def record_dropped_tick(self, session_id: UUID, sequence: int) -> None:
        if type(sequence) is not int or sequence < 0:
            raise ValueError("sequence must be a non-negative integer")
        with self._lock:
            impact = self._get_or_create(session_id)
            impact.dropped_tick_count += 1
            if impact.first_dropped_tick_sequence is None:
                impact.first_dropped_tick_sequence = sequence
            impact.last_dropped_tick_sequence = sequence

    def _add_reason(self, impact: _SessionImpact, reason: str) -> None:
        if reason in impact.reason_set:
            return
        if len(impact.reasons) < self._max_reasons:
            impact.reasons.append(reason)
            impact.reason_set.add(reason)
        elif "additional_reasons_omitted" not in impact.reason_set:
            impact.reason_set.remove(impact.reasons[-1])
            impact.reasons[-1] = "additional_reasons_omitted"
            impact.reason_set.add("additional_reasons_omitted")

    def effective_terminal_status(self, session_id: UUID, requested: str) -> str:
        if type(requested) is not str or not requested:
            raise ValueError("requested must be a non-empty string")
        with self._lock:
            return (
                "incomplete"
                if self._capacity_exhausted or session_id in self._sessions
                else requested
            )

    def snapshot(self, session_id: UUID) -> SessionImpactSnapshot:
        if type(session_id) is not UUID:
            raise TypeError("session_id must be UUID")
        with self._lock:
            impact = self._sessions.get(session_id)
            if impact is None:
                reasons = ("session_impact_capacity_exhausted",) if self._capacity_exhausted else ()
                return SessionImpactSnapshot(
                    session_id, self._capacity_exhausted, reasons, 0, None, None
                )
            return SessionImpactSnapshot(
                session_id,
                True,
                tuple(impact.reasons),
                impact.dropped_tick_count,
                impact.first_dropped_tick_sequence,
                impact.last_dropped_tick_sequence,
            )

    def clear(self, session_id: UUID) -> None:
        if type(session_id) is not UUID:
            raise TypeError("session_id must be UUID")
        with self._lock:
            self._sessions.pop(session_id, None)

    @property
    def tracked_session_count(self) -> int:
        with self._lock:
            return len(self._sessions)

    @property
    def capacity_exhausted(self) -> bool:
        with self._lock:
            return self._capacity_exhausted


@dataclass(frozen=True, slots=True)
class ControlledShutdownSnapshot:
    is_requested: bool
    reason: str | None
    request_count: int


class ControlledShutdown:
    """Idempotent cross-thread shutdown request retaining only its first reason."""

    def __init__(self) -> None:
        self._reason: str | None = None
        self._request_count = 0
        self._lock = Lock()

    def request_shutdown(self, reason: str) -> bool:
        _reason(reason)
        with self._lock:
            if self._reason is not None:
                return False
            self._reason = reason
            self._request_count = 1
            return True

    def snapshot(self) -> ControlledShutdownSnapshot:
        with self._lock:
            return ControlledShutdownSnapshot(
                self._reason is not None, self._reason, self._request_count
            )
