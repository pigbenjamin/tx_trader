from __future__ import annotations

import math
from dataclasses import FrozenInstanceError
from threading import Event
from uuid import UUID

import pytest

from tx_trade.replay import (
    ReplayError,
    ReplayFailureCode,
    ReplayMode,
    ReplayOptions,
    ReplaySessionDescriptor,
    ReplaySnapshot,
    ReplayState,
    ReplayTimer,
    SystemReplayTimer,
)

SESSION_ID = UUID("3b5789ee-6015-49ca-beb9-63a4f2437d15")


def test_replay_states_expose_expected_terminal_semantics() -> None:
    assert {state.value for state in ReplayState} == {
        "ready",
        "running",
        "paused",
        "stopped",
        "completed",
        "failed",
    }
    assert ReplayState.STOPPED.is_terminal
    assert ReplayState.COMPLETED.is_terminal
    assert ReplayState.FAILED.is_terminal
    assert not ReplayState.READY.is_terminal
    assert not ReplayState.RUNNING.is_terminal
    assert not ReplayState.PAUSED.is_terminal


def test_replay_options_are_immutable_and_normalize_speed() -> None:
    options = ReplayOptions(
        mode=ReplayMode.PACED,
        speed=2,
        after_ingest_sequence=17,
    )

    assert options.speed == 2.0
    assert type(options.speed) is float
    with pytest.raises(FrozenInstanceError):
        options.speed = 3.0  # type: ignore[misc]


@pytest.mark.parametrize("mode", ["paced", None, True])
def test_replay_options_reject_non_enum_mode(mode: object) -> None:
    with pytest.raises(TypeError, match="mode must be ReplayMode"):
        ReplayOptions(mode=mode)  # type: ignore[arg-type]


@pytest.mark.parametrize("speed", [True, False, "2", None])
def test_replay_options_reject_non_numeric_or_boolean_speed(speed: object) -> None:
    with pytest.raises(TypeError, match="speed must be a real number"):
        ReplayOptions(mode=ReplayMode.FASTEST, speed=speed)  # type: ignore[arg-type]


@pytest.mark.parametrize("speed", [0, -1, math.nan, math.inf, -math.inf])
def test_replay_options_reject_non_positive_or_non_finite_speed(speed: float) -> None:
    with pytest.raises(ValueError, match="speed must be finite and positive"):
        ReplayOptions(mode=ReplayMode.PACED, speed=speed)


@pytest.mark.parametrize("cursor", [True, 1.0, "1"])
def test_replay_options_reject_non_integer_cursor(cursor: object) -> None:
    with pytest.raises(TypeError, match="after_ingest_sequence must be an integer"):
        ReplayOptions(
            mode=ReplayMode.FASTEST,
            after_ingest_sequence=cursor,  # type: ignore[arg-type]
        )


def test_replay_options_reject_negative_cursor() -> None:
    with pytest.raises(ValueError, match="after_ingest_sequence must be non-negative"):
        ReplayOptions(mode=ReplayMode.FASTEST, after_ingest_sequence=-1)


def test_session_descriptor_accepts_sequence_gaps() -> None:
    descriptor = ReplaySessionDescriptor(
        session_id=SESSION_ID,
        status="complete",
        schema_version=1,
        event_count=2,
        first_ingest_sequence=4,
        last_ingest_sequence=100,
    )

    assert descriptor.first_ingest_sequence == 4
    assert descriptor.last_ingest_sequence == 100


@pytest.mark.parametrize(
    ("field", "value", "exception"),
    [
        ("session_id", "not-a-uuid", TypeError),
        ("status", "", ValueError),
        ("status", 1, TypeError),
        ("schema_version", True, TypeError),
        ("schema_version", 0, ValueError),
        ("event_count", True, TypeError),
        ("event_count", 0, ValueError),
        ("first_ingest_sequence", True, TypeError),
        ("first_ingest_sequence", -1, ValueError),
        ("last_ingest_sequence", 1.0, TypeError),
        ("last_ingest_sequence", -1, ValueError),
    ],
)
def test_session_descriptor_rejects_invalid_fields(
    field: str, value: object, exception: type[Exception]
) -> None:
    values: dict[str, object] = {
        "session_id": SESSION_ID,
        "status": "complete",
        "schema_version": 1,
        "event_count": 2,
        "first_ingest_sequence": 4,
        "last_ingest_sequence": 9,
    }
    values[field] = value

    with pytest.raises(exception):
        ReplaySessionDescriptor(**values)  # type: ignore[arg-type]


def test_session_descriptor_rejects_reversed_bounds() -> None:
    with pytest.raises(ValueError, match="first_ingest_sequence must not exceed last"):
        ReplaySessionDescriptor(
            session_id=SESSION_ID,
            status="complete",
            schema_version=1,
            event_count=1,
            first_ingest_sequence=8,
            last_ingest_sequence=7,
        )


def test_snapshot_enforces_failure_code_state_invariant() -> None:
    failed = ReplaySnapshot(
        state=ReplayState.FAILED,
        session_id=SESSION_ID,
        cursor=5,
        emitted_count=3,
        failure_code=ReplayFailureCode.SINK_FAILED,
    )

    assert failed.failure_code is ReplayFailureCode.SINK_FAILED
    with pytest.raises(ValueError, match="failed snapshots require a failure_code"):
        ReplaySnapshot(
            state=ReplayState.FAILED,
            session_id=SESSION_ID,
            cursor=None,
            emitted_count=0,
            failure_code=None,
        )
    with pytest.raises(ValueError, match="only failed snapshots"):
        ReplaySnapshot(
            state=ReplayState.RUNNING,
            session_id=SESSION_ID,
            cursor=None,
            emitted_count=0,
            failure_code=ReplayFailureCode.INTERNAL_FAILED,
        )


def test_replay_error_exposes_only_fixed_message() -> None:
    secret = "credential-canary"
    error = ReplayError(ReplayFailureCode.SOURCE_FAILED)

    assert error.code is ReplayFailureCode.SOURCE_FAILED
    assert str(error) == "replay source failed"
    assert secret not in repr(error)
    with pytest.raises(TypeError, match="code must be ReplayFailureCode"):
        ReplayError("source_failed")  # type: ignore[arg-type]


def test_system_timer_implements_protocol_and_interrupts_wait() -> None:
    timer = SystemReplayTimer()
    wake_event = Event()
    wake_event.set()

    assert isinstance(timer, ReplayTimer)
    assert timer.monotonic() >= 0.0
    assert timer.wait(60.0, wake_event) is True


def test_system_timer_reports_completed_zero_delay_wait() -> None:
    assert SystemReplayTimer().wait(0.0, Event()) is False


@pytest.mark.parametrize("delay", [True, "1", None])
def test_system_timer_rejects_non_numeric_or_boolean_delay(delay: object) -> None:
    with pytest.raises(TypeError, match="delay_seconds must be a real number"):
        SystemReplayTimer().wait(delay, Event())  # type: ignore[arg-type]


@pytest.mark.parametrize("delay", [-1, math.nan, math.inf])
def test_system_timer_rejects_invalid_delay(delay: float) -> None:
    with pytest.raises(ValueError, match="finite and non-negative"):
        SystemReplayTimer().wait(delay, Event())


def test_system_timer_rejects_non_event_wake_signal() -> None:
    with pytest.raises(TypeError, match="wake_event must be threading.Event"):
        SystemReplayTimer().wait(0.0, object())  # type: ignore[arg-type]
