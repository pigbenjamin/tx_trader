from datetime import datetime
from uuid import UUID

import pytest

from tx_trade.market_data.models import TAIPEI
from tx_trade.monitoring.health import (
    ControlledShutdown,
    HealthState,
    PipelineHealth,
    SessionImpactTracker,
)

NOW = datetime(2026, 7, 26, 9, 30, tzinfo=TAIPEI)
SESSION = UUID("12345678-1234-5678-1234-567812345678")


class Clock:
    def now(self):
        return NOW


def test_health_is_monotonic_and_reasons_are_bounded():
    health = PipelineHealth(Clock(), max_reasons=2)
    health.degrade("tick_ingress_overflow")
    health.fail("control_ingress_overflow")
    health.degrade("ignored_state_downgrade")
    snapshot = health.snapshot()
    assert snapshot.state is HealthState.FAILED
    assert snapshot.reasons == (
        "tick_ingress_overflow",
        "additional_reasons_omitted",
    )
    assert snapshot.observed_at == NOW


def test_session_impact_counts_ticks_and_is_cleared_at_finalize():
    tracker = SessionImpactTracker(max_sessions=1, max_reasons=2)
    tracker.mark_incomplete(SESSION, "tick_ingress_overflow")
    tracker.record_dropped_tick(SESSION, 12)
    tracker.record_dropped_tick(SESSION, 19)
    snapshot = tracker.snapshot(SESSION)
    assert snapshot.is_incomplete
    assert snapshot.dropped_tick_count == 2
    assert snapshot.first_dropped_tick_sequence == 12
    assert snapshot.last_dropped_tick_sequence == 19
    assert tracker.effective_terminal_status(SESSION, "complete") == "incomplete"
    with pytest.raises(RuntimeError, match="capacity"):
        tracker.mark_incomplete(UUID(int=2), "tick_ingress_overflow")
    assert tracker.capacity_exhausted
    tracker.clear(SESSION)
    assert tracker.effective_terminal_status(SESSION, "complete") == "incomplete"
    assert tracker.snapshot(UUID(int=2)).is_incomplete
    assert tracker.tracked_session_count == 0


def test_controlled_shutdown_retains_only_first_request():
    shutdown = ControlledShutdown()
    assert shutdown.request_shutdown("first") is True
    assert shutdown.request_shutdown("second") is False
    assert shutdown.snapshot().reason == "first"
    assert shutdown.snapshot().request_count == 1


def test_health_snapshot_rejects_naive_clock_time():
    class NaiveClock:
        def now(self):
            return datetime(2026, 7, 26, 9, 30)

    with pytest.raises(ValueError, match="timezone-aware"):
        PipelineHealth(NaiveClock()).snapshot()
