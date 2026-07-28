from dataclasses import replace
from datetime import datetime
from threading import Event
from uuid import UUID

import pytest

from tx_trade.market_data.ingress import (
    BoundedIngress,
    BoundedStaQuoteQueue,
    StaIngressDecision,
)
from tx_trade.market_data.models import (
    CapturedAdapterDiagnostic,
    CapturedConnectionNotification,
    CapturedKind,
    CapturedMarketDataEvent,
    CapturedQuoteSnapshot,
    CapturedTickNotification,
    SourceMode,
    StaLocalQuoteNotification,
    TAIPEI,
)
from tx_trade.market_data.ports import IngressDecision
from tx_trade.monitoring.health import (
    ControlledShutdown,
    HealthState,
    PipelineHealth,
    SessionImpactTracker,
)
from tx_trade.monitoring.metrics import IngressLane, IngressMetrics

NOW = datetime(2026, 7, 26, 9, 30, tzinfo=TAIPEI)
SESSION = UUID("12345678-1234-5678-1234-567812345678")


class Clock:
    def now(self):
        return NOW


def captured(kind, sequence, *, session=SESSION, generation=1, symbol=1, dedupe=None):
    if kind is CapturedKind.QUOTE_SNAPSHOT:
        payload = CapturedQuoteSnapshot(0, symbol, 1, 2, 1, 1, 1, 1, False, sequence, NOW)
    elif kind is CapturedKind.TICK_NOTIFICATION:
        payload = CapturedTickNotification(
            0, symbol, 0, 20260726, 93000, 0, 1, 2, 1, 1, 0, False, sequence, NOW
        )
    elif kind is CapturedKind.ADAPTER_DIAGNOSTIC:
        payload = CapturedAdapterDiagnostic(
            "adapter_error", None, None, 1, "safe", NOW, 1, generation, sequence, {}
        )
    else:
        payload = CapturedConnectionNotification(3003, 0, sequence, NOW)
    return CapturedMarketDataEvent(
        kind,
        payload,
        None,
        "fixture",
        SourceMode.OFFLINE,
        session,
        generation,
        sequence,
        None,
        NOW,
        None if kind is CapturedKind.ADAPTER_DIAGNOSTIC else NOW,
        None,
        None,
        dedupe,
    )


def components(**capacities):
    health = PipelineHealth(Clock())
    metrics = IngressMetrics()
    impact = SessionImpactTracker(max_sessions=capacities.get("sessions", 4))
    shutdown = ControlledShutdown()
    ingress = BoundedIngress(
        control_capacity=capacities.get("control", 1),
        diagnostic_capacity=capacities.get("diagnostic", 1),
        quote_capacity=capacities.get("quote", 1),
        tick_capacity=capacities.get("tick", 1),
        dedupe_capacity=capacities.get("dedupe", 2),
        health=health,
        metrics=metrics,
        session_impact=impact,
        shutdown=shutdown,
        priority_burst_limit=capacities.get("burst", 8),
    )
    return ingress, health, metrics, impact, shutdown


def test_reserved_lanes_and_overflow_policies():
    ingress, health, metrics, impact, shutdown = components()
    assert (
        ingress.try_publish(captured(CapturedKind.TICK_NOTIFICATION, 1)) is IngressDecision.ACCEPTED
    )
    assert ingress.try_publish(captured(CapturedKind.QUOTE_SNAPSHOT, 2)) is IngressDecision.ACCEPTED
    assert (
        ingress.try_publish(captured(CapturedKind.CONNECTION_NOTIFICATION, 3))
        is IngressDecision.ACCEPTED
    )
    assert (
        ingress.try_publish(captured(CapturedKind.ADAPTER_DIAGNOSTIC, 4))
        is IngressDecision.ACCEPTED
    )
    assert ingress.snapshot().total_depth == 4
    assert (
        ingress.try_publish(captured(CapturedKind.ADAPTER_DIAGNOSTIC, 5)) is IngressDecision.DROPPED
    )
    assert (
        ingress.try_publish(captured(CapturedKind.ADAPTER_DIAGNOSTIC, 6)) is IngressDecision.DROPPED
    )
    assert health.snapshot().state is HealthState.FAILED
    assert impact.effective_terminal_status(SESSION, "complete") == "incomplete"
    assert shutdown.snapshot().request_count == 1
    assert metrics.snapshot().overflow[IngressLane.DIAGNOSTIC] == 2


def test_quote_coalescing_stale_and_generation_isolation():
    ingress, *_ = components(quote=2)
    first = captured(CapturedKind.QUOTE_SNAPSHOT, 10, dedupe="a")
    assert ingress.try_publish(first) is IngressDecision.ACCEPTED
    assert (
        ingress.try_publish(
            replace(
                first,
                sequence=11,
                payload=replace(first.payload, callback_sequence=11),
                dedupe_candidate="b",
            )
        )
        is IngressDecision.COALESCED
    )
    assert (
        ingress.try_publish(
            replace(
                first,
                sequence=9,
                payload=replace(first.payload, callback_sequence=9),
                dedupe_candidate="c",
            )
        )
        is IngressDecision.DUPLICATE
    )
    assert (
        ingress.try_publish(captured(CapturedKind.QUOTE_SNAPSHOT, 1, generation=2, dedupe="a"))
        is IngressDecision.ACCEPTED
    )
    assert (
        ingress.try_publish(captured(CapturedKind.QUOTE_SNAPSHOT, 12, symbol=2))
        is IngressDecision.DROPPED
    )
    assert ingress.try_pop().sequence == 11
    assert ingress.try_pop().connection_generation == 2


def test_tick_drops_and_recent_dedupe_are_exact_and_bounded():
    ingress, _, metrics, impact, _ = components(tick=1, dedupe=1)
    assert (
        ingress.try_publish(captured(CapturedKind.TICK_NOTIFICATION, 1, dedupe="x"))
        is IngressDecision.ACCEPTED
    )
    assert (
        ingress.try_publish(captured(CapturedKind.TICK_NOTIFICATION, 2, dedupe="x"))
        is IngressDecision.DUPLICATE
    )
    assert (
        ingress.try_publish(captured(CapturedKind.TICK_NOTIFICATION, 3, dedupe="y"))
        is IngressDecision.DROPPED
    )
    assert (
        ingress.try_publish(captured(CapturedKind.TICK_NOTIFICATION, 4, dedupe="y"))
        is IngressDecision.DROPPED
    )
    snapshot = metrics.snapshot()
    assert snapshot.dropped_tick_count == 2
    assert snapshot.first_dropped_tick_sequence == 3
    assert snapshot.last_dropped_tick_sequence == 4
    assert impact.snapshot(SESSION).dropped_tick_count == 2
    assert ingress.snapshot().dedupe_depth == 1


def test_pop_priority_then_quote_tick_round_robin():
    ingress, *_ = components(control=2, diagnostic=2, quote=2, tick=2)
    ingress.try_publish(captured(CapturedKind.TICK_NOTIFICATION, 1))
    ingress.try_publish(captured(CapturedKind.QUOTE_SNAPSHOT, 2))
    ingress.try_publish(captured(CapturedKind.CONNECTION_NOTIFICATION, 3))
    ingress.try_publish(captured(CapturedKind.ADAPTER_DIAGNOSTIC, 4))
    assert [ingress.try_pop().sequence for _ in range(4)] == [4, 3, 2, 1]
    assert ingress.try_pop() is None


def test_sta_first_overflow_is_preserved_and_second_is_fatal_without_hook():
    health = PipelineHealth(Clock())
    metrics = IngressMetrics()
    impact = SessionImpactTracker(1)
    shutdown = ControlledShutdown()
    queue = BoundedStaQuoteQueue(
        1,
        health=health,
        metrics=metrics,
        session_impact=impact,
        shutdown=shutdown,
        session_id=SESSION,
    )
    item = StaLocalQuoteNotification(0, 1, False, 1, NOW)
    first_overflow = replace(item, callback_sequence=2)
    second_overflow = replace(item, callback_sequence=3)
    assert queue.try_publish(item) is StaIngressDecision.ACCEPTED
    assert queue.try_publish(first_overflow) is StaIngressDecision.OVERFLOW
    assert queue.snapshot().main_depth == 1
    assert queue.snapshot().overflow_depth == 1
    assert health.snapshot().state is HealthState.DEGRADED
    assert not shutdown.snapshot().is_requested
    assert queue.try_publish(second_overflow) is StaIngressDecision.OVERFLOW
    assert health.snapshot().state is HealthState.FAILED
    assert shutdown.snapshot().request_count == 1
    assert queue.try_pop_overflow() == first_overflow
    assert queue.try_pop_overflow() is None
    assert queue.try_pop() == item
    with pytest.raises(TypeError):
        queue.try_publish(object())


def test_sta_queue_rejects_the_old_arbitrary_blocking_overflow_hook():
    blocked = Event()
    ingress, health, metrics, impact, shutdown = components()
    del ingress
    with pytest.raises(TypeError, match="overflow_handler"):
        BoundedStaQuoteQueue(
            1,
            health=health,
            metrics=metrics,
            session_impact=impact,
            shutdown=shutdown,
            session_id=SESSION,
            overflow_handler=lambda notification: blocked.wait(60),
        )


def test_critical_and_market_lanes_are_bounded_fair():
    ingress, *_ = components(control=4, diagnostic=4, quote=2, tick=2, burst=2)
    for sequence in range(1, 4):
        ingress.try_publish(captured(CapturedKind.ADAPTER_DIAGNOSTIC, sequence))
        ingress.try_publish(captured(CapturedKind.CONNECTION_NOTIFICATION, sequence + 10))
    ingress.try_publish(captured(CapturedKind.TICK_NOTIFICATION, 30))
    popped = [ingress.try_pop() for _ in range(3)]
    assert [event.captured_kind for event in popped] == [
        CapturedKind.ADAPTER_DIAGNOSTIC,
        CapturedKind.CONNECTION_NOTIFICATION,
        CapturedKind.TICK_NOTIFICATION,
    ]
    remaining_critical = [ingress.try_pop() for _ in range(4)]
    kinds = [event.captured_kind for event in remaining_critical]
    assert kinds[:2] == [
        CapturedKind.ADAPTER_DIAGNOSTIC,
        CapturedKind.CONNECTION_NOTIFICATION,
    ]


def test_session_capacity_exhaustion_makes_unknown_dropped_session_incomplete():
    ingress, _, _, impact, shutdown = components(tick=1, sessions=1)
    session_b = UUID(int=2)
    impact.mark_incomplete(SESSION, "existing_session_damage")
    assert (
        ingress.try_publish(captured(CapturedKind.TICK_NOTIFICATION, 1)) is IngressDecision.ACCEPTED
    )
    assert (
        ingress.try_publish(
            captured(
                CapturedKind.TICK_NOTIFICATION,
                2,
                session=session_b,
            )
        )
        is IngressDecision.DROPPED
    )
    assert impact.capacity_exhausted
    assert impact.snapshot(session_b).is_incomplete
    assert impact.effective_terminal_status(session_b, "complete") == "incomplete"
    assert impact.tracked_session_count == 1
    assert shutdown.snapshot().is_requested
