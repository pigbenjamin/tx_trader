"""Bounded non-blocking ingress primitives for Phase 1.

The recent dedupe cache is deliberately only a best-effort pre-ingest filter.
Its finite LRU window cannot establish long-range uniqueness; the SQLite
``UNIQUE`` constraint remains the final authority.
"""

from __future__ import annotations

from collections import OrderedDict, deque
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from threading import Lock
from typing import TypeAlias
from uuid import UUID

from tx_trade.monitoring.health import (
    ControlledShutdown,
    PipelineHealth,
    SessionImpactTracker,
)
from tx_trade.monitoring.metrics import IngressLane, IngressMetrics

from .models import (
    CapturedKind,
    CapturedMarketDataEvent,
    CapturedQuoteSnapshot,
    StaLocalQuoteNotification,
)
from .pipeline import CapturedEventPipeline
from .ports import IngressDecision

_CONTROL_KINDS = {
    CapturedKind.CONNECTION_NOTIFICATION,
    CapturedKind.SERVER_TIME_NOTIFICATION,
    CapturedKind.STOCK_LIST_NOTIFICATION,
}

_QuoteKey: TypeAlias = tuple[UUID, int, int, int]
_DedupeKey: TypeAlias = tuple[UUID, int, str]


def _positive(value: int, name: str) -> None:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive integer")


@dataclass(frozen=True, slots=True)
class BoundedIngressSnapshot:
    diagnostic_depth: int
    control_depth: int
    quote_depth: int
    tick_depth: int
    dedupe_depth: int
    diagnostic_capacity: int
    control_capacity: int
    quote_capacity: int
    tick_capacity: int
    dedupe_capacity: int

    @property
    def total_depth(self) -> int:
        return (
            self.diagnostic_depth
            + self.control_depth
            + self.quote_depth
            + self.tick_depth
        )


class BoundedIngress:
    """Four independent bounded lanes safe for concurrent callback producers."""

    def __init__(
        self,
        *,
        control_capacity: int,
        diagnostic_capacity: int,
        quote_capacity: int,
        tick_capacity: int,
        dedupe_capacity: int,
        health: PipelineHealth,
        metrics: IngressMetrics,
        session_impact: SessionImpactTracker,
        shutdown: ControlledShutdown,
        priority_burst_limit: int = 8,
    ) -> None:
        for name, value in (
            ("control_capacity", control_capacity),
            ("diagnostic_capacity", diagnostic_capacity),
            ("quote_capacity", quote_capacity),
            ("tick_capacity", tick_capacity),
            ("dedupe_capacity", dedupe_capacity),
        ):
            _positive(value, name)
        _positive(priority_burst_limit, "priority_burst_limit")
        self._capacities = {
            IngressLane.CONTROL: control_capacity,
            IngressLane.DIAGNOSTIC: diagnostic_capacity,
            IngressLane.QUOTE: quote_capacity,
            IngressLane.TICK: tick_capacity,
        }
        self._dedupe_capacity = dedupe_capacity
        self._health = health
        self._metrics = metrics
        self._impact = session_impact
        self._shutdown = shutdown
        self._diagnostic: deque[CapturedMarketDataEvent] = deque()
        self._control: deque[CapturedMarketDataEvent] = deque()
        self._quotes: OrderedDict[_QuoteKey, CapturedMarketDataEvent] = OrderedDict()
        self._ticks: deque[CapturedMarketDataEvent] = deque()
        self._recent: OrderedDict[_DedupeKey, None] = OrderedDict()
        self._market_turn = IngressLane.QUOTE
        self._critical_turn = IngressLane.DIAGNOSTIC
        self._priority_burst_limit = priority_burst_limit
        self._priority_burst = 0
        self._lock = Lock()
        for lane, capacity in self._capacities.items():
            metrics.set_capacity(lane, capacity)

    def try_publish(self, event: CapturedMarketDataEvent) -> IngressDecision:
        if type(event) is not CapturedMarketDataEvent:
            raise TypeError("event must be exactly CapturedMarketDataEvent")
        lane = self._lane(event.captured_kind)
        with self._lock:
            dedupe_key = self._dedupe_key(event)
            if dedupe_key is not None and dedupe_key in self._recent:
                self._recent.move_to_end(dedupe_key)
                decision = IngressDecision.DUPLICATE
            elif lane is IngressLane.QUOTE:
                decision = self._publish_quote(event)
            else:
                decision = self._publish_fifo(lane, event)
            if (
                dedupe_key is not None
                and decision in (IngressDecision.ACCEPTED, IngressDecision.COALESCED)
            ):
                self._remember(dedupe_key)
            depth = self._lane_depth(lane)
            dropped = decision is IngressDecision.DROPPED
            self._metrics.record_result(
                lane,
                decision,
                sequence=event.sequence if lane is IngressLane.TICK else None,
                overflow=dropped,
                depth=depth,
            )
        if dropped:
            self._handle_overflow(lane, event)
        return decision

    @staticmethod
    def _lane(kind: CapturedKind) -> IngressLane:
        if kind is CapturedKind.ADAPTER_DIAGNOSTIC:
            return IngressLane.DIAGNOSTIC
        if kind in _CONTROL_KINDS:
            return IngressLane.CONTROL
        if kind is CapturedKind.QUOTE_SNAPSHOT:
            return IngressLane.QUOTE
        if kind is CapturedKind.TICK_NOTIFICATION:
            return IngressLane.TICK
        raise ValueError(f"unsupported captured kind: {kind}")

    @staticmethod
    def _dedupe_key(event: CapturedMarketDataEvent) -> _DedupeKey | None:
        candidate = event.dedupe_candidate
        if candidate is None:
            return None
        return event.session_id, event.connection_generation, candidate

    def _remember(self, key: _DedupeKey) -> None:
        self._recent[key] = None
        self._recent.move_to_end(key)
        while len(self._recent) > self._dedupe_capacity:
            self._recent.popitem(last=False)

    def _publish_fifo(
        self, lane: IngressLane, event: CapturedMarketDataEvent
    ) -> IngressDecision:
        queue = {
            IngressLane.DIAGNOSTIC: self._diagnostic,
            IngressLane.CONTROL: self._control,
            IngressLane.TICK: self._ticks,
        }[lane]
        if len(queue) >= self._capacities[lane]:
            return IngressDecision.DROPPED
        queue.append(event)
        return IngressDecision.ACCEPTED

    def _publish_quote(self, event: CapturedMarketDataEvent) -> IngressDecision:
        payload = event.payload
        assert type(payload) is CapturedQuoteSnapshot
        key = (
            event.session_id,
            event.connection_generation,
            payload.market_no_raw,
            payload.stock_idx_raw,
        )
        pending = self._quotes.get(key)
        if pending is not None:
            if event.sequence <= pending.sequence:
                return IngressDecision.DUPLICATE
            self._quotes[key] = event
            # A replacement is kept at the original queue position.
            return IngressDecision.COALESCED
        if len(self._quotes) >= self._capacities[IngressLane.QUOTE]:
            return IngressDecision.DROPPED
        self._quotes[key] = event
        return IngressDecision.ACCEPTED

    def _handle_overflow(
        self, lane: IngressLane, event: CapturedMarketDataEvent
    ) -> None:
        reason = f"{lane.value}_ingress_overflow"
        if lane in (IngressLane.DIAGNOSTIC, IngressLane.CONTROL):
            self._health.fail(reason)
        else:
            self._health.degrade(reason)
        self._mark_incomplete(event.session_id, reason)
        if lane is IngressLane.TICK:
            try:
                self._impact.record_dropped_tick(event.session_id, event.sequence)
            except RuntimeError:
                self._impact_capacity_failed()
        if lane in (IngressLane.DIAGNOSTIC, IngressLane.CONTROL):
            self._request_shutdown(reason)

    def _mark_incomplete(self, session_id: UUID, reason: str) -> None:
        try:
            self._impact.mark_incomplete(session_id, reason)
        except RuntimeError:
            self._impact_capacity_failed()

    def _impact_capacity_failed(self) -> None:
        reason = "session_impact_capacity_exhausted"
        self._health.fail(reason)
        self._request_shutdown(reason)

    def _request_shutdown(self, reason: str) -> None:
        if self._shutdown.request_shutdown(reason):
            self._metrics.record_shutdown_request()

    def try_pop(self) -> CapturedMarketDataEvent | None:
        with self._lock:
            critical_waiting = bool(self._diagnostic or self._control)
            market_waiting = bool(self._quotes or self._ticks)
            if critical_waiting and (
                not market_waiting
                or self._priority_burst < self._priority_burst_limit
            ):
                lane, event = self._pop_critical()
                self._priority_burst = min(
                    self._priority_burst + 1, self._priority_burst_limit
                )
            elif market_waiting:
                lane, event = self._pop_market()
                self._priority_burst = 0
            else:
                return None
            assert lane is not None and event is not None
            depth = self._lane_depth(lane)
            self._metrics.update_depth(lane, depth)
        return event

    def _pop_critical(
        self,
    ) -> tuple[IngressLane, CapturedMarketDataEvent]:
        if self._diagnostic and self._control:
            lane = self._critical_turn
            self._critical_turn = (
                IngressLane.CONTROL
                if lane is IngressLane.DIAGNOSTIC
                else IngressLane.DIAGNOSTIC
            )
        elif self._diagnostic:
            lane = IngressLane.DIAGNOSTIC
        else:
            lane = IngressLane.CONTROL
        queue = (
            self._diagnostic
            if lane is IngressLane.DIAGNOSTIC
            else self._control
        )
        return lane, queue.popleft()

    def _pop_market(
        self,
    ) -> tuple[IngressLane, CapturedMarketDataEvent]:
        if self._quotes and self._ticks:
            lane = self._market_turn
            self._market_turn = (
                IngressLane.TICK
                if lane is IngressLane.QUOTE
                else IngressLane.QUOTE
            )
        elif self._quotes:
            lane = IngressLane.QUOTE
        elif self._ticks:
            lane = IngressLane.TICK
        else:
            raise RuntimeError("market queue is empty")
        if lane is IngressLane.QUOTE:
            _, event = self._quotes.popitem(last=False)
            return lane, event
        event = self._ticks.popleft()
        return lane, event

    def _lane_depth(self, lane: IngressLane) -> int:
        return {
            IngressLane.DIAGNOSTIC: len(self._diagnostic),
            IngressLane.CONTROL: len(self._control),
            IngressLane.QUOTE: len(self._quotes),
            IngressLane.TICK: len(self._ticks),
        }[lane]

    def snapshot(self) -> BoundedIngressSnapshot:
        with self._lock:
            return BoundedIngressSnapshot(
                len(self._diagnostic),
                len(self._control),
                len(self._quotes),
                len(self._ticks),
                len(self._recent),
                self._capacities[IngressLane.DIAGNOSTIC],
                self._capacities[IngressLane.CONTROL],
                self._capacities[IngressLane.QUOTE],
                self._capacities[IngressLane.TICK],
                self._dedupe_capacity,
            )

    def depth(self, lane: IngressLane | None = None) -> int:
        snapshot = self.snapshot()
        if lane is None:
            return snapshot.total_depth
        if type(lane) is not IngressLane or lane is IngressLane.STA_QUOTE:
            raise ValueError("lane must be a cross-thread ingress lane")
        return {
            IngressLane.DIAGNOSTIC: snapshot.diagnostic_depth,
            IngressLane.CONTROL: snapshot.control_depth,
            IngressLane.QUOTE: snapshot.quote_depth,
            IngressLane.TICK: snapshot.tick_depth,
        }[lane]


class StaIngressDecision(StrEnum):
    ACCEPTED = "accepted"
    OVERFLOW = "overflow"


@dataclass(frozen=True, slots=True)
class BoundedStaQuoteQueueSnapshot:
    main_depth: int
    main_capacity: int
    overflow_depth: int
    overflow_capacity: int = 1

    @property
    def total_depth(self) -> int:
        return self.main_depth + self.overflow_depth


class BoundedStaQuoteQueue:
    """Bounded STA-local handoff; construction of captured events stays in Slice 5."""

    def __init__(
        self,
        capacity: int,
        *,
        health: PipelineHealth,
        metrics: IngressMetrics,
        session_impact: SessionImpactTracker,
        shutdown: ControlledShutdown,
        session_id: UUID,
    ) -> None:
        _positive(capacity, "capacity")
        if type(session_id) is not UUID:
            raise TypeError("session_id must be UUID")
        self._capacity = capacity
        self._health = health
        self._metrics = metrics
        self._impact = session_impact
        self._shutdown = shutdown
        self._session_id = session_id
        self._queue: deque[StaLocalQuoteNotification] = deque()
        self._overflow: StaLocalQuoteNotification | None = None
        self._lock = Lock()
        metrics.set_capacity(IngressLane.STA_QUOTE, capacity + 1)

    def try_publish(
        self, notification: StaLocalQuoteNotification
    ) -> StaIngressDecision:
        if type(notification) is not StaLocalQuoteNotification:
            raise TypeError("notification must be exactly StaLocalQuoteNotification")
        lane = IngressLane.STA_QUOTE
        with self._lock:
            if len(self._queue) < self._capacity:
                self._queue.append(notification)
                accepted = True
                fatal = False
            else:
                accepted = False
                fatal = self._overflow is not None
                if not fatal:
                    self._overflow = notification
            depth = len(self._queue) + (self._overflow is not None)
            self._metrics.record_result(
                lane,
                (
                    IngressDecision.ACCEPTED
                    if accepted
                    else IngressDecision.DROPPED
                ),
                overflow=not accepted,
                depth=depth,
            )
        if accepted:
            return StaIngressDecision.ACCEPTED
        reason = "sta_quote_ingress_overflow"
        self._health.degrade(reason)
        try:
            self._impact.mark_incomplete(self._session_id, reason)
        except RuntimeError:
            self._fail_and_shutdown("session_impact_capacity_exhausted")
        if fatal:
            self._fail_and_shutdown("sta_quote_overflow_slot_exhausted")
        return StaIngressDecision.OVERFLOW

    def _fail_and_shutdown(self, reason: str) -> None:
        self._health.fail(reason)
        if self._shutdown.request_shutdown(reason):
            self._metrics.record_shutdown_request()

    def try_pop(self) -> StaLocalQuoteNotification | None:
        with self._lock:
            if not self._queue:
                return None
            item = self._queue.popleft()
            depth = len(self._queue) + (self._overflow is not None)
            self._metrics.update_depth(IngressLane.STA_QUOTE, depth)
        return item

    def try_pop_overflow(self) -> StaLocalQuoteNotification | None:
        with self._lock:
            item = self._overflow
            self._overflow = None
            depth = len(self._queue)
            if item is not None:
                self._metrics.update_depth(IngressLane.STA_QUOTE, depth)
        return item

    @property
    def depth(self) -> int:
        with self._lock:
            return len(self._queue) + (self._overflow is not None)

    def snapshot(self) -> BoundedStaQuoteQueueSnapshot:
        with self._lock:
            return BoundedStaQuoteQueueSnapshot(
                len(self._queue),
                self._capacity,
                int(self._overflow is not None),
            )


class IngressProcessorHaltedError(RuntimeError):
    """Raised after the first popped event fails processing."""


@dataclass(frozen=True, slots=True)
class BoundedIngressProcessorSnapshot:
    is_halted: bool
    in_flight_failed_event: CapturedMarketDataEvent | None


class BoundedIngressProcessor:
    """Drains captured events; a popped-but-failed event is explicitly audited."""

    def __init__(
        self,
        ingress: BoundedIngress,
        pipeline: CapturedEventPipeline,
        health: PipelineHealth,
        metrics: IngressMetrics,
        session_impact: SessionImpactTracker,
        shutdown: ControlledShutdown,
    ) -> None:
        self._ingress = ingress
        self._pipeline = pipeline
        self._health = health
        self._metrics = metrics
        self._impact = session_impact
        self._shutdown = shutdown
        self._lock = Lock()
        self._halted = False
        self._in_flight_failed_event: CapturedMarketDataEvent | None = None

    def process_one(self) -> bool:
        with self._lock:
            if self._halted:
                raise IngressProcessorHaltedError("ingress processor is halted")
            event = self._ingress.try_pop()
            if event is None:
                return False
            try:
                self._pipeline.accept(event)
            except Exception:
                self._halted = True
                self._in_flight_failed_event = event
                reason = "ingress_processing_failure"
                self._metrics.record_processing_failure()
                self._health.fail(reason)
                try:
                    self._impact.mark_incomplete(event.session_id, reason)
                except RuntimeError:
                    self._health.fail("session_impact_capacity_exhausted")
                if self._shutdown.request_shutdown(reason):
                    self._metrics.record_shutdown_request()
                raise
            return True

    def drain(self, max_events: int) -> int:
        _positive(max_events, "max_events")
        processed = 0
        while processed < max_events and self.process_one():
            processed += 1
        return processed

    def snapshot(self) -> BoundedIngressProcessorSnapshot:
        with self._lock:
            return BoundedIngressProcessorSnapshot(
                self._halted, self._in_flight_failed_event
            )


class PipelineStorageFailureNotifier:
    """Bridges a background writer fatal state into pipeline shutdown state."""

    def __init__(
        self,
        session_id: UUID,
        health: PipelineHealth,
        metrics: IngressMetrics,
        session_impact: SessionImpactTracker,
        shutdown: ControlledShutdown,
    ) -> None:
        if type(session_id) is not UUID:
            raise TypeError("session_id must be UUID")
        self._session_id = session_id
        self._health = health
        self._metrics = metrics
        self._impact = session_impact
        self._shutdown = shutdown

    def notify_storage_failure(self) -> None:
        reason = "background_storage_failure"
        self._metrics.record_storage_failure()
        self._health.fail(reason)
        try:
            self._impact.mark_incomplete(self._session_id, reason)
        except RuntimeError:
            self._health.fail("session_impact_capacity_exhausted")
        if self._shutdown.request_shutdown(reason):
            self._metrics.record_shutdown_request()
