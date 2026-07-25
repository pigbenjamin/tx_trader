"""Low-cardinality, thread-safe counters for bounded ingress."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from threading import Lock
from types import MappingProxyType
from typing import TYPE_CHECKING, Mapping

if TYPE_CHECKING:
    from tx_trade.market_data.ports import IngressDecision


class IngressLane(StrEnum):
    DIAGNOSTIC = "diagnostic"
    CONTROL = "control"
    QUOTE = "quote"
    TICK = "tick"
    STA_QUOTE = "sta_quote"


def _frozen(values: dict[IngressLane, int]) -> Mapping[IngressLane, int]:
    return MappingProxyType(dict(values))


@dataclass(frozen=True, slots=True)
class IngressMetricsSnapshot:
    received: Mapping[IngressLane, int]
    accepted: Mapping[IngressLane, int]
    coalesced: Mapping[IngressLane, int]
    dropped: Mapping[IngressLane, int]
    duplicates: Mapping[IngressLane, int]
    depth: Mapping[IngressLane, int]
    capacity: Mapping[IngressLane, int]
    overflow: Mapping[IngressLane, int]
    dropped_tick_count: int
    first_dropped_tick_sequence: int | None
    last_dropped_tick_sequence: int | None
    quote_drops: int
    shutdown_requests: int
    processing_failures: int
    storage_failures: int


class IngressMetrics:
    """All updates are atomic and labels are restricted to :class:`IngressLane`."""

    def __init__(self, capacities: Mapping[IngressLane, int] | None = None) -> None:
        supplied = {} if capacities is None else dict(capacities)
        for lane, value in supplied.items():
            if type(lane) is not IngressLane:
                raise TypeError("capacity keys must be IngressLane")
            if type(value) is not int or value <= 0:
                raise ValueError("capacities must be positive integers")
        self._received = {lane: 0 for lane in IngressLane}
        self._accepted = {lane: 0 for lane in IngressLane}
        self._coalesced = {lane: 0 for lane in IngressLane}
        self._dropped = {lane: 0 for lane in IngressLane}
        self._duplicates = {lane: 0 for lane in IngressLane}
        self._depth = {lane: 0 for lane in IngressLane}
        self._capacity = {lane: supplied.get(lane, 0) for lane in IngressLane}
        self._overflow = {lane: 0 for lane in IngressLane}
        self._first_dropped_tick: int | None = None
        self._last_dropped_tick: int | None = None
        self._shutdown_requests = 0
        self._processing_failures = 0
        self._storage_failures = 0
        self._lock = Lock()

    def set_capacity(self, lane: IngressLane, capacity: int) -> None:
        self._validate_lane(lane)
        if type(capacity) is not int or capacity <= 0:
            raise ValueError("capacity must be a positive integer")
        with self._lock:
            current = self._capacity[lane]
            if current not in (0, capacity):
                raise ValueError("capacity was already set differently")
            self._capacity[lane] = capacity

    def record_result(
        self,
        lane: IngressLane,
        decision: IngressDecision,
        *,
        sequence: int | None = None,
        overflow: bool = False,
        depth: int | None = None,
    ) -> None:
        self._validate_lane(lane)
        from tx_trade.market_data.ports import IngressDecision

        if type(decision) is not IngressDecision:
            raise TypeError("decision must be IngressDecision")
        value = decision.value
        if lane is IngressLane.TICK and value == "dropped":
            if type(sequence) is not int or sequence < 0:
                raise ValueError("dropped tick requires a non-negative sequence")
        if type(overflow) is not bool:
            raise TypeError("overflow must be bool")
        if depth is not None and (type(depth) is not int or depth < 0):
            raise ValueError("depth must be non-negative")
        with self._lock:
            if depth is not None:
                capacity = self._capacity[lane]
                if capacity and depth > capacity:
                    raise ValueError("depth exceeds capacity")
            self._received[lane] += 1
            target = {
                "accepted": self._accepted,
                "coalesced": self._coalesced,
                "dropped": self._dropped,
                "duplicate": self._duplicates,
            }[value]
            target[lane] += 1
            if overflow:
                self._overflow[lane] += 1
            if depth is not None:
                self._depth[lane] = depth
            if lane is IngressLane.TICK and value == "dropped":
                assert sequence is not None
                if self._first_dropped_tick is None:
                    self._first_dropped_tick = sequence
                self._last_dropped_tick = sequence

    def update_depth(self, lane: IngressLane, depth: int) -> None:
        self._validate_lane(lane)
        if type(depth) is not int or depth < 0:
            raise ValueError("depth must be non-negative")
        with self._lock:
            capacity = self._capacity[lane]
            if capacity and depth > capacity:
                raise ValueError("depth exceeds capacity")
            self._depth[lane] = depth

    def record_shutdown_request(self) -> None:
        with self._lock:
            self._shutdown_requests += 1

    def record_processing_failure(self) -> None:
        with self._lock:
            self._processing_failures += 1

    def record_storage_failure(self) -> None:
        with self._lock:
            self._storage_failures += 1

    @staticmethod
    def _validate_lane(lane: IngressLane) -> None:
        if type(lane) is not IngressLane:
            raise TypeError("lane must be IngressLane")

    def snapshot(self) -> IngressMetricsSnapshot:
        with self._lock:
            return IngressMetricsSnapshot(
                _frozen(self._received),
                _frozen(self._accepted),
                _frozen(self._coalesced),
                _frozen(self._dropped),
                _frozen(self._duplicates),
                _frozen(self._depth),
                _frozen(self._capacity),
                _frozen(self._overflow),
                self._dropped[IngressLane.TICK],
                self._first_dropped_tick,
                self._last_dropped_tick,
                self._dropped[IngressLane.QUOTE],
                self._shutdown_requests,
                self._processing_failures,
                self._storage_failures,
            )
