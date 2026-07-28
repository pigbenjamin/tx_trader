"""Immutable contracts for deterministic paper strategy evaluation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING
from uuid import UUID

from tx_trade.orders.contracts import CancelIntent, OrderIntent, PaperOrder, PaperPosition

if TYPE_CHECKING:
    from .ports import StrategyPort


class StrategyExecutionMode(StrEnum):
    OBSERVE_ONLY = "observe_only"
    PAPER = "paper"


def _strict_nonempty_string(value: object, name: str) -> None:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    if not value.strip():
        raise ValueError(f"{name} must not be empty")


def _strict_nonnegative_int(value: object, name: str) -> None:
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    if value < 0:
        raise ValueError(f"{name} must be non-negative")


def _strict_taipei_datetime(value: object, name: str) -> None:
    if type(value) is not datetime:
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    if getattr(value.tzinfo, "key", None) != "Asia/Taipei":
        raise ValueError(f"{name} must use Asia/Taipei timezone")


@dataclass(frozen=True, slots=True)
class StrategyContext:
    strategy_id: str
    source_session_id: UUID
    source_ingest_sequence: int
    decision_at: datetime
    orders: tuple[PaperOrder, ...]
    positions: tuple[PaperPosition, ...]
    broker_snapshot_version: int

    def __post_init__(self) -> None:
        _strict_nonempty_string(self.strategy_id, "strategy_id")
        if type(self.source_session_id) is not UUID:
            raise TypeError("source_session_id must be UUID")
        _strict_nonnegative_int(self.source_ingest_sequence, "source_ingest_sequence")
        _strict_taipei_datetime(self.decision_at, "decision_at")
        if type(self.orders) is not tuple:
            raise TypeError("orders must be a tuple")
        if any(type(order) is not PaperOrder for order in self.orders):
            raise TypeError("orders must contain only PaperOrder")
        if type(self.positions) is not tuple:
            raise TypeError("positions must be a tuple")
        if any(type(position) is not PaperPosition for position in self.positions):
            raise TypeError("positions must contain only PaperPosition")
        _strict_nonnegative_int(self.broker_snapshot_version, "broker_snapshot_version")
        if any(order.intent.strategy_id != self.strategy_id for order in self.orders):
            raise ValueError("orders must belong to strategy_id")
        if any(position.strategy_id != self.strategy_id for position in self.positions):
            raise ValueError("positions must belong to strategy_id")
        order_keys = tuple(
            (
                order.intent.account_id,
                order.intent.instrument_id,
                order.intent.client_order_id,
                str(order.paper_order_id),
            )
            for order in self.orders
        )
        if order_keys != tuple(sorted(order_keys)):
            raise ValueError("orders must be sorted deterministically")
        position_keys = tuple(
            (position.account_id, position.instrument_id, str(position.paper_position_id))
            for position in self.positions
        )
        if position_keys != tuple(sorted(position_keys)):
            raise ValueError("positions must be sorted deterministically")


@dataclass(frozen=True, slots=True)
class StrategyDecision:
    commands: tuple[OrderIntent | CancelIntent, ...]

    def __post_init__(self) -> None:
        if type(self.commands) is not tuple:
            raise TypeError("commands must be a tuple")
        if any(type(command) not in {OrderIntent, CancelIntent} for command in self.commands):
            raise TypeError("commands must contain only OrderIntent or CancelIntent")


@dataclass(frozen=True, slots=True)
class StrategyRegistration:
    strategy_id: str
    strategy: StrategyPort

    def __post_init__(self) -> None:
        _strict_nonempty_string(self.strategy_id, "strategy_id")
        if self.strategy is None or not callable(getattr(self.strategy, "decide", None)):
            raise TypeError("strategy must implement StrategyPort")
