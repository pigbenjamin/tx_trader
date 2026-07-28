"""Structural ports for declarative strategy evaluation."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from tx_trade.market_data.models import MarketDataEnvelope
from tx_trade.orders.contracts import PaperBrokerSnapshot
from tx_trade.orders.ports import TransactionalPaperBrokerPort

from .contracts import StrategyContext, StrategyDecision


@runtime_checkable
class StrategyPort(Protocol):
    def decide(
        self,
        envelope: MarketDataEnvelope,
        context: StrategyContext,
    ) -> StrategyDecision: ...


@runtime_checkable
class TransactionalPaperBrokerSnapshotPort(TransactionalPaperBrokerPort, Protocol):
    def snapshot(self) -> PaperBrokerSnapshot: ...
