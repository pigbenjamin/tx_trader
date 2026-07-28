"""Small deterministic strategies for paper research composition."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from tx_trade.market_data.models import EventType, Instrument, MarketDataEnvelope
from tx_trade.orders.contracts import (
    OrderIntent,
    OrderSide,
    OrderType,
    TimeInForce,
)

from .contracts import StrategyContext, StrategyDecision


def _nonempty(value: object, name: str) -> None:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    if not value.strip():
        raise ValueError(f"{name} must not be empty")


@dataclass(frozen=True, slots=True)
class OrderTemplate:
    strategy_id: str
    client_order_id: str
    account_id: str
    instrument_id: str
    side: OrderSide
    quantity: Decimal
    order_type: OrderType
    limit_price: Decimal | None
    time_in_force: TimeInForce
    day_trade: bool

    def __post_init__(self) -> None:
        for name in ("strategy_id", "client_order_id", "account_id", "instrument_id"):
            _nonempty(getattr(self, name), name)
        if type(self.side) is not OrderSide:
            raise TypeError("side must be OrderSide")
        if type(self.quantity) is not Decimal:
            raise TypeError("quantity must be Decimal")
        if not self.quantity.is_finite() or self.quantity <= 0:
            raise ValueError("quantity must be finite and positive")
        if type(self.order_type) is not OrderType:
            raise TypeError("order_type must be OrderType")
        if type(self.time_in_force) is not TimeInForce:
            raise TypeError("time_in_force must be TimeInForce")
        if type(self.day_trade) is not bool:
            raise TypeError("day_trade must be bool")
        if self.order_type is OrderType.MARKET:
            if self.limit_price is not None:
                raise ValueError("market templates must not have a limit_price")
        elif (
            type(self.limit_price) is not Decimal
            or not self.limit_price.is_finite()
            or self.limit_price <= 0
        ):
            raise ValueError("limit templates require a finite positive limit_price")


@dataclass(frozen=True, slots=True)
class NoOpStrategy:
    def decide(
        self,
        envelope: MarketDataEnvelope,
        context: StrategyContext,
    ) -> StrategyDecision:
        return StrategyDecision(commands=())


@dataclass(frozen=True, slots=True)
class InstrumentTriggeredOrderStrategy:
    template: OrderTemplate

    def __post_init__(self) -> None:
        if type(self.template) is not OrderTemplate:
            raise TypeError("template must be OrderTemplate")

    def decide(
        self,
        envelope: MarketDataEnvelope,
        context: StrategyContext,
    ) -> StrategyDecision:
        if (
            envelope.event_type is not EventType.INSTRUMENT
            or type(envelope.payload) is not Instrument
            or envelope.payload.instrument_id != self.template.instrument_id
            or any(
                order.intent.client_order_id == self.template.client_order_id
                for order in context.orders
            )
        ):
            return StrategyDecision(commands=())
        return StrategyDecision(
            commands=(
                OrderIntent(
                    strategy_id=self.template.strategy_id,
                    client_order_id=self.template.client_order_id,
                    account_id=self.template.account_id,
                    instrument_id=self.template.instrument_id,
                    side=self.template.side,
                    quantity=self.template.quantity,
                    order_type=self.template.order_type,
                    limit_price=self.template.limit_price,
                    time_in_force=self.template.time_in_force,
                    day_trade=self.template.day_trade,
                    created_at=context.decision_at,
                    source_session_id=context.source_session_id,
                    source_ingest_sequence=context.source_ingest_sequence,
                ),
            )
        )
