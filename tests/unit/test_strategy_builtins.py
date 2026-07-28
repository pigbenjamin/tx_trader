from __future__ import annotations

from decimal import Decimal

import pytest

from tx_trade.market_data.fixtures import make_offline_fixture_envelopes
from tx_trade.orders import OrderSide, OrderType, TimeInForce
from tx_trade.strategy import (
    InstrumentTriggeredOrderStrategy,
    NoOpStrategy,
    OrderTemplate,
    StrategyContext,
)


def _template() -> OrderTemplate:
    return OrderTemplate(
        strategy_id="alpha",
        client_order_id="entry-1",
        account_id="paper-account",
        instrument_id="TAIFEX:0:TX00",
        side=OrderSide.BUY,
        quantity=Decimal("2"),
        order_type=OrderType.LIMIT,
        limit_price=Decimal("20002"),
        time_in_force=TimeInForce.DAY,
        day_trade=False,
    )


def _context(index: int) -> StrategyContext:
    envelope = make_offline_fixture_envelopes()[index]
    return StrategyContext(
        strategy_id="alpha",
        source_session_id=envelope.session_id,
        source_ingest_sequence=envelope.ingest_sequence,
        decision_at=envelope.event_at or envelope.received_at,
        orders=(),
        positions=(),
        broker_snapshot_version=0,
    )


def test_noop_is_stateless_and_deterministic() -> None:
    envelope = make_offline_fixture_envelopes()[0]
    strategy = NoOpStrategy()
    assert strategy.decide(envelope, _context(0)).commands == ()
    assert strategy.decide(envelope, _context(0)).commands == ()


def test_instrument_strategy_emits_source_bound_order_only_for_exact_instrument() -> None:
    envelopes = make_offline_fixture_envelopes()
    strategy = InstrumentTriggeredOrderStrategy(_template())

    assert strategy.decide(envelopes[0], _context(0)).commands == ()
    decision = strategy.decide(envelopes[2], _context(2))
    assert len(decision.commands) == 1
    intent = decision.commands[0]
    assert intent.strategy_id == "alpha"
    assert intent.created_at == envelopes[2].event_at
    assert intent.source_session_id == envelopes[2].session_id
    assert intent.source_ingest_sequence == 2
    assert strategy.decide(envelopes[2], _context(2)) == decision


def test_order_template_enforces_order_shape() -> None:
    with pytest.raises(ValueError, match="market templates"):
        OrderTemplate(
            strategy_id="alpha",
            client_order_id="entry-1",
            account_id="paper-account",
            instrument_id="TAIFEX:0:TX00",
            side=OrderSide.BUY,
            quantity=Decimal("1"),
            order_type=OrderType.MARKET,
            limit_price=Decimal("1"),
            time_in_force=TimeInForce.DAY,
            day_trade=False,
        )
