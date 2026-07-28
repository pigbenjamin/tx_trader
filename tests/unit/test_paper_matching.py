from __future__ import annotations

from dataclasses import replace
from decimal import Decimal, getcontext
from uuid import UUID

from tx_trade.market_data.fixtures import make_offline_fixture_envelopes
from tx_trade.market_data.models import Instrument, Quote
from tx_trade.orders.contracts import (
    InstrumentMetadataSnapshot,
    MatchSkipReason,
    OrderIntent,
    OrderSide,
    OrderStatus,
    OrderType,
    PaperOrder,
    TimeInForce,
)
from tx_trade.orders.matching import (
    QuoteTop,
    execution_price,
    inspect_quote_top,
    weighted_average,
)

RUN_ID = UUID("11111111-1111-1111-1111-111111111111")


def _fixture_values() -> tuple[Instrument, Quote]:
    envelopes = make_offline_fixture_envelopes()
    instrument = envelopes[2].payload
    quote = envelopes[3].payload
    assert isinstance(instrument, Instrument)
    assert isinstance(quote, Quote)
    return instrument, quote


def _order(
    side: OrderSide,
    order_type: OrderType = OrderType.MARKET,
    limit_price: Decimal | None = None,
) -> PaperOrder:
    _, quote = _fixture_values()
    intent = OrderIntent(
        strategy_id="strategy",
        client_order_id=f"{side.value}-{order_type.value}",
        account_id="paper",
        instrument_id=quote.instrument_id,
        side=side,
        quantity=Decimal("3"),
        order_type=order_type,
        limit_price=limit_price,
        time_in_force=TimeInForce.DAY,
        day_trade=False,
        created_at=quote.received_at,
    )
    return PaperOrder(
        paper_run_id=RUN_ID,
        paper_order_id=UUID("22222222-2222-2222-2222-222222222222"),
        intent=intent,
        status=OrderStatus.ACCEPTED,
        filled_quantity=Decimal(0),
        remaining_quantity=intent.quantity,
        average_fill_price=None,
        accepted_at=intent.created_at,
        updated_at=intent.created_at,
    )


def test_quote_top_uses_exact_scales_and_independent_capacities() -> None:
    instrument, quote = _fixture_values()
    metadata = InstrumentMetadataSnapshot(
        instrument_id=instrument.instrument_id,
        metadata_version=instrument.metadata_version,
        price_scale=instrument.price_scale,
        quantity_scale=Decimal("0.5"),
        currency=instrument.currency,
    )

    result = inspect_quote_top(quote, metadata)

    assert isinstance(result, QuoteTop)
    assert result.bid == Decimal("20000.00")
    assert result.ask == Decimal("20002.00")
    assert result.bid_capacity == Decimal("1.5")
    assert result.ask_capacity == Decimal("2.0")
    assert result.currency == "TWD"


def test_execution_price_uses_ask_for_buy_bid_for_sell_and_limit_crossing() -> None:
    top = QuoteTop(
        bid=Decimal("99"),
        ask=Decimal("101"),
        bid_capacity=Decimal("5"),
        ask_capacity=Decimal("5"),
    )

    assert execution_price(_order(OrderSide.BUY), top) == Decimal("101")
    assert execution_price(_order(OrderSide.SELL), top) == Decimal("99")
    assert (
        execution_price(
            _order(OrderSide.BUY, OrderType.LIMIT, Decimal("100")),
            top,
        )
        is None
    )
    assert (
        execution_price(
            _order(OrderSide.SELL, OrderType.LIMIT, Decimal("100")),
            top,
        )
        is None
    )


def test_weighted_average_ignores_process_global_decimal_context() -> None:
    original = getcontext().copy()
    try:
        getcontext().prec = 6
        low_context = weighted_average(
            Decimal("1"),
            Decimal("12345.67890123456789"),
            Decimal("2"),
            Decimal("12346.78901234567891"),
        )
        getcontext().prec = 50
        high_context = weighted_average(
            Decimal("1"),
            Decimal("12345.67890123456789"),
            Decimal("2"),
            Decimal("12346.78901234567891"),
        )
    finally:
        getcontext().prec = original.prec
        getcontext().rounding = original.rounding

    assert low_context == high_context
    assert low_context == Decimal("12346.41897530864190333333333333333")


def test_quote_inspection_fails_closed_for_mismatch_and_simulation() -> None:
    instrument, quote = _fixture_values()
    metadata = InstrumentMetadataSnapshot(
        instrument_id=instrument.instrument_id,
        metadata_version=1,
        price_scale=Decimal("0.1"),
        quantity_scale=Decimal("1"),
    )

    assert inspect_quote_top(quote, metadata) == (MatchSkipReason.METADATA_MISMATCH,)
    assert inspect_quote_top(
        replace(quote, is_simulated=None), replace(metadata, price_scale=quote.price_scale)
    ) == (MatchSkipReason.SIMULATED_QUOTE,)
