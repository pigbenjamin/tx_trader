"""Pure quote-top matching helpers for deterministic paper execution."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Context, Decimal, ROUND_HALF_EVEN, localcontext

from tx_trade.market_data.models import Quote

from .contracts import (
    InstrumentMetadataSnapshot,
    MatchSkipReason,
    OrderSide,
    OrderType,
    PaperOrder,
)

MATCHING_CONTEXT = Context(prec=34, rounding=ROUND_HALF_EVEN)


@dataclass(frozen=True, slots=True)
class QuoteTop:
    """Validated top-of-book values and independent side capacities."""

    bid: Decimal
    ask: Decimal
    bid_capacity: Decimal | None
    ask_capacity: Decimal | None
    currency: str | None = None
    skip_reasons: tuple[MatchSkipReason, ...] = ()


def inspect_quote_top(
    quote: Quote,
    metadata: InstrumentMetadataSnapshot | None,
) -> QuoteTop | tuple[MatchSkipReason, ...]:
    """Validate a quote against exact metadata, without guessing missing values."""

    if metadata is None:
        return (MatchSkipReason.METADATA_UNAVAILABLE,)
    if quote.instrument_id != metadata.instrument_id:
        return (MatchSkipReason.METADATA_MISMATCH,)
    if metadata.price_scale is None or quote.price_scale is None:
        return (MatchSkipReason.PRICE_SCALE_UNAVAILABLE,)
    if quote.price_scale != metadata.price_scale:
        return (MatchSkipReason.METADATA_MISMATCH,)
    if metadata.quantity_scale is None:
        return (MatchSkipReason.QUANTITY_SCALE_UNAVAILABLE,)
    if quote.is_simulated is not False:
        return (MatchSkipReason.SIMULATED_QUOTE,)
    if quote.bid_normalized is None or quote.ask_normalized is None:
        return (MatchSkipReason.PRICE_UNAVAILABLE,)
    if (
        not quote.bid_normalized.is_finite()
        or not quote.ask_normalized.is_finite()
        or quote.bid_normalized <= 0
        or quote.ask_normalized <= 0
        or quote.bid_normalized > quote.ask_normalized
    ):
        return (MatchSkipReason.INVALID_BOOK,)

    reasons: list[MatchSkipReason] = []
    bid_capacity = _capacity(quote.bid_qty_raw, metadata.quantity_scale)
    ask_capacity = _capacity(quote.ask_qty_raw, metadata.quantity_scale)
    if bid_capacity is None or ask_capacity is None:
        reasons.append(MatchSkipReason.QUANTITY_UNAVAILABLE)
    if bid_capacity == 0 and ask_capacity == 0:
        reasons.append(MatchSkipReason.NO_LIQUIDITY)
    return QuoteTop(
        bid=quote.bid_normalized,
        ask=quote.ask_normalized,
        bid_capacity=bid_capacity,
        ask_capacity=ask_capacity,
        currency=metadata.currency,
        skip_reasons=tuple(reasons),
    )


def execution_price(order: PaperOrder, top: QuoteTop) -> Decimal | None:
    """Return the executable top price, or None when a limit does not cross."""

    if order.intent.side is OrderSide.BUY:
        price = top.ask
    else:
        price = top.bid
    if order.intent.order_type is OrderType.MARKET:
        return price
    limit_price = order.intent.limit_price
    assert limit_price is not None
    crossed = price <= limit_price if order.intent.side is OrderSide.BUY else price >= limit_price
    return price if crossed else None


def weighted_average(
    previous_quantity: Decimal,
    previous_average: Decimal | None,
    fill_quantity: Decimal,
    fill_price: Decimal,
) -> Decimal:
    """Calculate an average using a process-global-context-independent policy."""

    with localcontext(MATCHING_CONTEXT):
        previous_notional = (
            Decimal(0) if previous_average is None else previous_quantity * previous_average
        )
        total_quantity = previous_quantity + fill_quantity
        result = (previous_notional + fill_quantity * fill_price) / total_quantity
    if not result.is_finite() or result <= 0:
        raise ArithmeticError("weighted average is not a finite positive Decimal")
    return result


def _capacity(raw_quantity: int | None, scale: Decimal) -> Decimal | None:
    if raw_quantity is None:
        return None
    with localcontext(MATCHING_CONTEXT):
        result = Decimal(raw_quantity) * scale
    if not result.is_finite() or result < 0:
        raise ArithmeticError("quote capacity is not a finite non-negative Decimal")
    return result
