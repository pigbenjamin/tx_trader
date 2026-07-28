from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal, localcontext
from uuid import UUID
from zoneinfo import ZoneInfo

import pytest

from tx_trade.orders.contracts import OrderSide, PaperFill
from tx_trade.orders.position_ledger import (
    ALLOW_NET_SHORT,
    PositionLedgerError,
    PositionLedgerErrorCode,
    apply_fill_to_position,
    paper_position_id,
)

TAIPEI = ZoneInfo("Asia/Taipei")
RUN_ID = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
SESSION_ID = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")


def _fill(
    side: OrderSide,
    quantity: str,
    price: str,
    *,
    sequence: int,
    fee: str = "0",
    currency: str | None = None,
) -> PaperFill:
    return PaperFill(
        paper_run_id=RUN_ID,
        paper_fill_id=UUID(int=sequence),
        paper_order_id=UUID(int=100 + sequence),
        strategy_id="strategy",
        account_id="account",
        instrument_id="TX",
        side=side,
        quantity=Decimal(quantity),
        execution_price=Decimal(price),
        fee=Decimal(fee),
        fee_currency=currency,
        source_session_id=SESSION_ID,
        source_ingest_sequence=sequence,
        occurred_at=datetime(2025, 1, 1, tzinfo=TAIPEI) + timedelta(seconds=sequence),
    )


def test_position_id_is_deterministic_and_delimiter_safe() -> None:
    first = paper_position_id(RUN_ID, "a|b", "c", "d")
    repeated = paper_position_id(RUN_ID, "a|b", "c", "d")
    distinct = paper_position_id(RUN_ID, "a", "b|c", "d")

    assert first == repeated
    assert first != distinct


def test_long_add_reduce_flat_and_reversal() -> None:
    opened = apply_fill_to_position(None, _fill(OrderSide.BUY, "2", "100", sequence=1))
    added = apply_fill_to_position(opened, _fill(OrderSide.BUY, "2", "110", sequence=2))
    reduced = apply_fill_to_position(added, _fill(OrderSide.SELL, "1", "120", sequence=3))
    flat = apply_fill_to_position(reduced, _fill(OrderSide.SELL, "3", "90", sequence=4))
    reversed_short = apply_fill_to_position(flat, _fill(OrderSide.SELL, "2", "95", sequence=5))
    reversed_long = apply_fill_to_position(
        reversed_short, _fill(OrderSide.BUY, "3", "105", sequence=6)
    )

    assert (opened.net_quantity, opened.average_open_price) == (
        Decimal("2"),
        Decimal("100"),
    )
    assert (added.net_quantity, added.average_open_price) == (
        Decimal("4"),
        Decimal("105"),
    )
    assert (reduced.net_quantity, reduced.average_open_price) == (
        Decimal("3"),
        Decimal("105"),
    )
    assert (flat.net_quantity, flat.average_open_price) == (Decimal("0"), None)
    assert (reversed_short.net_quantity, reversed_short.average_open_price) == (
        Decimal("-2"),
        Decimal("95"),
    )
    assert (reversed_long.net_quantity, reversed_long.average_open_price) == (
        Decimal("1"),
        Decimal("105"),
    )
    assert reversed_long.version == 6
    assert ALLOW_NET_SHORT is True


def test_short_add_reduce_and_cross_zero() -> None:
    short = apply_fill_to_position(None, _fill(OrderSide.SELL, "3", "100", sequence=1))
    added = apply_fill_to_position(short, _fill(OrderSide.SELL, "1", "80", sequence=2))
    reduced = apply_fill_to_position(added, _fill(OrderSide.BUY, "2", "70", sequence=3))
    crossed = apply_fill_to_position(reduced, _fill(OrderSide.BUY, "3", "90", sequence=4))

    assert added.average_open_price == Decimal("95")
    assert (reduced.net_quantity, reduced.average_open_price) == (
        Decimal("-2"),
        Decimal("95"),
    )
    assert (crossed.net_quantity, crossed.average_open_price) == (
        Decimal("1"),
        Decimal("90"),
    )


def test_fees_accumulate_and_currency_is_established_once_nonzero() -> None:
    first = apply_fill_to_position(None, _fill(OrderSide.BUY, "1", "100", sequence=1))
    second = apply_fill_to_position(
        first,
        _fill(
            OrderSide.BUY,
            "1",
            "100",
            sequence=2,
            fee="1.25",
            currency="TWD",
        ),
    )
    third = apply_fill_to_position(second, _fill(OrderSide.SELL, "2", "100", sequence=3))

    assert first.cumulative_fees == 0
    assert first.fee_currency is None
    assert third.cumulative_fees == Decimal("1.25")
    assert third.fee_currency == "TWD"
    assert third.net_quantity == 0


def test_currency_mismatch_and_key_mismatch_fail_closed() -> None:
    position = apply_fill_to_position(
        None,
        _fill(
            OrderSide.BUY,
            "1",
            "100",
            sequence=1,
            fee="1",
            currency="TWD",
        ),
    )
    usd_fill = _fill(
        OrderSide.BUY,
        "1",
        "100",
        sequence=2,
        fee="1",
        currency="USD",
    )
    with pytest.raises(PositionLedgerError, match="currency"):
        apply_fill_to_position(position, usd_fill)

    other_run_fill = PaperFill(
        **{
            field: getattr(usd_fill, field)
            for field in usd_fill.__dataclass_fields__
            if field != "paper_run_id"
        },
        paper_run_id=UUID("cccccccc-cccc-cccc-cccc-cccccccccccc"),
    )
    with pytest.raises(PositionLedgerError, match="key"):
        apply_fill_to_position(position, other_run_fill)


def test_temporally_older_fill_is_rejected() -> None:
    position = apply_fill_to_position(None, _fill(OrderSide.BUY, "1", "100", sequence=2))

    with pytest.raises(PositionLedgerError) as raised:
        apply_fill_to_position(position, _fill(OrderSide.BUY, "1", "100", sequence=1))

    assert raised.value.code is PositionLedgerErrorCode.TEMPORAL_ORDER


def test_net_quantity_inexact_addition_fails_closed() -> None:
    position = apply_fill_to_position(None, _fill(OrderSide.BUY, "1", "100", sequence=1))
    position = replace(
        position,
        net_quantity=Decimal("1e34"),
        average_open_price=Decimal("100"),
    )

    with pytest.raises(PositionLedgerError) as raised:
        apply_fill_to_position(position, _fill(OrderSide.SELL, "0.1", "100", sequence=2))

    assert raised.value.code is PositionLedgerErrorCode.ARITHMETIC_FAILURE


def test_cumulative_fee_inexact_addition_fails_closed() -> None:
    position = apply_fill_to_position(
        None,
        _fill(
            OrderSide.BUY,
            "1",
            "100",
            sequence=1,
            fee="1",
            currency="TWD",
        ),
    )
    position = replace(
        position,
        cumulative_fees=Decimal("1e34"),
        fee_currency="TWD",
    )

    with pytest.raises(PositionLedgerError) as raised:
        apply_fill_to_position(
            position,
            _fill(
                OrderSide.BUY,
                "1",
                "100",
                sequence=2,
                fee="1",
                currency="TWD",
            ),
        )

    assert raised.value.code is PositionLedgerErrorCode.ARITHMETIC_FAILURE


def test_zero_fee_preserves_large_cumulative_fee_exactly() -> None:
    position = apply_fill_to_position(
        None,
        _fill(
            OrderSide.BUY,
            "1",
            "100",
            sequence=1,
            fee="1",
            currency="TWD",
        ),
    )
    position = replace(
        position,
        cumulative_fees=Decimal("1e34"),
        fee_currency="TWD",
    )
    zero_fee_fill = _fill(OrderSide.BUY, "1", "110", sequence=2)

    updated = apply_fill_to_position(position, zero_fee_fill)

    assert updated.cumulative_fees is position.cumulative_fees
    assert updated.cumulative_fees == Decimal("1e34")
    assert updated.fee_currency == "TWD"
    assert updated.net_quantity == Decimal("2")
    assert updated.version == position.version + 1
    assert updated.updated_at == zero_fee_fill.occurred_at


def test_ledger_ignores_process_global_decimal_context() -> None:
    fill = _fill(OrderSide.BUY, "3", "100.123456789", sequence=1)
    baseline = apply_fill_to_position(None, fill)
    second_fill = _fill(OrderSide.BUY, "7", "101.987654321", sequence=2)
    baseline = apply_fill_to_position(baseline, second_fill)

    with localcontext() as context:
        context.prec = 5
        context.rounding = "ROUND_DOWN"
        changed = apply_fill_to_position(None, fill)
        changed = apply_fill_to_position(changed, second_fill)

    assert changed == baseline
