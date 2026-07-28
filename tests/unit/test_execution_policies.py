from __future__ import annotations

from decimal import Decimal, getcontext, localcontext

import pytest

from tx_trade.orders.contracts import (
    FeePolicyKind,
    FeeRoundingMode,
    MatchSkipReason,
    OrderSide,
    PaperFeeRule,
    PaperFeeSchedule,
    SlippageConfig,
    SlippageMode,
)
from tx_trade.orders.execution_policies import (
    ExecutionPolicyError,
    ExecutionPolicyErrorCode,
    assess_fee,
    assess_limit,
    assess_slippage,
)


def _schedule(*, rate: str = "0.125", quantum: str = "0.01") -> PaperFeeSchedule:
    return PaperFeeSchedule(
        kind=FeePolicyKind.PER_UNIT,
        rules=(
            PaperFeeRule(
                instrument_id="TX",
                currency="TWD",
                amount_per_unit=Decimal(rate),
                quantum=Decimal(quantum),
                rounding_mode=FeeRoundingMode.ROUND_HALF_UP,
                policy_id="test",
                policy_version="1",
            ),
        ),
    )


@pytest.mark.parametrize(
    ("side", "mode", "value", "expected"),
    [
        (OrderSide.BUY, SlippageMode.NONE, "0", "100"),
        (OrderSide.SELL, SlippageMode.NONE, "0", "100"),
        (OrderSide.BUY, SlippageMode.BASIS_POINTS, "25", "100.25"),
        (OrderSide.SELL, SlippageMode.BASIS_POINTS, "25", "99.75"),
        (OrderSide.BUY, SlippageMode.ABSOLUTE, "1.5", "101.5"),
        (OrderSide.SELL, SlippageMode.ABSOLUTE, "1.5", "98.5"),
    ],
)
def test_slippage_is_adverse_and_deterministic(
    side: OrderSide,
    mode: SlippageMode,
    value: str,
    expected: str,
) -> None:
    result = assess_slippage(
        Decimal("100"),
        side,
        SlippageConfig(mode=mode, value=Decimal(value)),
    )

    assert result.reference_price == Decimal("100")
    assert result.execution_price == Decimal(expected)
    assert result.slippage_amount == abs(Decimal(expected) - Decimal("100"))


def test_slippage_fails_closed_when_sell_price_is_not_positive() -> None:
    with pytest.raises(ExecutionPolicyError) as raised:
        assess_slippage(
            Decimal("1"),
            OrderSide.SELL,
            SlippageConfig(mode=SlippageMode.ABSOLUTE, value=Decimal("1")),
        )

    assert raised.value.code is ExecutionPolicyErrorCode.SLIPPAGE_UNREPRESENTABLE


def test_nonzero_slippage_cannot_collapse_at_decimal128_precision() -> None:
    with pytest.raises(ExecutionPolicyError) as raised:
        assess_slippage(
            Decimal("1e6144"),
            OrderSide.BUY,
            SlippageConfig(mode=SlippageMode.ABSOLUTE, value=Decimal("1e-6143")),
        )

    assert raised.value.code in {
        ExecutionPolicyErrorCode.ARITHMETIC_FAILURE,
        ExecutionPolicyErrorCode.SLIPPAGE_UNREPRESENTABLE,
    }


@pytest.mark.parametrize(
    ("side", "execution", "limit", "allowed"),
    [
        (OrderSide.BUY, "100", "100", True),
        (OrderSide.BUY, "100.01", "100", False),
        (OrderSide.SELL, "100", "100", True),
        (OrderSide.SELL, "99.99", "100", False),
    ],
)
def test_post_slippage_limit_matrix(
    side: OrderSide, execution: str, limit: str, allowed: bool
) -> None:
    result = assess_limit(Decimal(execution), side, Decimal(limit))

    assert result.executable is allowed
    assert result.skip_reason is (None if allowed else MatchSkipReason.SLIPPAGE_EXCEEDS_LIMIT)


def test_market_limit_assessment_is_allowed() -> None:
    assert assess_limit(Decimal("100"), OrderSide.BUY, None).executable


def test_zero_fee_does_not_require_currency_metadata() -> None:
    result = assess_fee(
        PaperFeeSchedule(),
        instrument_id="TX",
        metadata_currency=None,
        cumulative_quantity_before=Decimal("0"),
        cumulative_quantity_after=Decimal("2"),
    )

    assert result.fee == 0
    assert result.currency is None


@pytest.mark.parametrize(
    ("instrument", "currency", "code"),
    [
        ("MX", "TWD", ExecutionPolicyErrorCode.FEE_RULE_MISSING),
        ("TX", None, ExecutionPolicyErrorCode.FEE_CURRENCY_MISSING),
        ("TX", "USD", ExecutionPolicyErrorCode.FEE_CURRENCY_MISMATCH),
    ],
)
def test_per_unit_fee_metadata_failures_are_typed(
    instrument: str,
    currency: str | None,
    code: ExecutionPolicyErrorCode,
) -> None:
    with pytest.raises(ExecutionPolicyError) as raised:
        assess_fee(
            _schedule(),
            instrument_id=instrument,
            metadata_currency=currency,
            cumulative_quantity_before=Decimal("0"),
            cumulative_quantity_after=Decimal("1"),
        )

    assert raised.value.code is code


def test_fee_uses_cumulative_delta_and_is_split_invariant() -> None:
    schedule = _schedule(rate="0.333", quantum="0.01")
    whole = assess_fee(
        schedule,
        instrument_id="TX",
        metadata_currency="TWD",
        cumulative_quantity_before=Decimal("0"),
        cumulative_quantity_after=Decimal("3"),
    )
    first = assess_fee(
        schedule,
        instrument_id="TX",
        metadata_currency="TWD",
        cumulative_quantity_before=Decimal("0"),
        cumulative_quantity_after=Decimal("1"),
    )
    second = assess_fee(
        schedule,
        instrument_id="TX",
        metadata_currency="TWD",
        cumulative_quantity_before=Decimal("1"),
        cumulative_quantity_after=Decimal("3"),
    )

    assert whole.fee == Decimal("1.00")
    assert first.fee + second.fee == whole.fee


def test_fee_rounding_ties_use_half_up() -> None:
    result = assess_fee(
        _schedule(rate="0.005", quantum="0.01"),
        instrument_id="TX",
        metadata_currency="TWD",
        cumulative_quantity_before=Decimal("0"),
        cumulative_quantity_after=Decimal("1"),
    )

    assert result.fee == Decimal("0.01")
    assert result.currency == "TWD"


def test_fee_quantum_is_a_true_increment_not_only_a_decimal_scale() -> None:
    result = assess_fee(
        _schedule(rate="0.03", quantum="0.05"),
        instrument_id="TX",
        metadata_currency="TWD",
        cumulative_quantity_before=Decimal("0"),
        cumulative_quantity_after=Decimal("1"),
    )

    assert result.fee == Decimal("0.05")
    assert result.fee % Decimal("0.05") == 0


def test_non_power_of_ten_quantum_preserves_split_invariance() -> None:
    schedule = _schedule(rate="0.03", quantum="0.05")
    whole = assess_fee(
        schedule,
        instrument_id="TX",
        metadata_currency="TWD",
        cumulative_quantity_before=Decimal("0"),
        cumulative_quantity_after=Decimal("2"),
    )
    first = assess_fee(
        schedule,
        instrument_id="TX",
        metadata_currency="TWD",
        cumulative_quantity_before=Decimal("0"),
        cumulative_quantity_after=Decimal("1"),
    )
    second = assess_fee(
        schedule,
        instrument_id="TX",
        metadata_currency="TWD",
        cumulative_quantity_before=Decimal("1"),
        cumulative_quantity_after=Decimal("2"),
    )

    assert whole.fee == Decimal("0.05")
    assert first.fee + second.fee == whole.fee


def test_fee_extreme_arithmetic_fails_closed() -> None:
    with pytest.raises(ExecutionPolicyError) as raised:
        assess_fee(
            _schedule(rate="1e6144", quantum="1"),
            instrument_id="TX",
            metadata_currency="TWD",
            cumulative_quantity_before=Decimal("0"),
            cumulative_quantity_after=Decimal("10"),
        )

    assert raised.value.code is ExecutionPolicyErrorCode.ARITHMETIC_FAILURE


def test_policies_ignore_process_global_decimal_context() -> None:
    baseline_slippage = assess_slippage(
        Decimal("123.456"),
        OrderSide.BUY,
        SlippageConfig(mode=SlippageMode.BASIS_POINTS, value=Decimal("7.5")),
    )
    baseline_fee = assess_fee(
        _schedule(quantum="0.05"),
        instrument_id="TX",
        metadata_currency="TWD",
        cumulative_quantity_before=Decimal("2"),
        cumulative_quantity_after=Decimal("7"),
    )

    with localcontext() as context:
        context.prec = 6
        context.rounding = "ROUND_DOWN"
        changed_slippage = assess_slippage(
            Decimal("123.456"),
            OrderSide.BUY,
            SlippageConfig(mode=SlippageMode.BASIS_POINTS, value=Decimal("7.5")),
        )
        changed_fee = assess_fee(
            _schedule(quantum="0.05"),
            instrument_id="TX",
            metadata_currency="TWD",
            cumulative_quantity_before=Decimal("2"),
            cumulative_quantity_after=Decimal("7"),
        )

    assert changed_slippage == baseline_slippage
    assert changed_fee == baseline_fee
    assert getcontext().prec != 6
