"""Pure deterministic slippage and fee policies for paper execution."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import (
    Context,
    Decimal,
    DecimalException,
    Inexact,
    ROUND_HALF_EVEN,
    ROUND_HALF_UP,
    Rounded,
    localcontext,
)
from enum import StrEnum

from .contracts import (
    FeePolicyKind,
    FeeRoundingMode,
    MatchSkipReason,
    OrderSide,
    PaperFeeSchedule,
    SlippageConfig,
    SlippageMode,
)


def _decimal_context(*, rounding: str = ROUND_HALF_EVEN) -> Context:
    return Context(prec=34, rounding=rounding, Emin=-6143, Emax=6144, clamp=1)


def _exact_decimal_context(*, rounding: str = ROUND_HALF_EVEN) -> Context:
    context = _decimal_context(rounding=rounding)
    context.traps[Inexact] = True
    context.traps[Rounded] = True
    return context


class ExecutionPolicyErrorCode(StrEnum):
    INVALID_INPUT = "invalid_input"
    ARITHMETIC_FAILURE = "arithmetic_failure"
    SLIPPAGE_UNREPRESENTABLE = "slippage_unrepresentable"
    FEE_RULE_MISSING = "fee_rule_missing"
    FEE_CURRENCY_MISSING = "fee_currency_missing"
    FEE_CURRENCY_MISMATCH = "fee_currency_mismatch"


class ExecutionPolicyError(ArithmeticError):
    """Stable fail-closed error raised by a built-in execution policy."""

    def __init__(self, code: ExecutionPolicyErrorCode) -> None:
        self.code = code
        super().__init__(code.value)


@dataclass(frozen=True, slots=True)
class SlippageAssessment:
    reference_price: Decimal
    execution_price: Decimal
    slippage_amount: Decimal


@dataclass(frozen=True, slots=True)
class LimitAssessment:
    executable: bool
    skip_reason: MatchSkipReason | None = None

    def __post_init__(self) -> None:
        if self.executable != (self.skip_reason is None):
            raise ValueError("skip_reason must be present exactly when execution is disallowed")


@dataclass(frozen=True, slots=True)
class FeeAssessment:
    fee: Decimal
    currency: str | None

    def __post_init__(self) -> None:
        if type(self.fee) is not Decimal or not self.fee.is_finite() or self.fee < 0:
            raise ValueError("fee must be a finite non-negative Decimal")
        if (self.fee == 0) != (self.currency is None):
            raise ValueError("currency must be present exactly when fee is nonzero")


def assess_slippage(
    reference_price: Decimal,
    side: OrderSide,
    config: SlippageConfig,
) -> SlippageAssessment:
    """Apply configured adverse slippage using private decimal128 arithmetic."""

    if type(reference_price) is not Decimal or not reference_price.is_finite():
        raise ExecutionPolicyError(ExecutionPolicyErrorCode.INVALID_INPUT)
    if reference_price <= 0 or type(side) is not OrderSide or type(config) is not SlippageConfig:
        raise ExecutionPolicyError(ExecutionPolicyErrorCode.INVALID_INPUT)

    try:
        with localcontext(_decimal_context()):
            if config.mode is SlippageMode.NONE:
                delta = Decimal(0)
            elif config.mode is SlippageMode.BASIS_POINTS:
                delta = reference_price * config.value / Decimal(10_000)
            else:
                delta = +config.value
            execution = (
                reference_price + delta if side is OrderSide.BUY else reference_price - delta
            )
            actual_delta = abs(execution - reference_price)
    except DecimalException as exc:
        raise ExecutionPolicyError(ExecutionPolicyErrorCode.ARITHMETIC_FAILURE) from exc

    if not delta.is_finite() or delta < 0:
        raise ExecutionPolicyError(ExecutionPolicyErrorCode.ARITHMETIC_FAILURE)
    if config.mode is not SlippageMode.NONE and (delta == 0 or actual_delta == 0):
        raise ExecutionPolicyError(ExecutionPolicyErrorCode.SLIPPAGE_UNREPRESENTABLE)
    if not execution.is_finite() or execution <= 0:
        raise ExecutionPolicyError(ExecutionPolicyErrorCode.SLIPPAGE_UNREPRESENTABLE)
    if side is OrderSide.BUY and execution < reference_price:
        raise ExecutionPolicyError(ExecutionPolicyErrorCode.SLIPPAGE_UNREPRESENTABLE)
    if side is OrderSide.SELL and execution > reference_price:
        raise ExecutionPolicyError(ExecutionPolicyErrorCode.SLIPPAGE_UNREPRESENTABLE)
    return SlippageAssessment(reference_price, execution, actual_delta)


def assess_limit(
    execution_price: Decimal,
    side: OrderSide,
    limit_price: Decimal | None,
) -> LimitAssessment:
    """Check the post-slippage limit without capping the execution price."""

    if (
        type(execution_price) is not Decimal
        or not execution_price.is_finite()
        or execution_price <= 0
        or type(side) is not OrderSide
    ):
        raise ExecutionPolicyError(ExecutionPolicyErrorCode.INVALID_INPUT)
    if limit_price is None:
        return LimitAssessment(True)
    if type(limit_price) is not Decimal or not limit_price.is_finite() or limit_price <= 0:
        raise ExecutionPolicyError(ExecutionPolicyErrorCode.INVALID_INPUT)
    executable = (
        execution_price <= limit_price if side is OrderSide.BUY else execution_price >= limit_price
    )
    return (
        LimitAssessment(True)
        if executable
        else LimitAssessment(False, MatchSkipReason.SLIPPAGE_EXCEEDS_LIMIT)
    )


def assess_fee(
    schedule: PaperFeeSchedule,
    *,
    instrument_id: str,
    metadata_currency: str | None,
    cumulative_quantity_before: Decimal,
    cumulative_quantity_after: Decimal,
) -> FeeAssessment:
    """Calculate a cumulative-delta fee so partial-fill splits are invariant."""

    if type(schedule) is not PaperFeeSchedule or type(instrument_id) is not str:
        raise ExecutionPolicyError(ExecutionPolicyErrorCode.INVALID_INPUT)
    if (
        type(cumulative_quantity_before) is not Decimal
        or type(cumulative_quantity_after) is not Decimal
        or not cumulative_quantity_before.is_finite()
        or not cumulative_quantity_after.is_finite()
        or cumulative_quantity_before < 0
        or cumulative_quantity_after <= cumulative_quantity_before
    ):
        raise ExecutionPolicyError(ExecutionPolicyErrorCode.INVALID_INPUT)
    if schedule.kind is FeePolicyKind.ZERO:
        return FeeAssessment(Decimal(0), None)

    rule = next(
        (candidate for candidate in schedule.rules if candidate.instrument_id == instrument_id),
        None,
    )
    if rule is None:
        raise ExecutionPolicyError(ExecutionPolicyErrorCode.FEE_RULE_MISSING)
    if metadata_currency is None:
        raise ExecutionPolicyError(ExecutionPolicyErrorCode.FEE_CURRENCY_MISSING)
    if metadata_currency != rule.currency:
        raise ExecutionPolicyError(ExecutionPolicyErrorCode.FEE_CURRENCY_MISMATCH)

    if rule.rounding_mode is not FeeRoundingMode.ROUND_HALF_UP:
        raise ExecutionPolicyError(ExecutionPolicyErrorCode.INVALID_INPUT)
    try:
        with localcontext(_decimal_context(rounding=ROUND_HALF_UP)):
            before_amount = rule.amount_per_unit * cumulative_quantity_before
            after_amount = rule.amount_per_unit * cumulative_quantity_after
            before_units = (before_amount / rule.quantum).to_integral_value(rounding=ROUND_HALF_UP)
            after_units = (after_amount / rule.quantum).to_integral_value(rounding=ROUND_HALF_UP)
        with localcontext(_exact_decimal_context(rounding=ROUND_HALF_UP)):
            before = before_units * rule.quantum
            after = after_units * rule.quantum
            fee = after - before
    except DecimalException as exc:
        raise ExecutionPolicyError(ExecutionPolicyErrorCode.ARITHMETIC_FAILURE) from exc
    if not fee.is_finite() or fee < 0:
        raise ExecutionPolicyError(ExecutionPolicyErrorCode.ARITHMETIC_FAILURE)
    return FeeAssessment(fee, rule.currency if fee != 0 else None)
