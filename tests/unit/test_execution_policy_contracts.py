from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from decimal import Decimal, getcontext

import pytest

from tx_trade.orders import (
    DEFAULT_EXECUTION_CONFIG,
    DEFAULT_EXECUTION_CONFIG_FINGERPRINT,
    FeePolicyKind,
    FeeRoundingMode,
    PaperExecutionConfig,
    PaperFeeRule,
    PaperFeeSchedule,
    SlippageConfig,
    SlippageMode,
    canonical_json,
)


def fee_rule(**overrides: object) -> PaperFeeRule:
    values: dict[str, object] = {
        "instrument_id": "TXF-202608",
        "currency": "TWD",
        "amount_per_unit": Decimal("4"),
        "quantum": Decimal("1"),
        "rounding_mode": FeeRoundingMode.ROUND_HALF_UP,
        "policy_id": "paper-per-unit",
        "policy_version": "1",
    }
    values.update(overrides)
    return PaperFeeRule(**values)  # type: ignore[arg-type]


def test_default_execution_config_is_explicit_immutable_and_auditable() -> None:
    assert DEFAULT_EXECUTION_CONFIG == PaperExecutionConfig()
    assert DEFAULT_EXECUTION_CONFIG.slippage == SlippageConfig()
    assert DEFAULT_EXECUTION_CONFIG.fee_schedule == PaperFeeSchedule()
    assert DEFAULT_EXECUTION_CONFIG_FINGERPRINT.startswith("sha256:")
    assert len(DEFAULT_EXECUTION_CONFIG_FINGERPRINT) == 71
    assert DEFAULT_EXECUTION_CONFIG.fingerprint == DEFAULT_EXECUTION_CONFIG_FINGERPRINT
    assert '"algorithm_version":"paper-execution-v1"' in canonical_json(DEFAULT_EXECUTION_CONFIG)
    with pytest.raises(FrozenInstanceError):
        DEFAULT_EXECUTION_CONFIG.algorithm_version = "other"  # type: ignore[misc]


@pytest.mark.parametrize(
    ("mode", "value"),
    [
        (SlippageMode.NONE, Decimal("0")),
        (SlippageMode.BASIS_POINTS, Decimal("1.5")),
        (SlippageMode.ABSOLUTE, Decimal("2")),
    ],
)
def test_slippage_modes_accept_only_their_strict_decimal_domain(
    mode: SlippageMode, value: Decimal
) -> None:
    assert SlippageConfig(mode=mode, value=value).value == value
    with pytest.raises(TypeError, match="value must be Decimal"):
        SlippageConfig(mode=mode, value=1)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="mode must be SlippageMode"):
        SlippageConfig(mode=mode.value, value=value)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("mode", "value"),
    [
        (SlippageMode.NONE, Decimal("1")),
        (SlippageMode.BASIS_POINTS, Decimal("0")),
        (SlippageMode.BASIS_POINTS, Decimal("10000")),
        (SlippageMode.ABSOLUTE, Decimal("0")),
    ],
)
def test_slippage_rejects_invalid_mode_value_combinations(
    mode: SlippageMode, value: Decimal
) -> None:
    with pytest.raises(ValueError):
        SlippageConfig(mode=mode, value=value)


def test_per_unit_fee_schedule_is_sorted_unique_and_strict() -> None:
    first = fee_rule(instrument_id="MXF-202608")
    second = fee_rule()
    schedule = PaperFeeSchedule(kind=FeePolicyKind.PER_UNIT, rules=(first, second))

    assert schedule.rules == (first, second)
    with pytest.raises(ValueError, match="sorted"):
        replace(schedule, rules=(second, first))
    with pytest.raises(ValueError, match="unique"):
        replace(schedule, rules=(second, second))
    with pytest.raises(TypeError, match="rules must be a tuple"):
        replace(schedule, rules=[first])  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="must not contain"):
        PaperFeeSchedule(kind=FeePolicyKind.ZERO, rules=(first,))
    with pytest.raises(ValueError, match="requires at least one"):
        PaperFeeSchedule(kind=FeePolicyKind.PER_UNIT)


@pytest.mark.parametrize("currency", ["twd", "TW", "TW12", "臺幣"])
def test_fee_rule_rejects_noncanonical_currency(currency: str) -> None:
    with pytest.raises(ValueError, match="uppercase 3-letter"):
        fee_rule(currency=currency)


def test_execution_fingerprint_is_semantic_and_sensitive() -> None:
    left = PaperExecutionConfig(slippage=SlippageConfig(SlippageMode.BASIS_POINTS, Decimal("4.0")))
    right = PaperExecutionConfig(
        slippage=SlippageConfig(SlippageMode.BASIS_POINTS, Decimal("4.00"))
    )
    changed = PaperExecutionConfig(slippage=SlippageConfig(SlippageMode.BASIS_POINTS, Decimal("5")))

    assert left.fingerprint == right.fingerprint
    assert left.fingerprint != changed.fingerprint


def test_execution_fingerprint_does_not_depend_on_global_decimal_context() -> None:
    config = PaperExecutionConfig(
        slippage=SlippageConfig(
            SlippageMode.ABSOLUTE,
            Decimal("1234567890123456789012345678901234"),
        )
    )
    original_precision = getcontext().prec
    try:
        getcontext().prec = 6
        low_precision = config.fingerprint
        getcontext().prec = 34
        high_precision = config.fingerprint
    finally:
        getcontext().prec = original_precision

    assert low_precision == high_precision


@pytest.mark.parametrize(
    "value",
    [
        Decimal("12345678901234567890123456789012345"),
        Decimal("1e7000"),
        Decimal("NaN"),
        Decimal("Infinity"),
    ],
)
def test_execution_decimals_are_resource_bounded(value: Decimal) -> None:
    with pytest.raises(ValueError):
        SlippageConfig(SlippageMode.ABSOLUTE, value)


def test_execution_config_strings_are_bounded() -> None:
    with pytest.raises(ValueError, match="at most 128"):
        replace(DEFAULT_EXECUTION_CONFIG, algorithm_version="x" * 129)
    with pytest.raises(ValueError, match="at most 128"):
        fee_rule(policy_id="x" * 129)
