from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import FrozenInstanceError
from decimal import Decimal
from pathlib import Path
from traceback import format_exception
from uuid import UUID

import pytest

from tx_trade.app.research_paper_config import (
    ResearchPaperConfigError,
    ResearchRestartMode,
    ResearchPaperSettings,
    parse_research_paper_settings,
)
from tx_trade.orders import (
    FeePolicyKind,
    OrderSide,
    OrderType,
    SlippageMode,
    TimeInForce,
)
from tx_trade.replay import ReplayMode


SESSION_ID = "11111111-1111-1111-1111-111111111111"
RUN_ID = "22222222-2222-2222-2222-222222222222"


def _required() -> dict[str, str]:
    return {
        "TX_TRADE_RUNTIME_PRESET": "research_paper",
        "TX_TRADE_RESEARCH_PAPER_DB_PATH": "recordings.sqlite3",
        "TX_TRADE_RESEARCH_PAPER_SESSION_ID": SESSION_ID,
        "TX_TRADE_RESEARCH_PAPER_REPLAY_MODE": "fastest",
        "TX_TRADE_RESEARCH_PAPER_REPLAY_SPEED": "1.0",
        "TX_TRADE_RESEARCH_PAPER_RUN_ID": RUN_ID,
        "TX_TRADE_RESEARCH_PAPER_MAX_ORDERS": "10000",
        "TX_TRADE_RESEARCH_PAPER_MAX_OPEN_ORDERS": "10000",
        "TX_TRADE_RESEARCH_PAPER_MAX_FILLS": "100000",
        "TX_TRADE_RESEARCH_PAPER_MAX_EVENTS": "400000",
        "TX_TRADE_RESEARCH_PAPER_MAX_MARKET_DATA_RECORDS": "1000000",
        "TX_TRADE_RESEARCH_PAPER_MAX_INSTRUMENT_VERSIONS": "100000",
        "TX_TRADE_RESEARCH_PAPER_MAX_POSITIONS": "10000",
        "TX_TRADE_RESEARCH_PAPER_MAX_DECISION_RECORDS": "1000000",
        "TX_TRADE_RESEARCH_PAPER_SLIPPAGE_MODE": "none",
        "TX_TRADE_RESEARCH_PAPER_SLIPPAGE_VALUE": "0",
        "TX_TRADE_RESEARCH_PAPER_FEE_POLICY": "zero",
        "TX_TRADE_RESEARCH_PAPER_STRATEGY_ID": "alpha",
        "TX_TRADE_RESEARCH_PAPER_CLIENT_ORDER_ID": "entry-1",
        "TX_TRADE_RESEARCH_PAPER_ACCOUNT_ID": "paper",
        "TX_TRADE_RESEARCH_PAPER_INSTRUMENT_ID": "TAIFEX:0:TX00",
        "TX_TRADE_RESEARCH_PAPER_ORDER_SIDE": "buy",
        "TX_TRADE_RESEARCH_PAPER_ORDER_QUANTITY": "2",
        "TX_TRADE_RESEARCH_PAPER_ORDER_TYPE": "market",
        "TX_TRADE_RESEARCH_PAPER_TIME_IN_FORCE": "day",
        "TX_TRADE_RESEARCH_PAPER_DAY_TRADE": "0",
    }


def test_minimal_settings_are_complete_immutable_and_safe() -> None:
    settings = parse_research_paper_settings(_required())

    assert settings.runtime_preset == "research_paper"
    assert settings.execution_mode == "paper"
    assert settings.database_path == Path("recordings.sqlite3")
    assert settings.session_id == UUID(SESSION_ID)
    assert settings.paper_run_id == UUID(RUN_ID)
    assert settings.options.mode is ReplayMode.FASTEST
    assert settings.options.speed == 1.0
    assert settings.options.after_ingest_sequence is None
    assert settings.limits.max_orders == 10_000
    assert settings.limits.max_positions == 10_000
    assert settings.max_decision_records == 1_000_000
    assert settings.execution_config.slippage.mode is SlippageMode.NONE
    assert settings.execution_config.fee_schedule.kind is FeePolicyKind.ZERO
    assert settings.order_template.side is OrderSide.BUY
    assert settings.order_template.quantity == Decimal("2")
    assert settings.order_template.order_type is OrderType.MARKET
    assert settings.order_template.time_in_force is TimeInForce.DAY
    assert not settings.order_template.day_trade
    assert settings.restart_mode is ResearchRestartMode.DISABLED
    assert settings.state_database_path is None
    assert settings.max_state_main_database_bytes is None
    with pytest.raises(FrozenInstanceError):
        settings.execution_mode = "live"


def test_parses_all_limits_paced_execution_fee_and_limit_order() -> None:
    values = {
        **_required(),
        "TX_TRADE_RESEARCH_PAPER_REPLAY_MODE": "paced",
        "TX_TRADE_RESEARCH_PAPER_REPLAY_SPEED": "2.5",
        "TX_TRADE_RESEARCH_PAPER_MAX_ORDERS": "9",
        "TX_TRADE_RESEARCH_PAPER_MAX_OPEN_ORDERS": "8",
        "TX_TRADE_RESEARCH_PAPER_MAX_FILLS": "20",
        "TX_TRADE_RESEARCH_PAPER_MAX_EVENTS": "70",
        "TX_TRADE_RESEARCH_PAPER_MAX_MARKET_DATA_RECORDS": "100",
        "TX_TRADE_RESEARCH_PAPER_MAX_INSTRUMENT_VERSIONS": "10",
        "TX_TRADE_RESEARCH_PAPER_MAX_POSITIONS": "7",
        "TX_TRADE_RESEARCH_PAPER_MAX_DECISION_RECORDS": "100",
        "TX_TRADE_RESEARCH_PAPER_SLIPPAGE_MODE": "basis_points",
        "TX_TRADE_RESEARCH_PAPER_SLIPPAGE_VALUE": "10",
        "TX_TRADE_RESEARCH_PAPER_FEE_POLICY": "per_unit",
        "TX_TRADE_RESEARCH_PAPER_FEE_INSTRUMENT_ID": "TAIFEX:0:TX00",
        "TX_TRADE_RESEARCH_PAPER_FEE_CURRENCY": "TWD",
        "TX_TRADE_RESEARCH_PAPER_FEE_AMOUNT_PER_UNIT": "0.6",
        "TX_TRADE_RESEARCH_PAPER_FEE_QUANTUM": "0.01",
        "TX_TRADE_RESEARCH_PAPER_FEE_ROUNDING_MODE": "round_half_up",
        "TX_TRADE_RESEARCH_PAPER_FEE_POLICY_ID": "research-fee",
        "TX_TRADE_RESEARCH_PAPER_FEE_POLICY_VERSION": "1",
        "TX_TRADE_RESEARCH_PAPER_ORDER_TYPE": "limit",
        "TX_TRADE_RESEARCH_PAPER_LIMIT_PRICE": "20002",
        "TX_TRADE_RESEARCH_PAPER_TIME_IN_FORCE": "day",
        "TX_TRADE_RESEARCH_PAPER_DAY_TRADE": "1",
    }

    settings = parse_research_paper_settings(values)

    assert settings.options.mode is ReplayMode.PACED
    assert settings.options.speed == 2.5
    assert settings.limits.max_orders == 9
    assert settings.limits.max_open_orders == 8
    assert settings.limits.max_fills == 20
    assert settings.limits.max_events == 70
    assert settings.limits.max_market_data_records == 100
    assert settings.limits.max_instrument_versions == 10
    assert settings.limits.max_positions == 7
    assert settings.max_decision_records == 100
    assert settings.execution_config.slippage.mode is SlippageMode.BASIS_POINTS
    assert settings.execution_config.slippage.value == Decimal("10")
    fee = settings.execution_config.fee_schedule.rules[0]
    assert fee.instrument_id == "TAIFEX:0:TX00"
    assert fee.currency == "TWD"
    assert fee.amount_per_unit == Decimal("0.6")
    assert fee.quantum == Decimal("0.01")
    assert settings.order_template.order_type is OrderType.LIMIT
    assert settings.order_template.limit_price == Decimal("20002")
    assert settings.order_template.day_trade


@pytest.mark.parametrize("cursor", ["0", "1", "", "-1"])
def test_any_cursor_is_rejected_without_broker_checkpoint(cursor: str) -> None:
    values = {
        **_required(),
        "TX_TRADE_RESEARCH_PAPER_REPLAY_AFTER_INGEST_SEQUENCE": cursor,
    }

    with pytest.raises(ResearchPaperConfigError, match="checkpoint"):
        parse_research_paper_settings(values)


@pytest.mark.parametrize("mode", ["create", "resume"])
def test_restart_modes_require_and_parse_state_settings(mode: str) -> None:
    values = {
        **_required(),
        "TX_TRADE_RESEARCH_PAPER_RESTART_MODE": mode,
        "TX_TRADE_RESEARCH_PAPER_STATE_DB_PATH": "paper-state.sqlite3",
        "TX_TRADE_RESEARCH_PAPER_MAX_STATE_MAIN_DB_BYTES": "50000000",
    }

    settings = parse_research_paper_settings(values)

    assert settings.restart_mode is ResearchRestartMode(mode)
    assert settings.state_database_path == Path("paper-state.sqlite3")
    assert settings.max_state_main_database_bytes == 50_000_000


@pytest.mark.parametrize(
    "mode",
    ["disabled", "create", "resume"],
)
def test_raw_cursor_remains_rejected_in_every_restart_mode(mode: str) -> None:
    values = {
        **_required(),
        "TX_TRADE_RESEARCH_PAPER_RESTART_MODE": mode,
        "TX_TRADE_RESEARCH_PAPER_REPLAY_AFTER_INGEST_SEQUENCE": "0",
    }
    if mode != "disabled":
        values["TX_TRADE_RESEARCH_PAPER_STATE_DB_PATH"] = "paper-state.sqlite3"
        values["TX_TRADE_RESEARCH_PAPER_MAX_STATE_MAIN_DB_BYTES"] = "50000000"

    with pytest.raises(ResearchPaperConfigError, match="checkpoint"):
        parse_research_paper_settings(values)


@pytest.mark.parametrize("missing_suffix", ["STATE_DB_PATH", "MAX_STATE_MAIN_DB_BYTES"])
def test_enabled_restart_requires_all_state_settings(missing_suffix: str) -> None:
    values = {
        **_required(),
        "TX_TRADE_RESEARCH_PAPER_RESTART_MODE": "create",
        "TX_TRADE_RESEARCH_PAPER_STATE_DB_PATH": "paper-state.sqlite3",
        "TX_TRADE_RESEARCH_PAPER_MAX_STATE_MAIN_DB_BYTES": "50000000",
    }
    del values[f"TX_TRADE_RESEARCH_PAPER_{missing_suffix}"]

    with pytest.raises(ResearchPaperConfigError, match=missing_suffix):
        parse_research_paper_settings(values)


@pytest.mark.parametrize("suffix", ["STATE_DB_PATH", "MAX_STATE_MAIN_DB_BYTES"])
def test_disabled_restart_rejects_state_settings(suffix: str) -> None:
    value = "paper-state.sqlite3" if suffix == "STATE_DB_PATH" else "50000000"
    values = {
        **_required(),
        "TX_TRADE_RESEARCH_PAPER_RESTART_MODE": "disabled",
        f"TX_TRADE_RESEARCH_PAPER_{suffix}": value,
    }

    with pytest.raises(ResearchPaperConfigError, match=suffix):
        parse_research_paper_settings(values)


def test_ambiguous_legacy_state_database_limit_key_is_unknown() -> None:
    values = {
        **_required(),
        "TX_TRADE_RESEARCH_PAPER_MAX_STATE_DB_BYTES": "50000000",
    }

    with pytest.raises(
        ResearchPaperConfigError,
        match="unknown research paper setting.*MAX_STATE_DB_BYTES",
    ):
        parse_research_paper_settings(values)


def test_research_fingerprint_is_semantic_not_operational() -> None:
    baseline = parse_research_paper_settings(_required())
    operational = parse_research_paper_settings(
        {
            **_required(),
            "TX_TRADE_RESEARCH_PAPER_DB_PATH": "elsewhere.sqlite3",
            "TX_TRADE_RESEARCH_PAPER_REPLAY_MODE": "paced",
            "TX_TRADE_RESEARCH_PAPER_REPLAY_SPEED": "9.5",
            "TX_TRADE_RESEARCH_PAPER_RESTART_MODE": "create",
            "TX_TRADE_RESEARCH_PAPER_STATE_DB_PATH": "state.sqlite3",
            "TX_TRADE_RESEARCH_PAPER_MAX_STATE_MAIN_DB_BYTES": "999999",
        }
    )

    assert baseline.research_config_fingerprint == operational.research_config_fingerprint
    assert baseline.research_config_fingerprint.startswith("sha256:")
    assert len(baseline.research_config_fingerprint) == 71


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("TX_TRADE_RESEARCH_PAPER_MAX_EVENTS", "399999"),
        ("TX_TRADE_RESEARCH_PAPER_SLIPPAGE_MODE", "absolute"),
        ("TX_TRADE_RESEARCH_PAPER_ACCOUNT_ID", "another-paper-account"),
        ("TX_TRADE_RESEARCH_PAPER_ORDER_QUANTITY", "3"),
    ],
)
def test_research_fingerprint_changes_with_semantics(key: str, value: str) -> None:
    baseline = parse_research_paper_settings(_required())
    changed_values = {**_required(), key: value}
    if key.endswith("SLIPPAGE_MODE"):
        changed_values["TX_TRADE_RESEARCH_PAPER_SLIPPAGE_VALUE"] = "1"

    changed = parse_research_paper_settings(changed_values)

    assert baseline.research_config_fingerprint != changed.research_config_fingerprint


def test_research_fingerprint_does_not_contain_live_secrets() -> None:
    values = {
        **_required(),
        "TX_TRADE_ACCOUNT": "live-account-canary",
        "TX_TRADE_PASSWORD": "password-canary",
        "TX_TRADE_SKCOM_DLL_PATH": "dll-canary",
    }

    fingerprint = parse_research_paper_settings(values).research_config_fingerprint

    assert "canary" not in fingerprint


@pytest.mark.parametrize(
    "missing",
    [
        "TX_TRADE_RESEARCH_PAPER_DB_PATH",
        "TX_TRADE_RESEARCH_PAPER_SESSION_ID",
        "TX_TRADE_RESEARCH_PAPER_REPLAY_MODE",
        "TX_TRADE_RESEARCH_PAPER_REPLAY_SPEED",
        "TX_TRADE_RESEARCH_PAPER_RUN_ID",
        "TX_TRADE_RESEARCH_PAPER_MAX_ORDERS",
        "TX_TRADE_RESEARCH_PAPER_MAX_OPEN_ORDERS",
        "TX_TRADE_RESEARCH_PAPER_MAX_FILLS",
        "TX_TRADE_RESEARCH_PAPER_MAX_EVENTS",
        "TX_TRADE_RESEARCH_PAPER_MAX_MARKET_DATA_RECORDS",
        "TX_TRADE_RESEARCH_PAPER_MAX_INSTRUMENT_VERSIONS",
        "TX_TRADE_RESEARCH_PAPER_MAX_POSITIONS",
        "TX_TRADE_RESEARCH_PAPER_MAX_DECISION_RECORDS",
        "TX_TRADE_RESEARCH_PAPER_SLIPPAGE_MODE",
        "TX_TRADE_RESEARCH_PAPER_SLIPPAGE_VALUE",
        "TX_TRADE_RESEARCH_PAPER_FEE_POLICY",
        "TX_TRADE_RESEARCH_PAPER_STRATEGY_ID",
        "TX_TRADE_RESEARCH_PAPER_CLIENT_ORDER_ID",
        "TX_TRADE_RESEARCH_PAPER_ACCOUNT_ID",
        "TX_TRADE_RESEARCH_PAPER_INSTRUMENT_ID",
        "TX_TRADE_RESEARCH_PAPER_ORDER_SIDE",
        "TX_TRADE_RESEARCH_PAPER_ORDER_QUANTITY",
        "TX_TRADE_RESEARCH_PAPER_ORDER_TYPE",
        "TX_TRADE_RESEARCH_PAPER_TIME_IN_FORCE",
        "TX_TRADE_RESEARCH_PAPER_DAY_TRADE",
    ],
)
def test_required_values_are_enforced(missing: str) -> None:
    values = _required()
    del values[missing]

    with pytest.raises(ResearchPaperConfigError, match=missing):
        parse_research_paper_settings(values)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("TX_TRADE_RUNTIME_PRESET", "phase2_replay"),
        ("TX_TRADE_RESEARCH_PAPER_SESSION_ID", "not-a-uuid"),
        ("TX_TRADE_RESEARCH_PAPER_RUN_ID", "NOT-A-UUID"),
        ("TX_TRADE_RESEARCH_PAPER_REPLAY_SPEED", "nan"),
        ("TX_TRADE_RESEARCH_PAPER_REPLAY_SPEED", "0"),
        ("TX_TRADE_RESEARCH_PAPER_MAX_ORDERS", "0"),
        ("TX_TRADE_RESEARCH_PAPER_MAX_EVENTS", "-1"),
        ("TX_TRADE_RESEARCH_PAPER_ORDER_QUANTITY", "NaN"),
        ("TX_TRADE_RESEARCH_PAPER_ORDER_QUANTITY", "1e2"),
        ("TX_TRADE_RESEARCH_PAPER_ORDER_SIDE", "BUY"),
        ("TX_TRADE_RESEARCH_PAPER_DAY_TRADE", "true"),
    ],
)
def test_invalid_values_fail_closed(key: str, value: str) -> None:
    values = {**_required(), key: value}

    with pytest.raises(ResearchPaperConfigError):
        parse_research_paper_settings(values)


def test_oversized_integer_is_rejected_before_conversion_without_value_leak() -> None:
    oversized = "9" * 5000
    values = {
        **_required(),
        "TX_TRADE_RESEARCH_PAPER_MAX_EVENTS": oversized,
    }

    with pytest.raises(ResearchPaperConfigError) as caught:
        parse_research_paper_settings(values)

    assert oversized not in str(caught.value)


def test_limit_order_requires_price_and_market_rejects_price() -> None:
    limit_values = {
        **_required(),
        "TX_TRADE_RESEARCH_PAPER_ORDER_TYPE": "limit",
    }
    with pytest.raises(ResearchPaperConfigError, match="LIMIT_PRICE"):
        parse_research_paper_settings(limit_values)

    market_values = {
        **_required(),
        "TX_TRADE_RESEARCH_PAPER_LIMIT_PRICE": "1",
    }
    with pytest.raises(ResearchPaperConfigError, match="LIMIT_PRICE"):
        parse_research_paper_settings(market_values)


def test_per_unit_fee_requires_complete_rule() -> None:
    values = {
        **_required(),
        "TX_TRADE_RESEARCH_PAPER_FEE_POLICY": "per_unit",
    }

    with pytest.raises(
        ResearchPaperConfigError,
        match="FEE_CURRENCY|FEE_INSTRUMENT_ID",
    ):
        parse_research_paper_settings(values)


@pytest.mark.parametrize(
    "detail_key",
    [
        "TX_TRADE_RESEARCH_PAPER_FEE_INSTRUMENT_ID",
        "TX_TRADE_RESEARCH_PAPER_FEE_CURRENCY",
        "TX_TRADE_RESEARCH_PAPER_FEE_AMOUNT_PER_UNIT",
        "TX_TRADE_RESEARCH_PAPER_FEE_QUANTUM",
        "TX_TRADE_RESEARCH_PAPER_FEE_ROUNDING_MODE",
        "TX_TRADE_RESEARCH_PAPER_FEE_POLICY_ID",
        "TX_TRADE_RESEARCH_PAPER_FEE_POLICY_VERSION",
    ],
)
def test_zero_fee_rejects_inapplicable_fee_details(detail_key: str) -> None:
    values = {**_required(), detail_key: "unused-secret-value"}

    with pytest.raises(ResearchPaperConfigError, match=detail_key) as caught:
        parse_research_paper_settings(values)

    assert "unused-secret-value" not in str(caught.value)


def test_unknown_research_paper_key_fails_closed_without_rendering_value() -> None:
    unknown_key = "TX_TRADE_RESEARCH_PAPER_MAX_EVENTZ"
    canary = "unknown-setting-secret-canary"
    values = {**_required(), unknown_key: canary}

    with pytest.raises(ResearchPaperConfigError, match=unknown_key) as caught:
        parse_research_paper_settings(values)

    assert canary not in str(caught.value)


def test_direct_construction_rejects_cursor() -> None:
    valid = parse_research_paper_settings(_required())
    with pytest.raises(ResearchPaperConfigError, match="checkpoint"):
        ResearchPaperSettings(
            runtime_preset=valid.runtime_preset,
            execution_mode=valid.execution_mode,
            database_path=valid.database_path,
            session_id=valid.session_id,
            options=type(valid.options)(
                mode=valid.options.mode,
                speed=valid.options.speed,
                after_ingest_sequence=0,
            ),
            paper_run_id=valid.paper_run_id,
            limits=valid.limits,
            execution_config=valid.execution_config,
            max_decision_records=valid.max_decision_records,
            order_template=valid.order_template,
        )


def test_non_mapping_and_non_string_values_are_rejected() -> None:
    with pytest.raises(ResearchPaperConfigError, match="mapping"):
        parse_research_paper_settings(None)  # type: ignore[arg-type]
    values = _required()
    values["TX_TRADE_RESEARCH_PAPER_DB_PATH"] = True  # type: ignore[assignment]
    with pytest.raises(ResearchPaperConfigError, match="DB_PATH"):
        parse_research_paper_settings(values)


def test_sensitive_invalid_value_is_not_exposed_by_exception_chain() -> None:
    canary = "sensitive-research-paper-canary"
    values = {
        **_required(),
        "TX_TRADE_RESEARCH_PAPER_REPLAY_SPEED": canary,
    }

    with pytest.raises(ResearchPaperConfigError) as caught:
        parse_research_paper_settings(values)

    rendered = "".join(
        format_exception(type(caught.value), caught.value, caught.value.__traceback__)
    )
    assert canary not in rendered
    assert caught.value.__cause__ is None


class _AccessTrackingMapping(Mapping[str, str]):
    def __init__(self, values: dict[str, str]) -> None:
        self._values = values
        self.accessed: list[str] = []

    def __getitem__(self, key: str) -> str:
        self.accessed.append(key)
        return self._values[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def get(self, key: str, default=None):
        self.accessed.append(key)
        return self._values.get(key, default)


def test_parser_reads_only_research_paper_allowlist() -> None:
    values = _AccessTrackingMapping(
        {
            **_required(),
            "TX_TRADE_ACCOUNT": "account-canary",
            "TX_TRADE_PASSWORD": "password-canary",
            "TX_TRADE_SKCOM_DLL_PATH": "dll-canary",
            "TX_TRADE_REPLAY_DB_PATH": "legacy-canary",
            "TX_TRADE_EXECUTION_MODE": "live",
        }
    )

    settings = parse_research_paper_settings(values)

    assert settings.execution_mode == "paper"
    assert all(
        key == "TX_TRADE_RUNTIME_PRESET" or key.startswith("TX_TRADE_RESEARCH_PAPER_")
        for key in values.accessed
    )
    assert not {
        "TX_TRADE_ACCOUNT",
        "TX_TRADE_PASSWORD",
        "TX_TRADE_SKCOM_DLL_PATH",
        "TX_TRADE_REPLAY_DB_PATH",
        "TX_TRADE_EXECUTION_MODE",
    }.intersection(values.accessed)


def test_parser_does_not_access_database_or_environment(monkeypatch) -> None:
    monkeypatch.setattr(
        "builtins.open",
        lambda *args, **kwargs: pytest.fail("unexpected filesystem access"),
    )

    settings = parse_research_paper_settings(_required())

    assert settings.database_path == Path("recordings.sqlite3")
