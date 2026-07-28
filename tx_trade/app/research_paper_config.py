"""Pure, allowlisted configuration for deterministic research-paper replay."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from hashlib import sha256
import json
from pathlib import Path
from typing import Literal, TypeVar
from uuid import UUID

from tx_trade.orders import (
    FeePolicyKind,
    FeeRoundingMode,
    OrderSide,
    OrderType,
    PaperBrokerLimits,
    PaperExecutionConfig,
    PaperFeeRule,
    PaperFeeSchedule,
    SlippageConfig,
    SlippageMode,
    TimeInForce,
)
from tx_trade.replay import ReplayMode, ReplayOptions
from tx_trade.strategy import OrderTemplate


class ResearchPaperConfigError(ValueError):
    """Raised when research-paper settings are missing or unsafe."""


class ResearchRestartMode(StrEnum):
    """Durability mode for a research-paper run."""

    DISABLED = "disabled"
    CREATE = "create"
    RESUME = "resume"


@dataclass(frozen=True, slots=True)
class ResearchPaperSettings:
    """Validated deterministic paper replay settings.

    ``max_state_main_database_bytes`` caps SQLite main-database logical pages.
    SQLite WAL and SHM sidecars are deliberately excluded from that limit.
    """

    runtime_preset: Literal["research_paper"]
    execution_mode: Literal["paper"]
    database_path: Path
    session_id: UUID
    options: ReplayOptions
    paper_run_id: UUID
    limits: PaperBrokerLimits
    execution_config: PaperExecutionConfig
    max_decision_records: int
    order_template: OrderTemplate
    restart_mode: ResearchRestartMode = ResearchRestartMode.DISABLED
    state_database_path: Path | None = None
    max_state_main_database_bytes: int | None = None

    def __post_init__(self) -> None:
        if self.runtime_preset != "research_paper":
            raise ResearchPaperConfigError("runtime_preset must be research_paper")
        if self.execution_mode != "paper":
            raise ResearchPaperConfigError("execution_mode must be paper")
        if not isinstance(self.database_path, Path):
            raise ResearchPaperConfigError("database_path must be a Path")
        if type(self.session_id) is not UUID:
            raise ResearchPaperConfigError("session_id must be a UUID")
        if type(self.options) is not ReplayOptions:
            raise ResearchPaperConfigError("options must be ReplayOptions")
        if self.options.after_ingest_sequence is not None:
            raise ResearchPaperConfigError(
                "research_paper replay cursor requires a broker checkpoint"
            )
        if type(self.paper_run_id) is not UUID:
            raise ResearchPaperConfigError("paper_run_id must be a UUID")
        if type(self.limits) is not PaperBrokerLimits:
            raise ResearchPaperConfigError("limits must be PaperBrokerLimits")
        if type(self.execution_config) is not PaperExecutionConfig:
            raise ResearchPaperConfigError("execution_config must be PaperExecutionConfig")
        if type(self.max_decision_records) is not int or self.max_decision_records < 1:
            raise ResearchPaperConfigError("max_decision_records must be a positive integer")
        if type(self.order_template) is not OrderTemplate:
            raise ResearchPaperConfigError("order_template must be OrderTemplate")
        if type(self.restart_mode) is not ResearchRestartMode:
            raise ResearchPaperConfigError("restart_mode must be ResearchRestartMode")
        if self.restart_mode is ResearchRestartMode.DISABLED:
            if self.state_database_path is not None:
                raise ResearchPaperConfigError(
                    "state_database_path must be absent when restart is disabled"
                )
            if self.max_state_main_database_bytes is not None:
                raise ResearchPaperConfigError(
                    "max_state_main_database_bytes must be absent when restart is disabled"
                )
        else:
            if not isinstance(self.state_database_path, Path):
                raise ResearchPaperConfigError(
                    "state_database_path must be a Path when restart is enabled"
                )
            if (
                type(self.max_state_main_database_bytes) is not int
                or self.max_state_main_database_bytes < 1
                or self.max_state_main_database_bytes > 1_000_000_000
            ):
                raise ResearchPaperConfigError(
                    "max_state_main_database_bytes must be between 1 and 1000000000; "
                    "it caps SQLite main-database logical pages and excludes WAL/SHM"
                )

    @property
    def research_config_fingerprint(self) -> str:
        """Return a pacing/path-independent fingerprint of paper semantics."""

        payload = {
            "broker_limits": {
                "max_events": self.limits.max_events,
                "max_fills": self.limits.max_fills,
                "max_instrument_versions": self.limits.max_instrument_versions,
                "max_market_data_records": self.limits.max_market_data_records,
                "max_open_orders": self.limits.max_open_orders,
                "max_orders": self.limits.max_orders,
                "max_positions": self.limits.max_positions,
            },
            "execution_config_fingerprint": self.execution_config.fingerprint,
            "output": {
                "algorithm_version": "research-jsonl-v1",
                "schema_version": 1,
            },
            "strategy": self._strategy_material(),
        }
        encoded = json.dumps(
            payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
        return f"sha256:{sha256(b'tx_trade.research.config.v1\\0' + encoded).hexdigest()}"

    @property
    def strategy_fingerprint(self) -> str:
        """Return the configured strategy fingerprint for run identity composition."""

        encoded = json.dumps(
            self._strategy_material(),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        marker = b"tx_trade.research.strategy.instrument_triggered_order.v1\0"
        return f"sha256:{sha256(marker + encoded).hexdigest()}"

    def _strategy_material(self) -> dict[str, object]:
        template = self.order_template
        return {
            "kind": "instrument-triggered-order",
            "max_decision_records": self.max_decision_records,
            "parameters": {
                "account_id": template.account_id,
                "client_order_id": template.client_order_id,
                "day_trade": template.day_trade,
                "instrument_id": template.instrument_id,
                "limit_price": (
                    None
                    if template.limit_price is None
                    else _semantic_decimal(template.limit_price)
                ),
                "order_type": template.order_type.value,
                "quantity": _semantic_decimal(template.quantity),
                "side": template.side.value,
                "strategy_id": template.strategy_id,
                "time_in_force": template.time_in_force.value,
            },
            "version": "instrument-triggered-order-v1",
        }


def _semantic_decimal(value: Decimal) -> str:
    normalized = value.normalize()
    rendered = format(normalized, "f")
    return "0" if rendered in {"-0", ""} else rendered


_PREFIX = "TX_TRADE_RESEARCH_PAPER_"
_MISSING = object()
_DECIMAL_PATTERN = re.compile(r"(?:0|[1-9][0-9]*)(?:\.[0-9]+)?\Z", re.ASCII)
_LIMIT_SUFFIXES = (
    "MAX_ORDERS",
    "MAX_OPEN_ORDERS",
    "MAX_FILLS",
    "MAX_EVENTS",
    "MAX_MARKET_DATA_RECORDS",
    "MAX_INSTRUMENT_VERSIONS",
    "MAX_POSITIONS",
    "MAX_DECISION_RECORDS",
)
_FEE_DETAIL_SUFFIXES = (
    "FEE_INSTRUMENT_ID",
    "FEE_CURRENCY",
    "FEE_AMOUNT_PER_UNIT",
    "FEE_QUANTUM",
    "FEE_ROUNDING_MODE",
    "FEE_POLICY_ID",
    "FEE_POLICY_VERSION",
)
_ALLOWED_SUFFIXES = frozenset(
    (
        "DB_PATH",
        "SESSION_ID",
        "REPLAY_MODE",
        "REPLAY_SPEED",
        "REPLAY_AFTER_INGEST_SEQUENCE",
        "RUN_ID",
        "SLIPPAGE_MODE",
        "SLIPPAGE_VALUE",
        "FEE_POLICY",
        "STRATEGY_ID",
        "CLIENT_ORDER_ID",
        "ACCOUNT_ID",
        "INSTRUMENT_ID",
        "ORDER_SIDE",
        "ORDER_QUANTITY",
        "ORDER_TYPE",
        "LIMIT_PRICE",
        "TIME_IN_FORCE",
        "DAY_TRADE",
        "RESTART_MODE",
        "STATE_DB_PATH",
        "MAX_STATE_MAIN_DB_BYTES",
        *_LIMIT_SUFFIXES,
        *_FEE_DETAIL_SUFFIXES,
    )
)
_StrEnumT = TypeVar("_StrEnumT", bound=StrEnum)


def _key(suffix: str) -> str:
    return f"{_PREFIX}{suffix}"


_ALLOWED_KEYS = frozenset(_key(suffix) for suffix in _ALLOWED_SUFFIXES)


def _read(
    values: Mapping[str, str],
    suffix: str,
    *,
    default: str | None = None,
    required: bool = False,
) -> str | None:
    key = _key(suffix)
    raw = values.get(key, _MISSING)
    if raw is _MISSING:
        if required:
            raise ResearchPaperConfigError(f"{key} is required")
        return default
    if type(raw) is not str:
        raise ResearchPaperConfigError(f"{key} must be a string")
    if not raw or raw != raw.strip():
        raise ResearchPaperConfigError(f"{key} must be non-empty without outer whitespace")
    return raw


def _identifier(values: Mapping[str, str], suffix: str) -> str:
    raw = _read(values, suffix, required=True)
    assert raw is not None
    if len(raw) > 128:
        raise ResearchPaperConfigError(f"{_key(suffix)} must be at most 128 characters")
    return raw


def _uuid(values: Mapping[str, str], suffix: str) -> UUID:
    raw = _read(values, suffix, required=True)
    assert raw is not None
    try:
        parsed = UUID(raw)
    except (AttributeError, TypeError, ValueError):
        raise ResearchPaperConfigError(f"{_key(suffix)} must be a UUID") from None
    if str(parsed) != raw:
        raise ResearchPaperConfigError(f"{_key(suffix)} must be a canonical lowercase UUID")
    return parsed


def _positive_int(
    values: Mapping[str, str],
    suffix: str,
) -> int:
    raw = _read(values, suffix, required=True)
    assert raw is not None
    if len(raw) > 10 or not raw.isascii() or not raw.isdecimal():
        raise ResearchPaperConfigError(f"{_key(suffix)} must be ASCII decimal digits")
    try:
        result = int(raw)
    except ValueError:
        raise ResearchPaperConfigError(f"{_key(suffix)} must be ASCII decimal digits") from None
    if result < 1 or result > 1_000_000_000:
        raise ResearchPaperConfigError(f"{_key(suffix)} must be between 1 and 1000000000")
    return result


def _decimal(
    values: Mapping[str, str],
    suffix: str,
    *,
    default: str | None = None,
    required: bool = False,
    positive: bool = False,
) -> Decimal | None:
    raw = _read(values, suffix, default=default, required=required)
    if raw is None:
        return None
    if len(raw) > 64 or _DECIMAL_PATTERN.fullmatch(raw) is None:
        raise ResearchPaperConfigError(f"{_key(suffix)} must be a bounded non-negative decimal")
    result = Decimal(raw)
    parts = result.as_tuple()
    exponent = parts.exponent
    assert isinstance(exponent, int)
    if len(parts.digits) > 34 or exponent < -6143:
        raise ResearchPaperConfigError(f"{_key(suffix)} exceeds Decimal bounds")
    if positive and result <= 0:
        raise ResearchPaperConfigError(f"{_key(suffix)} must be greater than zero")
    return result


def _enum(
    values: Mapping[str, str],
    suffix: str,
    enum_type: type[_StrEnumT],
    *,
    default: str | None = None,
) -> _StrEnumT:
    raw = _read(values, suffix, default=default, required=default is None)
    assert raw is not None
    try:
        return enum_type(raw)
    except (TypeError, ValueError):
        allowed = ", ".join(item.value for item in enum_type)
        raise ResearchPaperConfigError(f"{_key(suffix)} must be one of: {allowed}") from None


def _replay_options(values: Mapping[str, str]) -> ReplayOptions:
    cursor_key = _key("REPLAY_AFTER_INGEST_SEQUENCE")
    if values.get(cursor_key, _MISSING) is not _MISSING:
        raise ResearchPaperConfigError(f"{cursor_key} requires a broker checkpoint")
    mode = _enum(values, "REPLAY_MODE", ReplayMode)
    speed_raw = _read(values, "REPLAY_SPEED", required=True)
    assert speed_raw is not None
    try:
        speed = float(speed_raw)
        return ReplayOptions(mode=mode, speed=speed)
    except (TypeError, ValueError):
        raise ResearchPaperConfigError(
            f"{_key('REPLAY_SPEED')} must be finite and positive"
        ) from None


def _limits(values: Mapping[str, str]) -> tuple[PaperBrokerLimits, int]:
    parsed = {suffix: _positive_int(values, suffix) for suffix in _LIMIT_SUFFIXES}
    try:
        limits = PaperBrokerLimits(
            max_orders=parsed["MAX_ORDERS"],
            max_open_orders=parsed["MAX_OPEN_ORDERS"],
            max_fills=parsed["MAX_FILLS"],
            max_events=parsed["MAX_EVENTS"],
            max_market_data_records=parsed["MAX_MARKET_DATA_RECORDS"],
            max_instrument_versions=parsed["MAX_INSTRUMENT_VERSIONS"],
            max_positions=parsed["MAX_POSITIONS"],
        )
    except (TypeError, ValueError):
        raise ResearchPaperConfigError("research paper limits are invalid") from None
    return limits, parsed["MAX_DECISION_RECORDS"]


def _execution_config(values: Mapping[str, str]) -> PaperExecutionConfig:
    slippage_mode = _enum(
        values,
        "SLIPPAGE_MODE",
        SlippageMode,
    )
    slippage_value = _decimal(
        values,
        "SLIPPAGE_VALUE",
        required=True,
        positive=slippage_mode is not SlippageMode.NONE,
    )
    assert slippage_value is not None
    fee_kind = _enum(
        values,
        "FEE_POLICY",
        FeePolicyKind,
    )
    rules: tuple[PaperFeeRule, ...] = ()
    present_fee_details = tuple(suffix for suffix in _FEE_DETAIL_SUFFIXES if _key(suffix) in values)
    if fee_kind is FeePolicyKind.ZERO and present_fee_details:
        raise ResearchPaperConfigError(f"{_key(present_fee_details[0])} is not valid for zero fees")
    if fee_kind is FeePolicyKind.PER_UNIT:
        currency = _read(values, "FEE_CURRENCY", required=True)
        assert currency is not None
        amount = _decimal(
            values,
            "FEE_AMOUNT_PER_UNIT",
            required=True,
            positive=True,
        )
        quantum = _decimal(
            values,
            "FEE_QUANTUM",
            required=True,
            positive=True,
        )
        assert amount is not None and quantum is not None
        rules = (
            PaperFeeRule(
                instrument_id=_identifier(values, "FEE_INSTRUMENT_ID"),
                currency=currency,
                amount_per_unit=amount,
                quantum=quantum,
                rounding_mode=_enum(
                    values,
                    "FEE_ROUNDING_MODE",
                    FeeRoundingMode,
                ),
                policy_id=_identifier(values, "FEE_POLICY_ID"),
                policy_version=_identifier(values, "FEE_POLICY_VERSION"),
            ),
        )
    try:
        return PaperExecutionConfig(
            slippage=SlippageConfig(mode=slippage_mode, value=slippage_value),
            fee_schedule=PaperFeeSchedule(kind=fee_kind, rules=rules),
        )
    except (TypeError, ValueError):
        raise ResearchPaperConfigError(
            "research paper execution configuration is invalid"
        ) from None


def _order_template(values: Mapping[str, str]) -> OrderTemplate:
    order_type = _enum(values, "ORDER_TYPE", OrderType)
    if order_type is OrderType.MARKET and _key("LIMIT_PRICE") in values:
        raise ResearchPaperConfigError(f"{_key('LIMIT_PRICE')} is not valid for market orders")
    limit_price = _decimal(
        values,
        "LIMIT_PRICE",
        required=order_type is OrderType.LIMIT,
        positive=True,
    )
    day_trade_raw = _read(values, "DAY_TRADE", required=True)
    assert day_trade_raw is not None
    if day_trade_raw not in {"0", "1"}:
        raise ResearchPaperConfigError(f"{_key('DAY_TRADE')} must be exactly '0' or '1'")
    quantity = _decimal(values, "ORDER_QUANTITY", required=True, positive=True)
    assert quantity is not None
    strategy_id = _identifier(values, "STRATEGY_ID")
    client_order_id = _identifier(values, "CLIENT_ORDER_ID")
    account_id = _identifier(values, "ACCOUNT_ID")
    instrument_id = _identifier(values, "INSTRUMENT_ID")
    side = _enum(values, "ORDER_SIDE", OrderSide)
    time_in_force = _enum(
        values,
        "TIME_IN_FORCE",
        TimeInForce,
    )
    try:
        return OrderTemplate(
            strategy_id=strategy_id,
            client_order_id=client_order_id,
            account_id=account_id,
            instrument_id=instrument_id,
            side=side,
            quantity=quantity,
            order_type=order_type,
            limit_price=limit_price,
            time_in_force=time_in_force,
            day_trade=day_trade_raw == "1",
        )
    except (TypeError, ValueError):
        raise ResearchPaperConfigError("research paper order template is invalid") from None


def parse_research_paper_settings(
    values: Mapping[str, str],
) -> ResearchPaperSettings:
    """Parse only the explicit research-paper allowlist without side effects."""

    if not isinstance(values, Mapping):
        raise ResearchPaperConfigError("values must be a mapping")
    for key in values:
        if type(key) is not str:
            raise ResearchPaperConfigError("configuration keys must be strings")
        if key.startswith(_PREFIX) and key not in _ALLOWED_KEYS:
            raise ResearchPaperConfigError(f"unknown research paper setting: {key}")
    preset = values.get("TX_TRADE_RUNTIME_PRESET", _MISSING)
    if type(preset) is not str or preset != "research_paper":
        raise ResearchPaperConfigError("TX_TRADE_RUNTIME_PRESET must be research_paper")
    database_raw = _read(values, "DB_PATH", required=True)
    assert database_raw is not None
    if len(database_raw) > 4096:
        raise ResearchPaperConfigError(f"{_key('DB_PATH')} must be at most 4096 characters")
    restart_mode = _enum(
        values,
        "RESTART_MODE",
        ResearchRestartMode,
        default=ResearchRestartMode.DISABLED.value,
    )
    state_database_path: Path | None = None
    max_state_main_database_bytes: int | None = None
    state_path_present = _key("STATE_DB_PATH") in values
    state_max_present = _key("MAX_STATE_MAIN_DB_BYTES") in values
    if restart_mode is ResearchRestartMode.DISABLED:
        if state_path_present:
            raise ResearchPaperConfigError(
                f"{_key('STATE_DB_PATH')} is not valid when restart is disabled"
            )
        if state_max_present:
            raise ResearchPaperConfigError(
                f"{_key('MAX_STATE_MAIN_DB_BYTES')} is not valid when restart is disabled"
            )
    else:
        state_raw = _read(values, "STATE_DB_PATH", required=True)
        assert state_raw is not None
        if len(state_raw) > 4096:
            raise ResearchPaperConfigError(
                f"{_key('STATE_DB_PATH')} must be at most 4096 characters"
            )
        state_database_path = Path(state_raw)
        max_state_main_database_bytes = _positive_int(values, "MAX_STATE_MAIN_DB_BYTES")
    limits, max_decision_records = _limits(values)
    return ResearchPaperSettings(
        runtime_preset="research_paper",
        execution_mode="paper",
        database_path=Path(database_raw),
        session_id=_uuid(values, "SESSION_ID"),
        options=_replay_options(values),
        paper_run_id=_uuid(values, "RUN_ID"),
        limits=limits,
        execution_config=_execution_config(values),
        max_decision_records=max_decision_records,
        order_template=_order_template(values),
        restart_mode=restart_mode,
        state_database_path=state_database_path,
        max_state_main_database_bytes=max_state_main_database_bytes,
    )
