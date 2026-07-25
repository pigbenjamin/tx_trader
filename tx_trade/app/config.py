"""Pure, fail-closed Phase 1 runtime configuration.

Supported keys:

* ``TX_TRADE_RUNTIME_PRESET``
* ``TX_TRADE_QUOTE_SOURCE``
* ``TX_TRADE_EXECUTION_MODE``
* ``TX_TRADE_ENABLE_LIVE_QUOTE``
* ``TX_TRADE_INGRESS_QUEUE_CAPACITY``
* ``TX_TRADE_INGRESS_CONNECTION_CAPACITY``
* ``TX_TRADE_INGRESS_DIAGNOSTIC_RESERVED_CAPACITY``
* ``TX_TRADE_INGRESS_QUOTE_CAPACITY``
* ``TX_TRADE_INGRESS_TICK_CAPACITY``
* ``TX_TRADE_INGRESS_DEDUPE_CAPACITY``
* ``TX_TRADE_STA_QUOTE_ENRICHMENT_CAPACITY``
* ``TX_TRADE_STORAGE_WRITER_QUEUE_CAPACITY``
* ``TX_TRADE_STORAGE_BATCH_SIZE``
* ``TX_TRADE_STORAGE_FLUSH_INTERVAL_MS``

This module deliberately does not read the environment or filesystem.  A
composition root may pass a previously captured mapping to
``parse_phase1_settings``.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum


class ConfigError(ValueError):
    """Raised when settings are invalid or unsafe for Phase 1."""


class QuoteSource(StrEnum):
    OFFLINE = "offline"
    REPLAY = "replay"
    LIVE = "live"


class ExecutionMode(StrEnum):
    DISABLED = "disabled"
    PAPER = "paper"
    LIVE = "live"


class RuntimePreset(StrEnum):
    PHASE1_DEFAULT = "phase1_default"
    PHASE2_REPLAY = "phase2_replay"
    PHASE1_LIVE_QUOTE = "phase1_live_quote"
    RESEARCH_PAPER = "research_paper"
    LIVE_TRADE = "live_trade"


_PRESET_MODES = {
    RuntimePreset.PHASE1_DEFAULT: (QuoteSource.OFFLINE, ExecutionMode.DISABLED),
    RuntimePreset.PHASE2_REPLAY: (QuoteSource.REPLAY, ExecutionMode.DISABLED),
    RuntimePreset.PHASE1_LIVE_QUOTE: (QuoteSource.LIVE, ExecutionMode.DISABLED),
    RuntimePreset.RESEARCH_PAPER: (QuoteSource.REPLAY, ExecutionMode.PAPER),
    RuntimePreset.LIVE_TRADE: (QuoteSource.LIVE, ExecutionMode.LIVE),
}


@dataclass(frozen=True, slots=True)
class Phase1Settings:
    preset: RuntimePreset
    quote_source: QuoteSource
    execution_mode: ExecutionMode
    live_quote_opt_in: bool
    ingress_queue_capacity: int
    ingress_control_capacity: int
    ingress_diagnostic_capacity: int
    ingress_quote_capacity: int
    ingress_tick_capacity: int
    ingress_dedupe_capacity: int
    sta_quote_enrichment_capacity: int
    storage_writer_queue_capacity: int
    storage_batch_size: int
    storage_flush_interval_ms: int

    def __post_init__(self) -> None:
        if type(self.preset) is not RuntimePreset:
            raise ConfigError("preset must be a RuntimePreset")
        if type(self.quote_source) is not QuoteSource:
            raise ConfigError("quote_source must be a QuoteSource")
        if type(self.execution_mode) is not ExecutionMode:
            raise ConfigError("execution_mode must be an ExecutionMode")
        if type(self.live_quote_opt_in) is not bool:
            raise ConfigError("live_quote_opt_in must be bool")
        for name in (
            "ingress_queue_capacity",
            "ingress_control_capacity",
            "ingress_diagnostic_capacity",
            "ingress_quote_capacity",
            "ingress_tick_capacity",
            "ingress_dedupe_capacity",
            "sta_quote_enrichment_capacity",
            "storage_writer_queue_capacity",
            "storage_batch_size",
            "storage_flush_interval_ms",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ConfigError(f"{name} must be a positive integer")
        lane_capacity = (
            self.ingress_control_capacity
            + self.ingress_diagnostic_capacity
            + self.ingress_quote_capacity
            + self.ingress_tick_capacity
        )
        if lane_capacity > self.ingress_queue_capacity:
            raise ConfigError(
                "ingress lane capacities must not exceed ingress_queue_capacity"
            )

        if (self.quote_source, self.execution_mode) != _PRESET_MODES[self.preset]:
            raise ConfigError("quote_source/execution_mode must match preset")
        if self.preset in {RuntimePreset.PHASE2_REPLAY, RuntimePreset.RESEARCH_PAPER}:
            raise ConfigError(f"{self.preset.value} is not a Phase 1 runtime preset")
        if self.preset is RuntimePreset.LIVE_TRADE:
            raise ConfigError("live_trade is forbidden in Phase 1")
        if self.quote_source is QuoteSource.REPLAY:
            raise ConfigError("replay quote source is not available in Phase 1")
        if self.execution_mode is not ExecutionMode.DISABLED:
            raise ConfigError(
                f"execution mode {self.execution_mode.value!r} is forbidden in Phase 1"
            )
        if self.quote_source is QuoteSource.LIVE and not self.live_quote_opt_in:
            raise ConfigError(
                "live quote requires TX_TRADE_ENABLE_LIVE_QUOTE=1"
            )


def _enum_value(enum_type: type[StrEnum], raw: str, key: str) -> StrEnum:
    try:
        return enum_type(raw)
    except (TypeError, ValueError) as exc:
        allowed = ", ".join(item.value for item in enum_type)
        raise ConfigError(f"{key} must be one of: {allowed}") from exc


def _positive_int(values: Mapping[str, str], key: str, default: int) -> int:
    raw = values.get(key)
    if raw is None:
        return default
    if type(raw) is not str or not raw or not raw.isascii() or not raw.isdecimal():
        raise ConfigError(f"{key} must be ASCII decimal digits")
    value = int(raw)
    if value <= 0:
        raise ConfigError(f"{key} must be a positive integer")
    return value


_LANE_KEYS = (
    "TX_TRADE_INGRESS_CONNECTION_CAPACITY",
    "TX_TRADE_INGRESS_DIAGNOSTIC_RESERVED_CAPACITY",
    "TX_TRADE_INGRESS_QUOTE_CAPACITY",
    "TX_TRADE_INGRESS_TICK_CAPACITY",
)
_LANE_DEFAULTS = (256, 64, 1024, 2752)


def _ingress_capacities(values: Mapping[str, str]) -> tuple[int, int, int, int, int]:
    aggregate = _positive_int(values, "TX_TRADE_INGRESS_QUEUE_CAPACITY", 4096)
    lane_override = any(key in values for key in _LANE_KEYS)
    aggregate_override = "TX_TRADE_INGRESS_QUEUE_CAPACITY" in values
    if aggregate_override and not lane_override:
        if aggregate < 4:
            raise ConfigError(
                "TX_TRADE_INGRESS_QUEUE_CAPACITY must be at least 4"
            )
        remaining = aggregate - 4
        weighted = [remaining * weight // 4096 for weight in _LANE_DEFAULTS]
        remainder = remaining - sum(weighted)
        fractions = [
            (remaining * weight % 4096, index)
            for index, weight in enumerate(_LANE_DEFAULTS)
        ]
        for _, index in sorted(fractions, reverse=True)[:remainder]:
            weighted[index] += 1
        lanes = tuple(value + 1 for value in weighted)
    else:
        lanes = tuple(
            _positive_int(values, key, default)
            for key, default in zip(_LANE_KEYS, _LANE_DEFAULTS, strict=True)
        )
    return (aggregate, *lanes)


def parse_phase1_settings(
    values: Mapping[str, str] | None = None,
) -> Phase1Settings:
    """Parse an explicit mapping without consulting ambient process state.

    An aggregate-only ingress capacity override is deterministically divided
    across four positive lanes using the default lane weights. Once any lane
    is explicitly supplied, explicit/default lane values are instead checked
    against the aggregate fail-closed.
    """

    supplied: Mapping[str, str] = {} if values is None else values
    if not isinstance(supplied, Mapping):
        raise ConfigError("values must be a mapping")
    for key, value in supplied.items():
        if type(key) is not str or type(value) is not str:
            raise ConfigError("configuration keys and values must be strings")
    preset = _enum_value(
        RuntimePreset,
        supplied.get("TX_TRADE_RUNTIME_PRESET", RuntimePreset.PHASE1_DEFAULT.value),
        "TX_TRADE_RUNTIME_PRESET",
    )
    preset_quote, preset_execution = _PRESET_MODES[preset]
    quote_source = _enum_value(
        QuoteSource,
        supplied.get("TX_TRADE_QUOTE_SOURCE", preset_quote.value),
        "TX_TRADE_QUOTE_SOURCE",
    )
    execution_mode = _enum_value(
        ExecutionMode,
        supplied.get("TX_TRADE_EXECUTION_MODE", preset_execution.value),
        "TX_TRADE_EXECUTION_MODE",
    )
    if "TX_TRADE_QUOTE_SOURCE" in supplied and quote_source is not preset_quote:
        raise ConfigError("TX_TRADE_QUOTE_SOURCE conflicts with runtime preset")
    if (
        "TX_TRADE_EXECUTION_MODE" in supplied
        and execution_mode is not preset_execution
    ):
        raise ConfigError("TX_TRADE_EXECUTION_MODE conflicts with runtime preset")

    opt_in_raw = supplied.get("TX_TRADE_ENABLE_LIVE_QUOTE", "0")
    if opt_in_raw not in {"0", "1"}:
        raise ConfigError("TX_TRADE_ENABLE_LIVE_QUOTE must be exactly '0' or '1'")

    (
        ingress_capacity,
        control_capacity,
        diagnostic_capacity,
        quote_capacity,
        tick_capacity,
    ) = _ingress_capacities(supplied)
    settings = Phase1Settings(
        preset=preset,
        quote_source=quote_source,
        execution_mode=execution_mode,
        live_quote_opt_in=opt_in_raw == "1",
        ingress_queue_capacity=ingress_capacity,
        ingress_control_capacity=control_capacity,
        ingress_diagnostic_capacity=diagnostic_capacity,
        ingress_quote_capacity=quote_capacity,
        ingress_tick_capacity=tick_capacity,
        ingress_dedupe_capacity=_positive_int(
            supplied, "TX_TRADE_INGRESS_DEDUPE_CAPACITY", 4096
        ),
        sta_quote_enrichment_capacity=_positive_int(
            supplied, "TX_TRADE_STA_QUOTE_ENRICHMENT_CAPACITY", 1024
        ),
        storage_writer_queue_capacity=_positive_int(
            supplied, "TX_TRADE_STORAGE_WRITER_QUEUE_CAPACITY", 4096
        ),
        storage_batch_size=_positive_int(
            supplied, "TX_TRADE_STORAGE_BATCH_SIZE", 128
        ),
        storage_flush_interval_ms=_positive_int(
            supplied, "TX_TRADE_STORAGE_FLUSH_INTERVAL_MS", 250
        ),
    )
    return settings
