"""Pure, side-effect-free configuration for the Phase 2 replay runtime."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from uuid import UUID

from tx_trade.replay.contracts import ReplayMode, ReplayOptions


class Phase2ConfigError(ValueError):
    """Raised when Phase 2 replay settings are missing or invalid."""


@dataclass(frozen=True, slots=True)
class Phase2ReplaySettings:
    """Validated settings for replay-only execution."""

    runtime_preset: Literal["phase2_replay"]
    execution_mode: Literal["disabled"]
    database_path: Path
    session_id: UUID
    options: ReplayOptions

    def __post_init__(self) -> None:
        if self.runtime_preset != "phase2_replay":
            raise Phase2ConfigError("runtime_preset must be phase2_replay")
        if self.execution_mode != "disabled":
            raise Phase2ConfigError("execution_mode must be disabled")
        if not isinstance(self.database_path, Path):
            raise Phase2ConfigError("database_path must be a Path")
        if type(self.session_id) is not UUID:
            raise Phase2ConfigError("session_id must be a UUID")
        if type(self.options) is not ReplayOptions:
            raise Phase2ConfigError("options must be ReplayOptions")


def _read_string(
    values: Mapping[str, str],
    key: str,
    *,
    default: str | None = None,
) -> str:
    raw = values.get(key, default)
    if raw is None:
        raise Phase2ConfigError(f"{key} is required")
    if type(raw) is not str:
        raise Phase2ConfigError(f"{key} must be a string")
    if not raw.strip():
        raise Phase2ConfigError(f"{key} must not be empty")
    return raw


def _parse_session_id(raw: str) -> UUID:
    try:
        return UUID(raw)
    except (AttributeError, TypeError, ValueError):
        raise Phase2ConfigError("TX_TRADE_REPLAY_SESSION_ID must be a UUID") from None


def _parse_mode(raw: str) -> ReplayMode:
    try:
        return ReplayMode(raw)
    except (TypeError, ValueError):
        raise Phase2ConfigError("TX_TRADE_REPLAY_MODE must be one of: fastest, paced") from None


def _parse_speed(raw: str) -> float:
    try:
        speed = float(raw)
        ReplayOptions(mode=ReplayMode.FASTEST, speed=speed)
    except (TypeError, ValueError):
        raise Phase2ConfigError("TX_TRADE_REPLAY_SPEED must be finite and positive") from None
    return speed


def _parse_cursor(raw: str | None) -> int | None:
    if raw is None:
        return None
    if type(raw) is not str:
        raise Phase2ConfigError("TX_TRADE_REPLAY_AFTER_INGEST_SEQUENCE must be a string")
    if not raw or not raw.isascii() or not raw.isdecimal():
        raise Phase2ConfigError(
            "TX_TRADE_REPLAY_AFTER_INGEST_SEQUENCE must be non-negative ASCII digits"
        )
    return int(raw)


def parse_phase2_replay_settings(
    values: Mapping[str, str],
) -> Phase2ReplaySettings:
    """Parse an explicit mapping without reading environment or filesystem state."""

    if not isinstance(values, Mapping):
        raise Phase2ConfigError("values must be a mapping")

    preset = _read_string(
        values,
        "TX_TRADE_RUNTIME_PRESET",
        default="phase2_replay",
    )
    if preset != "phase2_replay":
        raise Phase2ConfigError("TX_TRADE_RUNTIME_PRESET must be phase2_replay")

    database_path = Path(_read_string(values, "TX_TRADE_REPLAY_DB_PATH"))
    session_id = _parse_session_id(_read_string(values, "TX_TRADE_REPLAY_SESSION_ID"))
    mode = _parse_mode(
        _read_string(values, "TX_TRADE_REPLAY_MODE", default=ReplayMode.FASTEST.value)
    )
    speed = _parse_speed(_read_string(values, "TX_TRADE_REPLAY_SPEED", default="1.0"))
    cursor = _parse_cursor(values.get("TX_TRADE_REPLAY_AFTER_INGEST_SEQUENCE"))

    return Phase2ReplaySettings(
        runtime_preset="phase2_replay",
        execution_mode="disabled",
        database_path=database_path,
        session_id=session_id,
        options=ReplayOptions(
            mode=mode,
            speed=speed,
            after_ingest_sequence=cursor,
        ),
    )
