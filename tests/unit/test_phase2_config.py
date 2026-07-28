from collections.abc import Iterator, Mapping
from dataclasses import FrozenInstanceError
from pathlib import Path
from traceback import format_exception
from uuid import UUID, uuid4

import pytest

from tx_trade.app.phase2_config import (
    Phase2ConfigError,
    Phase2ReplaySettings,
    parse_phase2_replay_settings,
)
from tx_trade.replay.contracts import ReplayMode, ReplayOptions


def _required_values() -> dict[str, str]:
    return {
        "TX_TRADE_REPLAY_DB_PATH": "recordings.sqlite3",
        "TX_TRADE_REPLAY_SESSION_ID": str(uuid4()),
    }


def test_defaults_are_replay_only_fastest_and_immutable():
    values = _required_values()

    settings = parse_phase2_replay_settings(values)

    assert settings.runtime_preset == "phase2_replay"
    assert settings.execution_mode == "disabled"
    assert settings.database_path == Path("recordings.sqlite3")
    assert settings.session_id == UUID(values["TX_TRADE_REPLAY_SESSION_ID"])
    assert settings.options == ReplayOptions(mode=ReplayMode.FASTEST)
    with pytest.raises(FrozenInstanceError):
        settings.execution_mode = "enabled"


def test_parses_paced_speed_and_exclusive_cursor():
    values = {
        **_required_values(),
        "TX_TRADE_RUNTIME_PRESET": "phase2_replay",
        "TX_TRADE_REPLAY_MODE": "paced",
        "TX_TRADE_REPLAY_SPEED": "2.5",
        "TX_TRADE_REPLAY_AFTER_INGEST_SEQUENCE": "0",
    }

    settings = parse_phase2_replay_settings(values)

    assert settings.options.mode is ReplayMode.PACED
    assert settings.options.speed == 2.5
    assert settings.options.after_ingest_sequence == 0


def test_fastest_retains_validated_speed():
    values = {
        **_required_values(),
        "TX_TRADE_REPLAY_MODE": "fastest",
        "TX_TRADE_REPLAY_SPEED": "8",
    }

    settings = parse_phase2_replay_settings(values)

    assert settings.options == ReplayOptions(mode=ReplayMode.FASTEST, speed=8.0)


@pytest.mark.parametrize(
    "missing_key",
    [
        "TX_TRADE_REPLAY_DB_PATH",
        "TX_TRADE_REPLAY_SESSION_ID",
    ],
)
def test_required_values_must_be_present(missing_key):
    values = _required_values()
    del values[missing_key]

    with pytest.raises(Phase2ConfigError, match=missing_key):
        parse_phase2_replay_settings(values)


@pytest.mark.parametrize(
    "key",
    [
        "TX_TRADE_RUNTIME_PRESET",
        "TX_TRADE_REPLAY_DB_PATH",
        "TX_TRADE_REPLAY_SESSION_ID",
        "TX_TRADE_REPLAY_MODE",
        "TX_TRADE_REPLAY_SPEED",
        "TX_TRADE_REPLAY_AFTER_INGEST_SEQUENCE",
    ],
)
def test_explicit_empty_values_are_rejected(key):
    values = _required_values()
    values[key] = ""

    with pytest.raises(Phase2ConfigError, match=key):
        parse_phase2_replay_settings(values)


@pytest.mark.parametrize("preset", ["phase1_default", "research_paper", "live_trade"])
def test_unknown_or_unsafe_preset_is_rejected(preset):
    values = {**_required_values(), "TX_TRADE_RUNTIME_PRESET": preset}

    with pytest.raises(Phase2ConfigError, match="RUNTIME_PRESET"):
        parse_phase2_replay_settings(values)


@pytest.mark.parametrize("mode", ["FASTEST", "instant", "live", ""])
def test_unknown_or_empty_mode_is_rejected(mode):
    values = {**_required_values(), "TX_TRADE_REPLAY_MODE": mode}

    with pytest.raises(Phase2ConfigError, match="REPLAY_MODE"):
        parse_phase2_replay_settings(values)


@pytest.mark.parametrize("session_id", ["not-a-uuid", "1234", " "])
def test_invalid_session_uuid_is_rejected(session_id):
    values = {**_required_values(), "TX_TRADE_REPLAY_SESSION_ID": session_id}

    with pytest.raises(Phase2ConfigError, match="REPLAY_SESSION_ID"):
        parse_phase2_replay_settings(values)


@pytest.mark.parametrize(
    "speed",
    ["0", "-1", "nan", "NaN", "inf", "-inf", "infinity", "not-a-number"],
)
def test_speed_must_be_finite_and_positive(speed):
    values = {**_required_values(), "TX_TRADE_REPLAY_SPEED": speed}

    with pytest.raises(Phase2ConfigError, match="REPLAY_SPEED"):
        parse_phase2_replay_settings(values)


@pytest.mark.parametrize("cursor", ["-1", "+1", "1.0", " 1", "1 ", "１２", "true"])
def test_cursor_must_be_nonnegative_ascii_digits(cursor):
    values = {
        **_required_values(),
        "TX_TRADE_REPLAY_AFTER_INGEST_SEQUENCE": cursor,
    }

    with pytest.raises(Phase2ConfigError, match="AFTER_INGEST_SEQUENCE"):
        parse_phase2_replay_settings(values)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("TX_TRADE_REPLAY_DB_PATH", True),
        ("TX_TRADE_REPLAY_SESSION_ID", 1),
        ("TX_TRADE_REPLAY_MODE", False),
        ("TX_TRADE_REPLAY_SPEED", 2.5),
        ("TX_TRADE_REPLAY_AFTER_INGEST_SEQUENCE", True),
    ],
)
def test_non_string_mapping_values_are_rejected(key, value):
    values = _required_values()
    values[key] = value

    with pytest.raises(Phase2ConfigError, match=key):
        parse_phase2_replay_settings(values)


def test_non_mapping_input_is_rejected():
    with pytest.raises(Phase2ConfigError, match="mapping"):
        parse_phase2_replay_settings(None)


def test_invalid_value_is_not_exposed_by_exception_chain():
    speed_canary = "sensitive-speed-canary"
    values = {**_required_values(), "TX_TRADE_REPLAY_SPEED": speed_canary}

    with pytest.raises(Phase2ConfigError) as caught:
        parse_phase2_replay_settings(values)

    rendered = "".join(
        format_exception(type(caught.value), caught.value, caught.value.__traceback__)
    )
    assert speed_canary not in rendered
    assert caught.value.__cause__ is None


def test_parser_does_not_access_database_path(monkeypatch):
    monkeypatch.setattr(
        "builtins.open",
        lambda *args, **kwargs: pytest.fail("unexpected filesystem access"),
    )
    values = {
        **_required_values(),
        "TX_TRADE_REPLAY_DB_PATH": "definitely/not/created.sqlite3",
    }

    settings = parse_phase2_replay_settings(values)

    assert settings.database_path == Path("definitely/not/created.sqlite3")


def test_direct_settings_construction_remains_fail_closed():
    valid = parse_phase2_replay_settings(_required_values())

    with pytest.raises(Phase2ConfigError, match="runtime_preset"):
        Phase2ReplaySettings(
            runtime_preset="live_trade",
            execution_mode="disabled",
            database_path=valid.database_path,
            session_id=valid.session_id,
            options=valid.options,
        )
    with pytest.raises(Phase2ConfigError, match="execution_mode"):
        Phase2ReplaySettings(
            runtime_preset="phase2_replay",
            execution_mode="paper",
            database_path=valid.database_path,
            session_id=valid.session_id,
            options=valid.options,
        )


class _AccessTrackingMapping(Mapping[str, str]):
    def __init__(self, values: dict[str, str]) -> None:
        self._values = values
        self.accessed: list[str] = []

    def __getitem__(self, key: str) -> str:
        self.accessed.append(key)
        return self._values[key]

    def __iter__(self) -> Iterator[str]:
        raise AssertionError("parser must not enumerate configuration")

    def __len__(self) -> int:
        return len(self._values)

    def get(self, key: str, default: str | None = None) -> str | None:
        self.accessed.append(key)
        return self._values.get(key, default)


def test_parser_reads_only_replay_whitelist_keys():
    values = _AccessTrackingMapping(
        {
            **_required_values(),
            "TX_TRADE_ACCOUNT": "account-canary",
            "TX_TRADE_PASSWORD": "password-canary",
            "TX_TRADE_SKCOM_DLL_PATH": "dll-canary",
            "TX_TRADE_EXECUTION_MODE": "live",
        }
    )

    settings = parse_phase2_replay_settings(values)

    assert settings.execution_mode == "disabled"
    assert set(values.accessed) == {
        "TX_TRADE_RUNTIME_PRESET",
        "TX_TRADE_REPLAY_DB_PATH",
        "TX_TRADE_REPLAY_SESSION_ID",
        "TX_TRADE_REPLAY_MODE",
        "TX_TRADE_REPLAY_SPEED",
        "TX_TRADE_REPLAY_AFTER_INGEST_SEQUENCE",
    }
