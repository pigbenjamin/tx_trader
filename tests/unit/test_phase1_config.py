from dataclasses import FrozenInstanceError

import pytest

from tx_trade.app.config import (
    ConfigError,
    ExecutionMode,
    QuoteSource,
    RuntimePreset,
    parse_phase1_settings,
)


def test_default_is_offline_disabled_and_immutable():
    settings = parse_phase1_settings({})
    assert settings.preset is RuntimePreset.PHASE1_DEFAULT
    assert settings.quote_source is QuoteSource.OFFLINE
    assert settings.execution_mode is ExecutionMode.DISABLED
    assert settings.live_quote_opt_in is False
    with pytest.raises(FrozenInstanceError):
        settings.quote_source = QuoteSource.LIVE


def test_live_quote_requires_exact_opt_in():
    with pytest.raises(ConfigError, match="requires"):
        parse_phase1_settings({"TX_TRADE_RUNTIME_PRESET": "phase1_live_quote"})
    settings = parse_phase1_settings(
        {
            "TX_TRADE_RUNTIME_PRESET": "phase1_live_quote",
            "TX_TRADE_ENABLE_LIVE_QUOTE": "1",
        }
    )
    assert settings.quote_source is QuoteSource.LIVE
    for value in ("true", "TRUE", "yes", ""):
        with pytest.raises(ConfigError, match="exactly"):
            parse_phase1_settings({"TX_TRADE_ENABLE_LIVE_QUOTE": value})


@pytest.mark.parametrize(
    "preset", ["phase2_replay", "research_paper", "live_trade"]
)
def test_future_or_execution_presets_are_rejected(preset):
    with pytest.raises(ConfigError):
        parse_phase1_settings(
            {
                "TX_TRADE_RUNTIME_PRESET": preset,
                "TX_TRADE_ENABLE_LIVE_QUOTE": "1",
            }
        )


@pytest.mark.parametrize("mode", ["paper", "live"])
def test_execution_is_always_disabled(mode):
    with pytest.raises(ConfigError, match="conflicts"):
        parse_phase1_settings({"TX_TRADE_EXECUTION_MODE": mode})


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("TX_TRADE_RUNTIME_PRESET", "mystery"),
        ("TX_TRADE_QUOTE_SOURCE", "fixture"),
        ("TX_TRADE_EXECUTION_MODE", "enabled"),
        ("TX_TRADE_INGRESS_QUEUE_CAPACITY", "0"),
        ("TX_TRADE_STA_QUOTE_ENRICHMENT_CAPACITY", "-1"),
        ("TX_TRADE_STORAGE_WRITER_QUEUE_CAPACITY", "many"),
    ],
)
def test_invalid_settings_fail_closed(key, value):
    with pytest.raises(ConfigError):
        parse_phase1_settings({key: value})


def test_parser_does_not_need_environment_credentials(monkeypatch):
    monkeypatch.setattr("builtins.open", lambda *args, **kwargs: pytest.fail("filesystem read"))
    settings = parse_phase1_settings({})
    assert settings.quote_source is QuoteSource.OFFLINE


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("TX_TRADE_QUOTE_SOURCE", "live"),
        ("TX_TRADE_EXECUTION_MODE", "paper"),
    ],
)
def test_mode_override_must_match_preset(key, value):
    with pytest.raises(ConfigError, match="conflicts"):
        parse_phase1_settings({key: value})


@pytest.mark.parametrize("value", [1, True, "+1", " 1", "1.0"])
def test_capacity_requires_ascii_digit_string(value):
    with pytest.raises(ConfigError):
        parse_phase1_settings({"TX_TRADE_INGRESS_QUEUE_CAPACITY": value})
