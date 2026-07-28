from pathlib import Path

import pytest

from tx_trade.app.config import ConfigError
from tx_trade.app.phase1 import Phase1Dependencies, Phase1RuntimeError, run_phase1


class TrackingEnvironment(dict):
    def __init__(self, values):
        super().__init__(values)
        self.reads = []

    def get(self, key, default=None):
        self.reads.append(key)
        return super().get(key, default)


def test_offline_validation_never_reads_live_secrets_or_constructs_live(tmp_path):
    environment = TrackingEnvironment({})

    def forbidden():
        raise AssertionError("live factory must remain lazy")

    result = run_phase1(
        environment,
        db_path=str(tmp_path / "offline.db"),
        dependencies=Phase1Dependencies(backend_factory=forbidden, adapter_factory=forbidden),
    )
    assert result.status == "complete"
    assert result.integrity_valid
    assert result.event_count == 6
    assert "TX_TRADE_ACCOUNT" not in environment.reads
    assert "TX_TRADE_PASSWORD" not in environment.reads
    assert "TX_TRADE_SKCOM_DLL_PATH" not in environment.reads


def test_live_without_production_opt_in_fails_before_secret_or_factory_reads():
    environment = TrackingEnvironment({"TX_TRADE_RUNTIME_PRESET": "phase1_live_quote"})
    called = []
    with pytest.raises(ConfigError, match="ENABLE_LIVE_QUOTE"):
        run_phase1(
            environment,
            dependencies=Phase1Dependencies(repository_factory=lambda path: called.append(path)),
        )
    assert called == []
    assert "TX_TRADE_ACCOUNT" not in environment.reads
    assert "TX_TRADE_PASSWORD" not in environment.reads


def test_live_missing_credentials_fails_before_database_and_com_factories():
    environment = {
        "TX_TRADE_RUNTIME_PRESET": "phase1_live_quote",
        "TX_TRADE_ENABLE_LIVE_QUOTE": "1",
    }
    called = []
    dependencies = Phase1Dependencies(
        repository_factory=lambda path: called.append(("repo", path)),
        backend_factory=lambda: called.append(("backend", None)),
        adapter_factory=lambda **kwargs: called.append(("adapter", kwargs)),
    )
    with pytest.raises(Phase1RuntimeError, match="configuration is incomplete"):
        run_phase1(environment, dependencies=dependencies)
    assert called == []


@pytest.mark.parametrize("preset", ["phase2_replay", "research_paper", "live_trade"])
def test_non_phase1_runtime_presets_fail_before_database(preset):
    called = []
    with pytest.raises(ConfigError):
        run_phase1(
            {"TX_TRADE_RUNTIME_PRESET": preset},
            dependencies=Phase1Dependencies(repository_factory=lambda path: called.append(path)),
        )
    assert called == []


def test_root_main_is_thin_and_contains_no_order_or_reply():
    source = Path("main.py").read_text(encoding="utf-8")
    assert "tx_trade.app.phase1" in source
    assert "Order" not in source
    assert "Reply" not in source
    assert "quote_client" not in source


def test_offline_session_factory_is_validated_before_repository_creation():
    called = []
    with pytest.raises(TypeError, match="session_id_factory"):
        run_phase1(
            {},
            dependencies=Phase1Dependencies(
                session_id_factory=lambda: "not-a-uuid",
                repository_factory=lambda path: called.append(path),
            ),
        )
    assert called == []
