from datetime import date, datetime
from uuid import UUID

import pytest

from tx_trade.market_data.models import SCHEMA_VERSION, SourceMode, TAIPEI
from tx_trade.market_data.ports import (
    HealthSnapshot,
    IngressDecision,
    ReadbackIntegrityReport,
    RecordingSession,
)

NOW = datetime(2026, 7, 26, 9, 30, tzinfo=TAIPEI)
SESSION = UUID("12345678-1234-5678-1234-567812345678")


def test_port_value_types_are_strict_and_immutable():
    session = RecordingSession(
        session_id=SESSION,
        schema_version=SCHEMA_VERSION,
        source="fixture",
        source_mode=SourceMode.OFFLINE,
        started_at=NOW,
        trading_day=date(2026, 7, 26),
        config_fingerprint="sha256:fixed",
    )
    assert session.session_id == SESSION
    assert {item.value for item in IngressDecision} == {
        "accepted",
        "coalesced",
        "dropped",
        "duplicate",
    }

    with pytest.raises(TypeError):
        RecordingSession(
            session_id=str(SESSION),
            schema_version=SCHEMA_VERSION,
            source="fixture",
            source_mode=SourceMode.OFFLINE,
            started_at=NOW,
            trading_day=None,
            config_fingerprint="fixed",
        )


def test_health_and_integrity_report_consistency():
    assert not HealthSnapshot(False, (), NOW).is_degraded
    with pytest.raises(ValueError, match="match"):
        HealthSnapshot(False, ("queue full",), NOW)

    valid = ReadbackIntegrityReport(SESSION, 0, None, None, True, ())
    assert valid.is_valid
    with pytest.raises(ValueError, match="sequence bounds"):
        ReadbackIntegrityReport(SESSION, 1, None, None, True, ())
    with pytest.raises(ValueError, match="errors"):
        ReadbackIntegrityReport(SESSION, 0, None, None, True, ("bad",))
