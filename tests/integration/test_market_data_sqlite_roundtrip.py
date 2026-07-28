from __future__ import annotations

from tx_trade.market_data.fixtures import (
    OFFLINE_FIXTURE_TIME,
    OFFLINE_FIXTURE_TRADING_DAY,
    make_offline_fixture_envelopes,
)
from tx_trade.market_data.models import SCHEMA_VERSION, SourceMode, serialize_envelope
from tx_trade.market_data.ports import RecordingSession
from tx_trade.storage import SQLiteMarketDataRepository, SQLiteReplaySource


def test_six_event_offline_fixture_sqlite_roundtrip(tmp_path) -> None:
    expected = make_offline_fixture_envelopes()
    repository = SQLiteMarketDataRepository(tmp_path / "recording.db")
    repository.begin_session(
        RecordingSession(
            expected[0].session_id,
            SCHEMA_VERSION,
            expected[0].source,
            SourceMode.OFFLINE,
            OFFLINE_FIXTURE_TIME,
            OFFLINE_FIXTURE_TRADING_DAY,
            "fixture",
        )
    )
    repository.append_batch(expected)
    actual = tuple(repository.iter_events(expected[0].session_id))
    assert [serialize_envelope(item) for item in actual] == [
        serialize_envelope(item) for item in expected
    ]
    replay = SQLiteReplaySource(repository)
    replay.open(expected[0].session_id)
    assert replay.verify_integrity().is_valid
