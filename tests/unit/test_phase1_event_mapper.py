from datetime import datetime
from uuid import UUID

import pytest

from tx_trade.market_data.event_mapper import Phase1CapturedEventMapper
from tx_trade.market_data.models import (
    AdapterDiagnostic,
    CapturedAdapterDiagnostic,
    CapturedConnectionNotification,
    CapturedKind,
    CapturedMarketDataEvent,
    CapturedQuoteSnapshot,
    CapturedServerTimeNotification,
    CapturedStockListNotification,
    CapturedTickNotification,
    ConnectionState,
    EventType,
    SourceMode,
    TAIPEI,
)

NOW = datetime(2026, 7, 26, 9, 30, tzinfo=TAIPEI)
SESSION = UUID("12345678-1234-5678-1234-567812345678")


def captured(kind, payload, *, event_at=None, trading_day=None, metadata_version=None):
    return CapturedMarketDataEvent(
        kind,
        payload,
        {"raw": payload.callback_sequence},
        "capital_skcom",
        SourceMode.LIVE,
        SESSION,
        2,
        payload.callback_sequence,
        None,
        NOW,
        event_at,
        trading_day,
        metadata_version,
        "untrusted-candidate",
    )


@pytest.mark.parametrize(
    ("kind", "payload", "expected_type"),
    [
        (
            CapturedKind.CONNECTION_NOTIFICATION,
            CapturedConnectionNotification(3003, 0, 1, NOW),
            EventType.CONNECTION_STATUS,
        ),
        (
            CapturedKind.SERVER_TIME_NOTIFICATION,
            CapturedServerTimeNotification(9, 30, 0, 93000, 2, NOW),
            EventType.SERVER_TIME,
        ),
        (
            CapturedKind.QUOTE_SNAPSHOT,
            CapturedQuoteSnapshot(0, 7, 100, 102, 101, 1, 2, None, True, 3, NOW),
            EventType.QUOTE,
        ),
        (
            CapturedKind.TICK_NOTIFICATION,
            CapturedTickNotification(
                0,
                7,
                42,
                20260726,
                93000,
                123,
                100,
                102,
                101,
                3,
                0,
                True,
                4,
                NOW,
            ),
            EventType.TICK,
        ),
        (
            CapturedKind.STOCK_LIST_NOTIFICATION,
            CapturedStockListNotification(0, b"TX00,raw", 5, NOW),
            EventType.ADAPTER_DIAGNOSTIC,
        ),
        (
            CapturedKind.ADAPTER_DIAGNOSTIC,
            CapturedAdapterDiagnostic(
                "adapter_error",
                0,
                7,
                -1,
                "bounded diagnostic",
                NOW,
                1,
                2,
                6,
                {"code": -1},
            ),
            EventType.ADAPTER_DIAGNOSTIC,
        ),
    ],
)
def test_mapper_covers_captured_union_and_preserves_metadata(kind, payload, expected_type):
    event = captured(kind, payload)
    envelope = Phase1CapturedEventMapper().build_envelope(event, 9)
    assert envelope.event_type is expected_type
    assert envelope.ingest_sequence == 9
    for name in (
        "source",
        "source_mode",
        "session_id",
        "connection_generation",
        "sequence",
        "broker_sequence",
        "received_at",
        "event_at",
        "trading_day",
        "metadata_version",
        "raw_payload",
    ):
        assert getattr(envelope, name) == getattr(event, name)
    assert "untrusted-candidate" not in envelope.dedupe_key


def test_actual_adapter_shaped_connection_uses_received_time_only_for_changed_at():
    payload = CapturedConnectionNotification(3003, 0, 1, NOW)
    envelope = Phase1CapturedEventMapper().build_envelope(
        captured(CapturedKind.CONNECTION_NOTIFICATION, payload), 0
    )
    assert envelope.event_at is None
    assert envelope.payload.changed_at == NOW
    assert envelope.payload.state is ConnectionState.STOCKS_READY
    assert envelope.payload.is_ready is True


def test_unknown_scales_keep_raw_values_and_leave_normalized_values_none():
    quote = CapturedQuoteSnapshot(0, 7, 12345, 12347, 12346, 1, 2, 3, True, 3, NOW)
    envelope = Phase1CapturedEventMapper().build_envelope(
        captured(CapturedKind.QUOTE_SNAPSHOT, quote, metadata_version=7), 0
    )
    assert envelope.payload.instrument_id == "synthetic:capital_skcom:0:7"
    assert envelope.payload.bid_raw == 12345
    assert envelope.payload.bid_normalized is None
    assert envelope.payload.price_scale is None


def test_stock_list_maps_to_lossless_diagnostic_without_fake_instrument():
    payload = CapturedStockListNotification(4, b"\xffTX", 8, NOW)
    envelope = Phase1CapturedEventMapper().build_envelope(
        captured(CapturedKind.STOCK_LIST_NOTIFICATION, payload), 0
    )
    assert type(envelope.payload) is AdapterDiagnostic
    assert envelope.payload.diagnostic_kind == "stock_list_parse_failure"
    assert envelope.payload.raw_notification["market_no_raw"] == 4
    assert envelope.payload.raw_notification["stock_list_raw"]["encoding"] == "base64"
    assert "instrument" not in envelope.payload.raw_notification


def test_mapper_dedupe_is_deterministic():
    payload = CapturedServerTimeNotification(9, 30, 0, 93000, 2, NOW)
    event = captured(CapturedKind.SERVER_TIME_NOTIFICATION, payload)
    mapper = Phase1CapturedEventMapper()
    assert mapper.build_envelope(event, 0).dedupe_key == mapper.build_envelope(event, 99).dedupe_key
