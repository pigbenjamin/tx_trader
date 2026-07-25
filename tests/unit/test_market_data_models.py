import json
from dataclasses import FrozenInstanceError, fields
from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from tx_trade.market_data.models import (
    SCHEMA_VERSION,
    TAIPEI,
    CapturedKind,
    CapturedMarketDataEvent,
    CapturedTickNotification,
    EventType,
    MarketDataEnvelope,
    SourceMode,
    StaLocalQuoteNotification,
    Tick,
    build_adapter_diagnostic_dedupe_key,
    serialize_envelope,
    to_primitive,
)

NOW = datetime(2026, 7, 26, 9, 30, tzinfo=TAIPEI)
SESSION = UUID("12345678-1234-5678-1234-567812345678")


def make_tick(**changes):
    values = dict(
        instrument_id="TAIFEX:0:TX00",
        market_no_raw=0,
        stock_idx_raw=7,
        source_pointer_raw=9,
        date_raw=20260726,
        time_hms_raw=93000,
        time_subsecond_raw=123,
        bid_raw=10001,
        ask_raw=10003,
        close_raw=10002,
        bid_normalized=Decimal("100.01"),
        ask_normalized=Decimal("100.03"),
        close_normalized=Decimal("100.02"),
        quantity_raw=2,
        quantity_normalized=Decimal("20"),
        simulate_raw=0,
        is_simulated=None,
        event_at=NOW,
        received_at=NOW,
        trading_day=date(2026, 7, 26),
        is_long_callback=True,
        price_scale=Decimal("0.01"),
        quantity_scale=Decimal("10"),
    )
    values.update(changes)
    return Tick(**values)


def make_envelope(payload=None, **changes):
    payload = payload or make_tick()
    values = dict(
        schema_version=SCHEMA_VERSION,
        event_type=EventType.TICK,
        payload=payload,
        source="fixture",
        source_mode=SourceMode.OFFLINE,
        session_id=SESSION,
        ingest_sequence=0,
        connection_generation=0,
        sequence=3,
        broker_sequence=None,
        dedupe_key="fixed-key",
        event_at=payload.event_at,
        received_at=payload.received_at,
        trading_day=payload.trading_day,
        metadata_version=1,
        raw_payload={"nested": {"prices": [10001, 10003]}},
    )
    values.update(changes)
    return MarketDataEnvelope(**values)


def test_models_are_frozen_and_slotted():
    tick = make_tick()
    assert not hasattr(tick, "__dict__")
    with pytest.raises(FrozenInstanceError):
        tick.quantity_raw = 3


@pytest.mark.parametrize(
    "bad_time",
    [datetime(2026, 7, 26, 9, 30), datetime(2026, 7, 26, 9, 30, tzinfo=timezone.utc)],
)
def test_datetime_requires_real_taipei_zone(bad_time):
    with pytest.raises(ValueError):
        make_tick(received_at=bad_time)


def test_decimal_scale_and_unknown_scale_rules():
    make_tick()
    with pytest.raises(ValueError, match="raw \\* scale"):
        make_tick(bid_normalized=Decimal("100.00"))
    with pytest.raises(ValueError, match="unknown"):
        make_tick(price_scale=None)
    unknown = make_tick(
        price_scale=None,
        bid_normalized=None,
        ask_normalized=None,
        close_normalized=None,
        quantity_scale=None,
        quantity_normalized=None,
    )
    assert unknown.close_normalized is None
    with pytest.raises(TypeError):
        make_tick(price_scale=0.01)


@pytest.mark.parametrize("invalid", ["NaN", "Infinity", "-Infinity"])
def test_decimal_scale_and_normalized_values_must_be_finite(invalid):
    value = Decimal(invalid)
    with pytest.raises(ValueError, match="finite"):
        make_tick(price_scale=value, bid_normalized=value)
    with pytest.raises(ValueError, match="finite"):
        make_tick(bid_normalized=value)


@pytest.mark.parametrize(
    "changes",
    [
        {"quantity_raw": -1},
        {"stock_idx_raw": -1},
        {"source_pointer_raw": -1},
    ],
)
def test_invalid_tick_raw_values(changes):
    with pytest.raises(ValueError):
        make_tick(**changes)


def test_local_quote_notification_cannot_be_ingress_payload():
    local = StaLocalQuoteNotification(0, 7, True, 3, NOW)
    assert "ingest_sequence" not in {field.name for field in fields(local)}
    with pytest.raises(ValueError, match="does not match"):
        CapturedMarketDataEvent(
            captured_kind=CapturedKind.TICK_NOTIFICATION,
            payload=local,
            raw_payload=None,
            source="capital",
            source_mode=SourceMode.LIVE,
            session_id=SESSION,
            connection_generation=0,
            sequence=3,
            broker_sequence=None,
            received_at=NOW,
            event_at=None,
            trading_day=None,
            metadata_version=None,
            dedupe_candidate=None,
        )


def test_captured_kind_pairing_and_no_ingest_sequence():
    payload = CapturedTickNotification(
        0, 7, 9, 20260726, 93000, 123, 10001, 10003, 10002, 2, 0, True, 3, NOW
    )
    event = CapturedMarketDataEvent(
        CapturedKind.TICK_NOTIFICATION,
        payload,
        None,
        "capital",
        SourceMode.LIVE,
        SESSION,
        0,
        3,
        None,
        NOW,
        None,
        None,
        None,
        None,
    )
    assert "ingest_sequence" not in {field.name for field in fields(event)}
    with pytest.raises(ValueError):
        CapturedMarketDataEvent(
            CapturedKind.CONNECTION_NOTIFICATION,
            payload,
            None,
            "capital",
            SourceMode.LIVE,
            SESSION,
            0,
            3,
            None,
            NOW,
            None,
            None,
            None,
            None,
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"schema_version": 2},
        {"ingest_sequence": -1},
        {"connection_generation": -1},
        {"sequence": -1},
        {"dedupe_key": " "},
        {"event_at": None},
    ],
)
def test_envelope_rejects_invalid_metadata(changes):
    with pytest.raises(ValueError):
        make_envelope(**changes)


def test_envelope_event_type_and_duplicate_times_must_match():
    with pytest.raises(ValueError, match="event_type"):
        make_envelope(event_type=EventType.QUOTE)
    with pytest.raises(ValueError, match="received_at"):
        make_envelope(received_at=NOW.replace(minute=31))


def test_raw_mapping_is_deeply_defensive():
    raw = {"nested": {"items": [1, 2]}}
    envelope = make_envelope(raw_payload=raw)
    raw["nested"]["items"].append(3)
    raw["nested"]["new"] = True
    assert to_primitive(envelope.raw_payload) == {"nested": {"items": [1, 2]}}
    with pytest.raises(TypeError):
        envelope.raw_payload["new"] = 1


def test_serialization_is_deterministic_and_json_compatible():
    envelope = make_envelope()
    first = serialize_envelope(envelope)
    second = serialize_envelope(envelope)
    assert first == second
    primitive = json.loads(first)
    assert primitive["session_id"] == str(SESSION)
    assert primitive["received_at"] == "2026-07-26T09:30:00+08:00"
    assert primitive["payload"]["bid_normalized"] == "100.01"
    assert primitive["event_type"] == "tick"


def test_diagnostic_dedupe_attempt_is_stable_and_distinct():
    args = ("capital", SESSION, 0, "quote_lookup_failure", 8, 1)
    first = build_adapter_diagnostic_dedupe_key(*args)
    assert first == build_adapter_diagnostic_dedupe_key(
        *args
    )
    assert first != build_adapter_diagnostic_dedupe_key(
        "capital", SESSION, 0, "quote_lookup_failure", 8, 2
    )


@pytest.mark.parametrize(
    "args",
    [
        ("other", SESSION, 0, "quote_lookup_failure", 8, 1),
        ("capital", UUID("22345678-1234-5678-1234-567812345678"), 0, "quote_lookup_failure", 8, 1),
        ("capital", SESSION, 1, "quote_lookup_failure", 8, 1),
        ("capital", SESSION, 0, "adapter_error", 8, 1),
        ("capital", SESSION, 0, "quote_lookup_failure", 9, 1),
    ],
)
def test_diagnostic_dedupe_all_dimensions_are_distinct(args):
    baseline = build_adapter_diagnostic_dedupe_key(
        "capital", SESSION, 0, "quote_lookup_failure", 8, 1
    )
    assert build_adapter_diagnostic_dedupe_key(*args) != baseline


def test_schema_bool_and_wrong_runtime_types_are_rejected():
    with pytest.raises(ValueError):
        make_envelope(schema_version=True)
    with pytest.raises(TypeError):
        make_envelope(source_mode="offline")
    with pytest.raises(TypeError):
        make_envelope(session_id=str(SESSION))
    with pytest.raises(TypeError):
        make_tick(is_long_callback=1)


@pytest.mark.parametrize("version", [0, -1, True])
def test_nullable_metadata_version_is_positive_when_present(version):
    with pytest.raises((TypeError, ValueError)):
        make_envelope(metadata_version=version)
