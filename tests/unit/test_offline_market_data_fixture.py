import re
from dataclasses import replace
from uuid import UUID

import pytest

from tx_trade.market_data.fixtures import (
    OFFLINE_FIXTURE_SESSION_ID,
    OFFLINE_FIXTURE_TIME,
    FakeClock,
    InMemoryReplaySource,
    make_offline_fixture_envelopes,
)
from tx_trade.market_data.models import EventType, TAIPEI, serialize_envelope
from tx_trade.market_data.models import build_adapter_diagnostic_dedupe_key


def test_fake_clock_advances_both_time_domains_deterministically():
    clock = FakeClock(OFFLINE_FIXTURE_TIME, initial_monotonic=10)
    clock.advance(1.25)
    assert clock.now().tzinfo is TAIPEI
    assert clock.now().timestamp() == OFFLINE_FIXTURE_TIME.timestamp() + 1.25
    assert clock.monotonic() == 11.25
    for bad in (-1, float("nan"), float("inf"), True):
        with pytest.raises((TypeError, ValueError)):
            clock.advance(bad)


def test_fixture_is_canonical_and_contains_every_event_type():
    first = make_offline_fixture_envelopes()
    second = make_offline_fixture_envelopes()
    assert tuple(map(serialize_envelope, first)) == tuple(map(serialize_envelope, second))
    assert {event.event_type for event in first} == set(EventType)
    assert [event.ingest_sequence for event in first] == list(range(6))
    assert all(re.fullmatch(r"[a-z_]+:sha256:[0-9a-f]{64}", event.dedupe_key) for event in first)

    diagnostic = first[-1]
    assert diagnostic.dedupe_key == build_adapter_diagnostic_dedupe_key(
        diagnostic.source,
        diagnostic.session_id,
        diagnostic.connection_generation,
        diagnostic.payload.diagnostic_kind,
        diagnostic.payload.callback_sequence,
        diagnostic.payload.attempt,
    )


def test_dedupe_identity_changes_with_generation_and_diagnostic_attempt():
    from tx_trade.market_data.fixtures import _build_fixture_dedupe_key

    base = dict(
        source="fixture",
        session_id=OFFLINE_FIXTURE_SESSION_ID,
        event_type=EventType.TICK,
        identity={"source_pointer_raw": 42},
    )
    first = _build_fixture_dedupe_key(connection_generation=0, **base)
    assert first == _build_fixture_dedupe_key(connection_generation=0, **base)
    assert first != _build_fixture_dedupe_key(connection_generation=1, **base)

    diagnostic = make_offline_fixture_envelopes()[-1]
    changed_attempt = build_adapter_diagnostic_dedupe_key(
        diagnostic.source,
        diagnostic.session_id,
        diagnostic.connection_generation,
        diagnostic.payload.diagnostic_kind,
        diagnostic.payload.callback_sequence,
        diagnostic.payload.attempt + 1,
    )
    assert changed_attempt != diagnostic.dedupe_key


def test_readback_requires_matching_open_and_uses_exclusive_cursor():
    events = make_offline_fixture_envelopes()
    source = InMemoryReplaySource(events)
    with pytest.raises(RuntimeError):
        list(source.iter_events())
    with pytest.raises(KeyError):
        source.open(UUID(int=0))
    source.open(OFFLINE_FIXTURE_SESSION_ID)
    assert list(source.iter_events(after_ingest_sequence=3)) == list(events[4:])
    assert source.verify_integrity().is_valid


def test_readback_reports_order_session_and_dedupe_failures():
    events = make_offline_fixture_envelopes()
    bad = (
        events[0],
        replace(events[1], ingest_sequence=0, dedupe_key=events[0].dedupe_key),
        replace(events[2], session_id=UUID(int=0)),
    )
    source = InMemoryReplaySource(bad)
    source.open(OFFLINE_FIXTURE_SESSION_ID)
    report = source.verify_integrity()
    assert not report.is_valid
    assert any("strictly increasing" in error for error in report.errors)
    assert any("duplicate dedupe_key" in error for error in report.errors)
    assert any("different session_id" in error for error in report.errors)
    assert [event.ingest_sequence for event in source.iter_events()] == [0, 0, 2]
