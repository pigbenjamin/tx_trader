"""Deterministic offline fixtures and replay-ready readback implementation."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterator
from datetime import date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from .models import (
    SCHEMA_VERSION,
    TAIPEI,
    AdapterDiagnostic,
    ConnectionState,
    ConnectionStatus,
    EventType,
    Instrument,
    MarketDataEnvelope,
    Quote,
    ServerTime,
    SourceMode,
    Tick,
    build_adapter_diagnostic_dedupe_key,
    serialize_envelope,
    to_primitive,
)
from .ports import ReadbackIntegrityReport

OFFLINE_FIXTURE_SESSION_ID = UUID("9d2d84b6-38d8-4ec4-b4de-5b3901f31234")
OFFLINE_FIXTURE_TIME = datetime(2026, 7, 26, 9, 30, 0, tzinfo=TAIPEI)
OFFLINE_FIXTURE_TRADING_DAY = date(2026, 7, 26)
_OFFLINE_FIXTURE_SOURCE = "offline-fixture"


def _build_fixture_dedupe_key(
    *,
    source: str,
    session_id: UUID,
    connection_generation: int,
    event_type: EventType,
    identity: object,
) -> str:
    """Hash event identity without coupling it to the ingest cursor."""

    canonical = json.dumps(
        [
            source,
            str(session_id),
            connection_generation,
            event_type.value,
            to_primitive(identity),
        ],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return f"{event_type.value}:sha256:{digest}"


def _require_taipei(value: object) -> None:
    if type(value) is not datetime:
        raise TypeError("initial_now must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("initial_now must be timezone-aware")
    if getattr(value.tzinfo, "key", None) != TAIPEI.key:
        raise ValueError("initial_now must use Asia/Taipei timezone")


def _require_finite_number(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a real number")
    converted = float(value)
    if not math.isfinite(converted):
        raise ValueError(f"{name} must be finite")
    return converted


class FakeClock:
    """A deterministic clock advanced explicitly by tests."""

    def __init__(
        self, initial_now: datetime, *, initial_monotonic: float = 0.0
    ) -> None:
        _require_taipei(initial_now)
        monotonic = _require_finite_number(initial_monotonic, "initial_monotonic")
        self._now = initial_now
        self._monotonic = monotonic

    def now(self) -> datetime:
        return self._now

    def monotonic(self) -> float:
        return self._monotonic

    def advance(self, seconds: float) -> None:
        increment = _require_finite_number(seconds, "seconds")
        if increment < 0:
            raise ValueError("seconds must be non-negative")
        self._now += timedelta(seconds=increment)
        self._monotonic += increment


class InMemoryReplaySource:
    """Immutable readback source; deliberately contains no playback timing."""

    def __init__(self, envelopes: tuple[MarketDataEnvelope, ...]) -> None:
        if type(envelopes) is not tuple:
            raise TypeError("envelopes must be a tuple")
        if not envelopes:
            raise ValueError("envelopes must not be empty")
        for envelope in envelopes:
            if type(envelope) is not MarketDataEnvelope:
                raise TypeError("envelopes must contain MarketDataEnvelope values")
        self._envelopes = envelopes
        self._session_id = envelopes[0].session_id
        self._is_open = False

    def open(self, session_id: UUID) -> None:
        if type(session_id) is not UUID:
            raise TypeError("session_id must be UUID")
        if session_id != self._session_id:
            raise KeyError(f"recording session not found: {session_id}")
        self._is_open = True

    def _require_open(self) -> None:
        if not self._is_open:
            raise RuntimeError("replay source is not open")

    def iter_events(
        self, *, after_ingest_sequence: int | None = None
    ) -> Iterator[MarketDataEnvelope]:
        self._require_open()
        if after_ingest_sequence is not None:
            if type(after_ingest_sequence) is not int:
                raise TypeError("after_ingest_sequence must be an integer or None")
            if after_ingest_sequence < 0:
                raise ValueError("after_ingest_sequence must be non-negative")
        cursor = -1 if after_ingest_sequence is None else after_ingest_sequence
        ordered = sorted(self._envelopes, key=lambda envelope: envelope.ingest_sequence)
        return (
            envelope
            for envelope in ordered
            if envelope.ingest_sequence > cursor
        )

    def verify_integrity(self) -> ReadbackIntegrityReport:
        self._require_open()
        errors: list[str] = []
        previous: int | None = None
        dedupe_keys: set[str] = set()
        for index, envelope in enumerate(self._envelopes):
            if envelope.session_id != self._session_id:
                errors.append(f"event {index} has a different session_id")
            if previous is not None and envelope.ingest_sequence <= previous:
                errors.append("ingest_sequence is not strictly increasing")
            previous = envelope.ingest_sequence
            if envelope.dedupe_key in dedupe_keys:
                errors.append(f"duplicate dedupe_key at event {index}")
            dedupe_keys.add(envelope.dedupe_key)
            first_serialization = serialize_envelope(envelope)
            second_serialization = serialize_envelope(envelope)
            if first_serialization != second_serialization:
                errors.append(f"event {index} serialization is not deterministic")

        sequence_values = [
            envelope.ingest_sequence for envelope in self._envelopes
        ]
        return ReadbackIntegrityReport(
            session_id=self._session_id,
            event_count=len(self._envelopes),
            first_ingest_sequence=min(sequence_values),
            last_ingest_sequence=max(sequence_values),
            is_valid=not errors,
            errors=tuple(errors),
        )


def make_offline_fixture_envelopes() -> tuple[MarketDataEnvelope, ...]:
    """Return six canonical event types with no wall-clock or random inputs."""

    instrument_id = "TAIFEX:0:TX00"
    instrument = Instrument(
        instrument_id=instrument_id,
        symbol="TX00",
        venue="TAIFEX",
        market_no=0,
        stock_idx=7,
        display_name="臺指期近月",
        asset_class="future",
        currency="TWD",
        price_scale=Decimal("0.01"),
        quantity_scale=Decimal("1"),
        metadata_version=1,
        updated_at=OFFLINE_FIXTURE_TIME,
        raw_payload={"symbol": "TX00", "market_no": 0, "stock_idx": 7},
    )
    connection = ConnectionStatus(
        state=ConnectionState.STOCKS_READY,
        broker_kind_raw=3003,
        broker_code_raw=0,
        message="offline fixture ready",
        is_ready=True,
        changed_at=OFFLINE_FIXTURE_TIME,
        connection_generation=0,
    )
    server_time = ServerTime(
        event_at=OFFLINE_FIXTURE_TIME,
        hour_raw=9,
        minute_raw=30,
        second_raw=0,
        total_raw=93000,
        received_at=OFFLINE_FIXTURE_TIME,
        trading_day=OFFLINE_FIXTURE_TRADING_DAY,
    )
    quote = Quote(
        instrument_id=instrument_id,
        market_no_raw=0,
        stock_idx_raw=7,
        bid_raw=2000000,
        ask_raw=2000200,
        last_raw=2000100,
        bid_normalized=Decimal("20000.00"),
        ask_normalized=Decimal("20002.00"),
        last_normalized=Decimal("20001.00"),
        bid_qty_raw=3,
        ask_qty_raw=4,
        last_qty_raw=1,
        event_at=OFFLINE_FIXTURE_TIME,
        received_at=OFFLINE_FIXTURE_TIME,
        trading_day=OFFLINE_FIXTURE_TRADING_DAY,
        is_simulated=False,
        is_long_callback=True,
        price_scale=Decimal("0.01"),
    )
    tick = Tick(
        instrument_id=instrument_id,
        market_no_raw=0,
        stock_idx_raw=7,
        source_pointer_raw=42,
        date_raw=20260726,
        time_hms_raw=93000,
        time_subsecond_raw=123,
        bid_raw=2000000,
        ask_raw=2000200,
        close_raw=2000100,
        bid_normalized=Decimal("20000.00"),
        ask_normalized=Decimal("20002.00"),
        close_normalized=Decimal("20001.00"),
        quantity_raw=2,
        quantity_normalized=Decimal("2"),
        simulate_raw=0,
        is_simulated=False,
        event_at=OFFLINE_FIXTURE_TIME,
        received_at=OFFLINE_FIXTURE_TIME,
        trading_day=OFFLINE_FIXTURE_TRADING_DAY,
        is_long_callback=True,
        price_scale=Decimal("0.01"),
        quantity_scale=Decimal("1"),
    )
    diagnostic = AdapterDiagnostic(
        diagnostic_kind="adapter_error",
        market_no_raw=0,
        stock_idx_raw=7,
        error_code_raw=-1,
        message="deterministic offline diagnostic",
        received_at=OFFLINE_FIXTURE_TIME,
        attempt=1,
        connection_generation=0,
        callback_sequence=5,
        raw_notification={"fixture": True},
    )
    payloads = (
        (
            EventType.CONNECTION_STATUS,
            connection,
            OFFLINE_FIXTURE_TIME,
            None,
            None,
            {
                "callback_sequence": 0,
                "state": connection.state,
                "broker_kind_raw": connection.broker_kind_raw,
                "broker_code_raw": connection.broker_code_raw,
            },
        ),
        (
            EventType.SERVER_TIME,
            server_time,
            OFFLINE_FIXTURE_TIME,
            OFFLINE_FIXTURE_TRADING_DAY,
            None,
            {
                "hour_raw": server_time.hour_raw,
                "minute_raw": server_time.minute_raw,
                "second_raw": server_time.second_raw,
                "total_raw": server_time.total_raw,
                "trading_day": server_time.trading_day,
            },
        ),
        (
            EventType.INSTRUMENT,
            instrument,
            OFFLINE_FIXTURE_TIME,
            None,
            1,
            {
                "instrument_id": instrument.instrument_id,
                "metadata_version": instrument.metadata_version,
            },
        ),
        (
            EventType.QUOTE,
            quote,
            OFFLINE_FIXTURE_TIME,
            OFFLINE_FIXTURE_TRADING_DAY,
            1,
            {
                "instrument_id": quote.instrument_id,
                "market_no_raw": quote.market_no_raw,
                "stock_idx_raw": quote.stock_idx_raw,
                "callback_sequence": 3,
                "bid_raw": quote.bid_raw,
                "ask_raw": quote.ask_raw,
                "last_raw": quote.last_raw,
                "bid_qty_raw": quote.bid_qty_raw,
                "ask_qty_raw": quote.ask_qty_raw,
                "last_qty_raw": quote.last_qty_raw,
                "event_at": quote.event_at,
                "received_at": quote.received_at,
            },
        ),
        (
            EventType.TICK,
            tick,
            OFFLINE_FIXTURE_TIME,
            OFFLINE_FIXTURE_TRADING_DAY,
            1,
            {
                "instrument_id": tick.instrument_id,
                "market_no_raw": tick.market_no_raw,
                "stock_idx_raw": tick.stock_idx_raw,
                "source_pointer_raw": tick.source_pointer_raw,
                "date_raw": tick.date_raw,
                "time_hms_raw": tick.time_hms_raw,
                "time_subsecond_raw": tick.time_subsecond_raw,
                "bid_raw": tick.bid_raw,
                "ask_raw": tick.ask_raw,
                "close_raw": tick.close_raw,
                "quantity_raw": tick.quantity_raw,
                "simulate_raw": tick.simulate_raw,
            },
        ),
        (
            EventType.ADAPTER_DIAGNOSTIC,
            diagnostic,
            None,
            None,
            None,
            None,
        ),
    )
    envelopes: list[MarketDataEnvelope] = []
    for index, (
        event_type,
        payload,
        event_at,
        trading_day,
        metadata_version,
        identity,
    ) in enumerate(payloads):
        if event_type is EventType.ADAPTER_DIAGNOSTIC:
            assert isinstance(payload, AdapterDiagnostic)
            dedupe_key = build_adapter_diagnostic_dedupe_key(
                _OFFLINE_FIXTURE_SOURCE,
                OFFLINE_FIXTURE_SESSION_ID,
                payload.connection_generation,
                payload.diagnostic_kind,
                payload.callback_sequence,
                payload.attempt,
            )
        else:
            dedupe_key = _build_fixture_dedupe_key(
                source=_OFFLINE_FIXTURE_SOURCE,
                session_id=OFFLINE_FIXTURE_SESSION_ID,
                connection_generation=0,
                event_type=event_type,
                identity=identity,
            )
        envelopes.append(
            MarketDataEnvelope(
                schema_version=SCHEMA_VERSION,
                event_type=event_type,
                payload=payload,
                source=_OFFLINE_FIXTURE_SOURCE,
                source_mode=SourceMode.OFFLINE,
                session_id=OFFLINE_FIXTURE_SESSION_ID,
                ingest_sequence=index,
                connection_generation=0,
                sequence=index,
                broker_sequence=None,
                dedupe_key=dedupe_key,
                event_at=event_at,
                received_at=OFFLINE_FIXTURE_TIME,
                trading_day=trading_day,
                metadata_version=metadata_version,
                raw_payload={"fixture_index": index},
            )
        )
    return tuple(envelopes)
