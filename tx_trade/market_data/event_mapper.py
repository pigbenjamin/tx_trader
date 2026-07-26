"""Production mapping from lossless captured values to Phase 1 envelopes."""

from __future__ import annotations

import base64
import hashlib
import json
from typing import Any

from .models import (
    SCHEMA_VERSION,
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
    ConnectionStatus,
    EventType,
    MarketDataEnvelope,
    Quote,
    ServerTime,
    Tick,
    build_adapter_diagnostic_dedupe_key,
    to_primitive,
)

_CONNECTION_STATES = {
    3001: (ConnectionState.CONNECTED, False),
    3002: (ConnectionState.DISCONNECTED, False),
    3003: (ConnectionState.STOCKS_READY, True),
}
_STOCK_LIST_MESSAGE = "stock list retained as raw notification"


def _instrument_id(event: CapturedMarketDataEvent, market_no: int, stock_idx: int) -> str:
    """Return an explicitly synthetic identity without inventing a symbol."""

    return f"synthetic:{event.source}:{market_no}:{stock_idx}"


def _dedupe_key(
    event: CapturedMarketDataEvent, event_type: EventType, identity: Any
) -> str:
    canonical = json.dumps(
        [
            event.source,
            str(event.session_id),
            event.connection_generation,
            event_type.value,
            to_primitive(identity),
        ],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return f"{event_type.value}:sha256:{digest}"


class Phase1CapturedEventMapper:
    """Strict mapper that never guesses price, quantity, symbol, or time metadata."""

    def validate(self, event: CapturedMarketDataEvent) -> None:
        if type(event) is not CapturedMarketDataEvent:
            raise TypeError("event must be exactly CapturedMarketDataEvent")
        if event.captured_kind in {
            CapturedKind.ADAPTER_DIAGNOSTIC,
            CapturedKind.STOCK_LIST_NOTIFICATION,
        } and (event.event_at is not None or event.trading_day is not None):
            raise ValueError("diagnostic capture must not carry event time metadata")

    def build_envelope(
        self, event: CapturedMarketDataEvent, ingest_sequence: int
    ) -> MarketDataEnvelope:
        self.validate(event)
        payload = event.payload
        if type(payload) is CapturedConnectionNotification:
            state, ready = _CONNECTION_STATES.get(
                payload.broker_kind_raw, (ConnectionState.ERROR, False)
            )
            domain_payload = ConnectionStatus(
                state=state,
                broker_kind_raw=payload.broker_kind_raw,
                broker_code_raw=payload.broker_code_raw,
                message=None,
                is_ready=ready and payload.broker_code_raw == 0,
                changed_at=event.event_at or event.received_at,
                connection_generation=event.connection_generation,
            )
            event_type = EventType.CONNECTION_STATUS
            identity = (
                payload.broker_kind_raw,
                payload.broker_code_raw,
                event.sequence,
            )
        elif type(payload) is CapturedServerTimeNotification:
            domain_payload = ServerTime(
                event_at=event.event_at,
                hour_raw=payload.hour_raw,
                minute_raw=payload.minute_raw,
                second_raw=payload.second_raw,
                total_raw=payload.total_raw,
                received_at=payload.received_at,
                trading_day=event.trading_day,
            )
            event_type = EventType.SERVER_TIME
            identity = (
                payload.hour_raw,
                payload.minute_raw,
                payload.second_raw,
                payload.total_raw,
                event.sequence,
            )
        elif type(payload) is CapturedQuoteSnapshot:
            domain_payload = Quote(
                instrument_id=_instrument_id(
                    event, payload.market_no_raw, payload.stock_idx_raw
                ),
                market_no_raw=payload.market_no_raw,
                stock_idx_raw=payload.stock_idx_raw,
                bid_raw=payload.bid_raw,
                ask_raw=payload.ask_raw,
                last_raw=payload.last_raw,
                bid_normalized=None,
                ask_normalized=None,
                last_normalized=None,
                bid_qty_raw=payload.bid_qty_raw,
                ask_qty_raw=payload.ask_qty_raw,
                last_qty_raw=payload.last_qty_raw,
                event_at=event.event_at,
                received_at=payload.received_at,
                trading_day=event.trading_day,
                is_simulated=None,
                is_long_callback=payload.is_long_callback,
                price_scale=None,
            )
            event_type = EventType.QUOTE
            identity = (
                payload.market_no_raw,
                payload.stock_idx_raw,
                payload.callback_sequence,
            )
        elif type(payload) is CapturedTickNotification:
            domain_payload = Tick(
                instrument_id=_instrument_id(
                    event, payload.market_no_raw, payload.stock_idx_raw
                ),
                market_no_raw=payload.market_no_raw,
                stock_idx_raw=payload.stock_idx_raw,
                source_pointer_raw=payload.source_pointer_raw,
                date_raw=payload.date_raw,
                time_hms_raw=payload.time_hms_raw,
                time_subsecond_raw=payload.time_subsecond_raw,
                bid_raw=payload.bid_raw,
                ask_raw=payload.ask_raw,
                close_raw=payload.close_raw,
                bid_normalized=None,
                ask_normalized=None,
                close_normalized=None,
                quantity_raw=payload.quantity_raw,
                quantity_normalized=None,
                simulate_raw=payload.simulate_raw,
                is_simulated=None,
                event_at=event.event_at,
                received_at=payload.received_at,
                trading_day=event.trading_day,
                is_long_callback=payload.is_long_callback,
                price_scale=None,
                quantity_scale=None,
            )
            event_type = EventType.TICK
            identity = (
                payload.market_no_raw,
                payload.stock_idx_raw,
                payload.source_pointer_raw,
                payload.callback_sequence,
            )
        elif type(payload) is CapturedStockListNotification:
            raw_list: str | dict[str, str]
            if type(payload.stock_list_raw) is bytes:
                raw_list = {
                    "encoding": "base64",
                    "data": base64.b64encode(payload.stock_list_raw).decode("ascii"),
                }
            else:
                raw_list = payload.stock_list_raw
            domain_payload = AdapterDiagnostic(
                diagnostic_kind="stock_list_parse_failure",
                market_no_raw=payload.market_no_raw,
                stock_idx_raw=None,
                error_code_raw=None,
                message=_STOCK_LIST_MESSAGE,
                received_at=payload.received_at,
                attempt=1,
                connection_generation=event.connection_generation,
                callback_sequence=payload.callback_sequence,
                raw_notification={
                    "market_no_raw": payload.market_no_raw,
                    "stock_list_raw": raw_list,
                },
            )
            event_type = EventType.ADAPTER_DIAGNOSTIC
        elif type(payload) is CapturedAdapterDiagnostic:
            domain_payload = AdapterDiagnostic(
                diagnostic_kind=payload.diagnostic_kind,
                market_no_raw=payload.market_no_raw,
                stock_idx_raw=payload.stock_idx_raw,
                error_code_raw=payload.error_code_raw,
                message=payload.message,
                received_at=payload.received_at,
                attempt=payload.attempt,
                connection_generation=payload.connection_generation,
                callback_sequence=payload.callback_sequence,
                raw_notification=payload.raw_notification,
            )
            event_type = EventType.ADAPTER_DIAGNOSTIC
        else:  # pragma: no cover - the capture model validates its closed union.
            raise ValueError("unsupported captured payload")

        if isinstance(domain_payload, AdapterDiagnostic):
            dedupe_key = build_adapter_diagnostic_dedupe_key(
                event.source,
                event.session_id,
                event.connection_generation,
                domain_payload.diagnostic_kind,
                domain_payload.callback_sequence,
                domain_payload.attempt,
            )
        else:
            dedupe_key = _dedupe_key(event, event_type, identity)
        return MarketDataEnvelope(
            schema_version=SCHEMA_VERSION,
            event_type=event_type,
            payload=domain_payload,
            source=event.source,
            source_mode=event.source_mode,
            session_id=event.session_id,
            ingest_sequence=ingest_sequence,
            connection_generation=event.connection_generation,
            sequence=event.sequence,
            broker_sequence=event.broker_sequence,
            dedupe_key=dedupe_key,
            event_at=event.event_at,
            received_at=event.received_at,
            trading_day=event.trading_day,
            metadata_version=event.metadata_version,
            raw_payload=event.raw_payload,
        )
