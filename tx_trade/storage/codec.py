"""Canonical, collision-free codec for authoritative market-data storage."""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import fields, is_dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping, cast
from uuid import UUID

from tx_trade.market_data.models import (
    AdapterDiagnostic,
    ConnectionState,
    ConnectionStatus,
    DomainPayload,
    EventType,
    Instrument,
    MarketDataEnvelope,
    Quote,
    ServerTime,
    SourceMode,
    TAIPEI,
    Tick,
)

_TYPE_KEY = "$tx-storage-type"


def encode_storage_value(value: Any) -> Any:
    """Encode every composite value with an unambiguous storage-owned tag."""
    if is_dataclass(value) and not isinstance(value, type):
        value = {field.name: getattr(value, field.name) for field in fields(value)}
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return {
            _TYPE_KEY: "bytes",
            "data": base64.b64encode(value).decode("ascii"),
        }
    if isinstance(value, Mapping):
        if any(type(key) is not str for key in value):
            raise TypeError("storage mapping keys must be strings")
        items = [[key, encode_storage_value(value[key])] for key in sorted(value)]
        return {_TYPE_KEY: "map", "items": items}
    if isinstance(value, (tuple, list)):
        return {
            _TYPE_KEY: "sequence",
            "items": [encode_storage_value(item) for item in value],
        }
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        raise TypeError("binary float is not supported")
    raise TypeError(f"unsupported storage value: {type(value).__name__}")


def decode_storage_value(value: Any) -> Any:
    if not isinstance(value, dict):
        return value
    kind = value.get(_TYPE_KEY)
    if kind == "bytes" and set(value) == {_TYPE_KEY, "data"}:
        return base64.b64decode(value["data"], validate=True)
    if kind == "sequence" and set(value) == {_TYPE_KEY, "items"}:
        return [decode_storage_value(item) for item in value["items"]]
    if kind == "map" and set(value) == {_TYPE_KEY, "items"}:
        result: dict[str, Any] = {}
        for pair in value["items"]:
            if not isinstance(pair, list) or len(pair) != 2 or type(pair[0]) is not str:
                raise ValueError("invalid encoded mapping entry")
            if pair[0] in result:
                raise ValueError("duplicate encoded mapping key")
            result[pair[0]] = decode_storage_value(pair[1])
        return result
    raise ValueError("invalid or unknown storage composite tag")


def canonical_json(value: object) -> str:
    return json.dumps(
        encode_storage_value(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def payload_sha256(payload_json: str) -> str:
    return hashlib.sha256(payload_json.encode("utf-8")).hexdigest()


_RECORD_FIELDS = (
    "session_id",
    "ingest_sequence",
    "schema_version",
    "event_type",
    "source",
    "source_mode",
    "connection_generation",
    "sequence",
    "broker_sequence",
    "dedupe_key",
    "event_at",
    "trading_day",
    "received_at",
    "metadata_version",
    "payload_json",
    "raw_json",
    "payload_sha256",
)


def record_sha256(row: Mapping[str, Any]) -> str:
    material = [row[field] for field in _RECORD_FIELDS]
    canonical = json.dumps(material, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def encode_envelope(
    envelope: MarketDataEnvelope,
) -> tuple[str, str | None, str]:
    payload_json = canonical_json(envelope.payload)
    raw_json = None if envelope.raw_payload is None else canonical_json(envelope.raw_payload)
    return payload_json, raw_json, payload_sha256(payload_json)


def _datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("stored datetime must be timezone-aware")
    if parsed.utcoffset() != timedelta(hours=8):
        raise ValueError("stored datetime must have +08:00 offset")
    return parsed.astimezone(TAIPEI)


def _date(value: str | None) -> date | None:
    return None if value is None else date.fromisoformat(value)


def _decimal(value: str | None) -> Decimal | None:
    return None if value is None else Decimal(value)


def _payload(event_type: EventType, data: Mapping[str, Any]) -> DomainPayload:
    if event_type is EventType.CONNECTION_STATUS:
        return ConnectionStatus(
            state=ConnectionState(data["state"]),
            broker_kind_raw=data["broker_kind_raw"],
            broker_code_raw=data["broker_code_raw"],
            message=data["message"],
            is_ready=data["is_ready"],
            changed_at=cast(datetime, _datetime(data["changed_at"])),
            connection_generation=data["connection_generation"],
        )
    if event_type is EventType.SERVER_TIME:
        return ServerTime(
            event_at=_datetime(data["event_at"]),
            hour_raw=data["hour_raw"],
            minute_raw=data["minute_raw"],
            second_raw=data["second_raw"],
            total_raw=data["total_raw"],
            received_at=cast(datetime, _datetime(data["received_at"])),
            trading_day=_date(data["trading_day"]),
        )
    if event_type is EventType.INSTRUMENT:
        return Instrument(
            instrument_id=data["instrument_id"],
            symbol=data["symbol"],
            venue=data["venue"],
            market_no=data["market_no"],
            stock_idx=data["stock_idx"],
            display_name=data["display_name"],
            asset_class=data["asset_class"],
            currency=data["currency"],
            price_scale=_decimal(data["price_scale"]),
            quantity_scale=_decimal(data["quantity_scale"]),
            metadata_version=data["metadata_version"],
            updated_at=cast(datetime, _datetime(data["updated_at"])),
            raw_payload=data["raw_payload"],
        )
    if event_type is EventType.QUOTE:
        return Quote(
            instrument_id=data["instrument_id"],
            market_no_raw=data["market_no_raw"],
            stock_idx_raw=data["stock_idx_raw"],
            bid_raw=data["bid_raw"],
            ask_raw=data["ask_raw"],
            last_raw=data["last_raw"],
            bid_normalized=_decimal(data["bid_normalized"]),
            ask_normalized=_decimal(data["ask_normalized"]),
            last_normalized=_decimal(data["last_normalized"]),
            bid_qty_raw=data["bid_qty_raw"],
            ask_qty_raw=data["ask_qty_raw"],
            last_qty_raw=data["last_qty_raw"],
            event_at=_datetime(data["event_at"]),
            received_at=cast(datetime, _datetime(data["received_at"])),
            trading_day=_date(data["trading_day"]),
            is_simulated=data["is_simulated"],
            is_long_callback=data["is_long_callback"],
            price_scale=_decimal(data["price_scale"]),
        )
    if event_type is EventType.TICK:
        return Tick(
            instrument_id=data["instrument_id"],
            market_no_raw=data["market_no_raw"],
            stock_idx_raw=data["stock_idx_raw"],
            source_pointer_raw=data["source_pointer_raw"],
            date_raw=data["date_raw"],
            time_hms_raw=data["time_hms_raw"],
            time_subsecond_raw=data["time_subsecond_raw"],
            bid_raw=data["bid_raw"],
            ask_raw=data["ask_raw"],
            close_raw=data["close_raw"],
            bid_normalized=_decimal(data["bid_normalized"]),
            ask_normalized=_decimal(data["ask_normalized"]),
            close_normalized=_decimal(data["close_normalized"]),
            quantity_raw=data["quantity_raw"],
            quantity_normalized=_decimal(data["quantity_normalized"]),
            simulate_raw=data["simulate_raw"],
            is_simulated=data["is_simulated"],
            event_at=_datetime(data["event_at"]),
            received_at=cast(datetime, _datetime(data["received_at"])),
            trading_day=_date(data["trading_day"]),
            is_long_callback=data["is_long_callback"],
            price_scale=_decimal(data["price_scale"]),
            quantity_scale=_decimal(data["quantity_scale"]),
        )
    if event_type is EventType.ADAPTER_DIAGNOSTIC:
        return AdapterDiagnostic(
            diagnostic_kind=data["diagnostic_kind"],
            market_no_raw=data["market_no_raw"],
            stock_idx_raw=data["stock_idx_raw"],
            error_code_raw=data["error_code_raw"],
            message=data["message"],
            received_at=cast(datetime, _datetime(data["received_at"])),
            attempt=data["attempt"],
            connection_generation=data["connection_generation"],
            callback_sequence=data["callback_sequence"],
            raw_notification=data["raw_notification"],
        )
    raise ValueError(f"unsupported event type: {event_type}")


def decode_envelope(row: Mapping[str, Any]) -> MarketDataEnvelope:
    payload_json = row["payload_json"]
    if payload_sha256(payload_json) != row["payload_sha256"]:
        raise ValueError("payload checksum mismatch")
    if record_sha256(row) != row["record_sha256"]:
        raise ValueError("authoritative record checksum mismatch")
    event_type = EventType(row["event_type"])
    data = decode_storage_value(json.loads(payload_json))
    raw = None if row["raw_json"] is None else decode_storage_value(json.loads(row["raw_json"]))
    if not isinstance(data, Mapping):
        raise ValueError("payload must decode to a mapping")
    return MarketDataEnvelope(
        schema_version=row["schema_version"],
        event_type=event_type,
        payload=_payload(event_type, data),
        source=row["source"],
        source_mode=SourceMode(row["source_mode"]),
        session_id=UUID(row["session_id"]),
        ingest_sequence=row["ingest_sequence"],
        connection_generation=row["connection_generation"],
        sequence=row["sequence"],
        broker_sequence=row["broker_sequence"],
        dedupe_key=row["dedupe_key"],
        event_at=_datetime(row["event_at"]),
        received_at=cast(datetime, _datetime(row["received_at"])),
        trading_day=_date(row["trading_day"]),
        metadata_version=row["metadata_version"],
        raw_payload=raw,
    )
