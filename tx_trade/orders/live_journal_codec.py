"""Strict, side-effect-free codec for the durable live-order journal."""

from __future__ import annotations

import base64
import binascii
from dataclasses import fields, is_dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from hashlib import sha256
import json
import re
from types import UnionType
from typing import Any, Callable, TypeVar, cast, get_args

from .live_contracts import (
    AmendOrderCommand,
    BrokerCorrelation,
    CancelOrderCommand,
    DecreaseOrderCommand,
    DispatchReceipt,
    DispatchState,
    LiveCommandKind,
    LiveFailureCode,
    LiveFill,
    LiveOrder,
    LiveOrderIntent,
    LiveOrderState,
    LiveOrderType,
    LiveSide,
    LiveTimeInForce,
    NewOrderCommand,
    NormalizedBrokerFillEvent,
    NormalizedBrokerOrderEvent,
    PendingCommandBinding,
    CorrelationStatus,
    BrokerOrderEventType,
)
from .live_journal_contracts import (
    DurableReconciliationRequirement,
    LiveJournalIdentity,
    LiveJournalRecoverySnapshot,
    OutstandingDispatchClaim,
)
from .live_ports import AmbiguousObservation, RawBrokerObservation
from .live_state_machine import AppliedEvent, AppliedEventLedger

SCHEMA_VERSION = 1
MAX_CODEC_PAYLOAD_BYTES = 1_048_576
MAX_DECIMAL_TEXT_LENGTH = 128
MAX_DECIMAL_DIGITS = 128
MAX_DECIMAL_ABS_EXPONENT = 128
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_DOMAIN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
_DECIMAL_TEXT = re.compile(
    r"^-?(?P<coefficient>(?:0|[1-9][0-9]*)(?:\.[0-9]+)?)(?:e(?P<exponent>-?[0-9]+))?$"
)
_T = TypeVar("_T")


class LiveJournalCodecError(ValueError):
    """A sanitized failure while encoding or decoding journal data."""

    def __init__(self, message: str = "invalid live journal payload") -> None:
        super().__init__(message)


_DATACLASSES: dict[str, type[object]] = {
    cls.__name__: cls
    for cls in (
        LiveOrderIntent,
        NewOrderCommand,
        CancelOrderCommand,
        AmendOrderCommand,
        DecreaseOrderCommand,
        PendingCommandBinding,
        LiveOrder,
        DispatchReceipt,
        BrokerCorrelation,
        NormalizedBrokerOrderEvent,
        NormalizedBrokerFillEvent,
        LiveFill,
        RawBrokerObservation,
        AmbiguousObservation,
        AppliedEvent,
        AppliedEventLedger,
        LiveJournalIdentity,
        OutstandingDispatchClaim,
        DurableReconciliationRequirement,
        LiveJournalRecoverySnapshot,
    )
}
_ENUMS: dict[str, type[Enum]] = {
    cls.__name__: cls
    for cls in (
        LiveSide,
        LiveOrderType,
        LiveTimeInForce,
        LiveOrderState,
        LiveCommandKind,
        DispatchState,
        LiveFailureCode,
        BrokerOrderEventType,
        CorrelationStatus,
    )
}


def _bounded_decimal_text(raw: object) -> bool:
    if type(raw) is not str or len(raw) > MAX_DECIMAL_TEXT_LENGTH:
        return False
    match = _DECIMAL_TEXT.fullmatch(raw)
    if match is None:
        return False
    coefficient = match.group("coefficient")
    if sum(character.isdigit() for character in coefficient) > MAX_DECIMAL_DIGITS:
        return False
    exponent_text = match.group("exponent")
    if exponent_text is None:
        return True
    unsigned_exponent = exponent_text.removeprefix("-")
    return len(unsigned_exponent) <= 3 and abs(int(exponent_text)) <= MAX_DECIMAL_ABS_EXPONENT


def _canonical_decimal(value: Decimal) -> str:
    if not value.is_finite():
        raise LiveJournalCodecError()
    _, digits, exponent = value.as_tuple()
    if (
        len(digits) > MAX_DECIMAL_DIGITS
        or not isinstance(exponent, int)
        or abs(exponent) > MAX_DECIMAL_ABS_EXPONENT
    ):
        raise LiveJournalCodecError()
    if value == 0:
        return "0"
    normalized = value.normalize()
    sign, digits, exponent = normalized.as_tuple()
    if not isinstance(exponent, int):
        raise LiveJournalCodecError()
    coefficient = "".join(str(digit) for digit in digits)
    adjusted = exponent + len(coefficient) - 1
    scientific_coefficient = (
        coefficient if len(coefficient) == 1 else f"{coefficient[0]}.{coefficient[1:]}"
    )
    scientific = f"{scientific_coefficient}e{adjusted}" if adjusted else scientific_coefficient
    point = len(coefficient) + exponent
    if point <= 0:
        plain = f"0.{('0' * -point)}{coefficient}"
    elif point >= len(coefficient):
        plain = f"{coefficient}{('0' * exponent)}"
    else:
        plain = f"{coefficient[:point]}.{coefficient[point:]}"
    result = min(plain, scientific, key=len)
    signed_result = f"-{result}" if sign else result
    if not _bounded_decimal_text(signed_result):
        raise LiveJournalCodecError()
    return signed_result


def _canonical_time(value: datetime) -> str:
    if (
        type(value) is not datetime
        or value.tzinfo is None
        or value.utcoffset() != timezone.utc.utcoffset(value)
    ):
        raise LiveJournalCodecError()
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _node(value: object) -> object:
    if is_dataclass(value) and not isinstance(value, type):
        data_cls = type(value)
        if data_cls.__name__ not in _DATACLASSES or _DATACLASSES[data_cls.__name__] is not data_cls:
            raise LiveJournalCodecError()
        return {
            "$type": data_cls.__name__,
            "$value": {item.name: _node(getattr(value, item.name)) for item in fields(value)},
        }
    if isinstance(value, Enum):
        enum_cls = type(value)
        if enum_cls.__name__ not in _ENUMS or _ENUMS[enum_cls.__name__] is not enum_cls:
            raise LiveJournalCodecError()
        return {"$enum": enum_cls.__name__, "$value": value.value}
    if type(value) is datetime:
        return {"$datetime": _canonical_time(value)}
    if type(value) is Decimal:
        return {"$decimal": _canonical_decimal(value)}
    if type(value) is bytes:
        return {"$bytes": base64.b64encode(value).decode("ascii")}
    if type(value) is tuple:
        return {"$tuple": [_node(item) for item in value]}
    if value is None or type(value) in {str, int, bool}:
        return value
    raise LiveJournalCodecError()


def encode_journal_value(value: object) -> bytes:
    """Encode one supported durable contract to canonical UTF-8 JSON."""

    try:
        root = _node(value)
        if not isinstance(root, dict) or "$type" not in root:
            raise LiveJournalCodecError()
        document = {
            "schema_version": SCHEMA_VERSION,
            "type": root["$type"],
            "value": root["$value"],
        }
        payload = json.dumps(
            document,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        if not payload or len(payload) > MAX_CODEC_PAYLOAD_BYTES:
            raise LiveJournalCodecError()
        return payload
    except LiveJournalCodecError:
        raise
    except Exception:
        raise LiveJournalCodecError() from None


def _keys(value: object, required: set[str]) -> dict[str, object]:
    if type(value) is not dict or set(value) != required:
        raise LiveJournalCodecError()
    return value


def _strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise LiveJournalCodecError()
        result[key] = value
    return result


def _decode_node(value: object) -> object:
    if type(value) is float:
        raise LiveJournalCodecError()
    if value is None or type(value) in {str, int, bool}:
        return value
    if type(value) is list:
        raise LiveJournalCodecError()
    if type(value) is not dict:
        raise LiveJournalCodecError()
    keys = set(value)
    if keys == {"$type", "$value"}:
        type_name = value["$type"]
        members = value["$value"]
        if type(type_name) is not str or type_name not in _DATACLASSES:
            raise LiveJournalCodecError()
        cls = _DATACLASSES[type_name]
        if type(members) is not dict:
            raise LiveJournalCodecError()
        data_fields = fields(cast(Any, cls))
        expected = {item.name for item in data_fields}
        if set(members) != expected:
            raise LiveJournalCodecError()
        decoded = {name: _decode_node(item) for name, item in members.items()}
        try:
            constructor = cast(Callable[..., object], cls)
            result = constructor(
                **{item.name: decoded[item.name] for item in data_fields if item.init}
            )
        except Exception:
            raise LiveJournalCodecError() from None
        if any(
            not item.init and getattr(result, item.name) != decoded[item.name]
            for item in data_fields
        ):
            raise LiveJournalCodecError()
        return result
    if keys == {"$enum", "$value"}:
        enum_name = value["$enum"]
        enum_value = value["$value"]
        if type(enum_name) is not str or enum_name not in _ENUMS or type(enum_value) is not str:
            raise LiveJournalCodecError()
        try:
            return _ENUMS[enum_name](enum_value)
        except (TypeError, ValueError):
            raise LiveJournalCodecError() from None
    if keys == {"$datetime"}:
        raw = value["$datetime"]
        if type(raw) is not str or not raw.endswith("Z"):
            raise LiveJournalCodecError()
        try:
            decoded_datetime = datetime.fromisoformat(f"{raw[:-1]}+00:00")
        except ValueError:
            raise LiveJournalCodecError() from None
        if _canonical_time(decoded_datetime) != raw:
            raise LiveJournalCodecError()
        return decoded_datetime
    if keys == {"$decimal"}:
        raw = value["$decimal"]
        if not _bounded_decimal_text(raw):
            raise LiveJournalCodecError()
        assert type(raw) is str
        try:
            decoded_decimal = Decimal(raw)
        except InvalidOperation:
            raise LiveJournalCodecError() from None
        if _canonical_decimal(decoded_decimal) != raw:
            raise LiveJournalCodecError()
        return decoded_decimal
    if keys == {"$bytes"}:
        raw = value["$bytes"]
        if type(raw) is not str:
            raise LiveJournalCodecError()
        try:
            decoded_bytes = base64.b64decode(raw, validate=True)
        except (ValueError, binascii.Error):
            raise LiveJournalCodecError() from None
        if base64.b64encode(decoded_bytes).decode("ascii") != raw:
            raise LiveJournalCodecError()
        return decoded_bytes
    if keys == {"$tuple"}:
        raw = value["$tuple"]
        if type(raw) is not list:
            raise LiveJournalCodecError()
        return tuple(_decode_node(item) for item in raw)
    raise LiveJournalCodecError()


def _matches_expected(value: object, expected_type: object) -> bool:
    if isinstance(expected_type, UnionType):
        return type(value) in get_args(expected_type)
    if type(expected_type) is tuple:
        return type(value) in expected_type
    return isinstance(expected_type, type) and type(value) is expected_type


def decode_journal_value(
    payload: bytes,
    expected_type: type[_T] | UnionType | tuple[type[object], ...] | None = None,
    *,
    domain: str | None = None,
    expected_digest: str | None = None,
) -> _T | object:
    """Decode a canonical payload, optionally verifying its domain digest."""

    try:
        if type(payload) is not bytes or not payload or len(payload) > MAX_CODEC_PAYLOAD_BYTES:
            raise LiveJournalCodecError()
        if (domain is None) != (expected_digest is None):
            raise LiveJournalCodecError()
        if domain is not None:
            if type(expected_digest) is not str or not _DIGEST.fullmatch(expected_digest):
                raise LiveJournalCodecError()
            if journal_digest(domain, payload) != expected_digest:
                raise LiveJournalCodecError()
        try:
            text = payload.decode("utf-8")
            document = json.loads(
                text,
                parse_float=lambda _: (_ for _ in ()).throw(LiveJournalCodecError()),
                parse_constant=lambda _: (_ for _ in ()).throw(LiveJournalCodecError()),
                object_pairs_hook=_strict_object,
            )
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise LiveJournalCodecError() from None
        root = _keys(document, {"schema_version", "type", "value"})
        if root["schema_version"] != SCHEMA_VERSION or type(root["schema_version"]) is not int:
            raise LiveJournalCodecError()
        if type(root["type"]) is not str or root["type"] not in _DATACLASSES:
            raise LiveJournalCodecError()
        value = _decode_node({"$type": root["type"], "$value": root["value"]})
        if expected_type is not None and not _matches_expected(value, expected_type):
            raise LiveJournalCodecError()
        if encode_journal_value(value) != payload:
            raise LiveJournalCodecError()
        return value
    except LiveJournalCodecError:
        raise
    except Exception:
        raise LiveJournalCodecError() from None


def journal_digest(domain: str, payload: bytes) -> str:
    """Return a domain-separated SHA-256 digest for already encoded bytes."""

    if (
        type(domain) is not str
        or not _DOMAIN.fullmatch(domain)
        or type(payload) is not bytes
        or not payload
    ):
        raise LiveJournalCodecError()
    separator = bytes((0,))
    return f"sha256:{sha256(domain.encode('ascii') + separator + payload).hexdigest()}"


encode_payload = encode_journal_value
decode_payload = decode_journal_value

__all__ = [
    "MAX_CODEC_PAYLOAD_BYTES",
    "MAX_DECIMAL_ABS_EXPONENT",
    "MAX_DECIMAL_DIGITS",
    "MAX_DECIMAL_TEXT_LENGTH",
    "SCHEMA_VERSION",
    "LiveJournalCodecError",
    "decode_journal_value",
    "decode_payload",
    "encode_journal_value",
    "encode_payload",
    "journal_digest",
]
