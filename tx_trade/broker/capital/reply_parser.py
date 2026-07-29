"""Strict parser for Capital domestic-futures ``OnNewData`` replies."""

from __future__ import annotations

import csv
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from io import StringIO
from typing import TypeAlias

from tx_trade.broker.capital.trading_contracts import (
    CAPITAL_ON_NEW_DATA_FIELD_COUNT,
    CapitalOnNewDataRecord,
    CapitalRawReplyCallback,
)
from tx_trade.orders.live_contracts import (
    BrokerCorrelation,
    BrokerOrderEventType,
    CorrelationStatus,
    LiveFailureCode,
    LiveSide,
    NormalizedBrokerFillEvent,
    NormalizedBrokerOrderEvent,
)

NormalizedCapitalReply: TypeAlias = NormalizedBrokerOrderEvent | NormalizedBrokerFillEvent

_PACKED_FUTURES = re.compile(r"^(?P<side>[BS])(?P<offset>[YNO7])(?P<tif>[IRF])(?P<price>[1-5])$")
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
_DATE = re.compile(r"^\d{8}$")
_TIME = re.compile(r"^\d{2}:\d{2}:\d{2}$")
_ORDER_TYPES = frozenset("NCUPDBS")
_ORDER_ERRORS = frozenset({"N", "Y", "T"})
_EVENT_DOMAIN = "tx_trade.capital.on_new_data.event.v1"


class CapitalReplyParseError(ValueError):
    """A deliberately sanitized broker reply parsing failure."""


def _invalid(reason: str) -> CapitalReplyParseError:
    return CapitalReplyParseError(f"invalid Capital domestic futures reply ({reason})")


def _parse_csv(payload: str) -> tuple[str, ...]:
    try:
        rows = list(csv.reader(StringIO(payload), strict=True))
    except csv.Error:
        raise _invalid("malformed_csv") from None
    if len(rows) != 1:
        raise _invalid("record_count")
    values = tuple(rows[0])
    if len(values) != CAPITAL_ON_NEW_DATA_FIELD_COUNT:
        raise _invalid("schema mismatch")
    return values


def _positive_decimal(value: str, field_name: str, *, required: bool) -> Decimal | None:
    if not value:
        if required:
            raise _invalid(f"{field_name}_missing")
        return None
    if value != value.strip():
        raise _invalid(f"{field_name}_format")
    try:
        parsed = Decimal(value)
    except InvalidOperation:
        raise _invalid(f"{field_name}_format") from None
    if not parsed.is_finite() or parsed <= 0:
        raise _invalid(f"{field_name}_range")
    return parsed


def _occurred_at(record: CapitalOnNewDataRecord, received_at: datetime) -> datetime | None:
    if not record.Date and not record.Time:
        return None
    from zoneinfo import ZoneInfo

    if not _DATE.fullmatch(record.Date) or not _TIME.fullmatch(record.Time):
        raise _invalid("broker_time_format")
    try:
        local = datetime.strptime(f"{record.Date} {record.Time}", "%Y%m%d %H:%M:%S").replace(
            tzinfo=ZoneInfo("Asia/Taipei")
        )
    except ValueError:
        raise _invalid("broker_time_value") from None
    occurred_at = local.astimezone(received_at.tzinfo)
    if occurred_at > received_at:
        raise _invalid("broker_time_future")
    return occurred_at


def _identifier(value: str, field_name: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise _invalid(f"{field_name}_format")
    return value


def _packed_side(record: CapitalOnNewDataRecord) -> LiveSide | None:
    # The TF row documents positions [0] through [3]. Its [4] cell is N/A,
    # meaning there is no fifth encoded character for domestic futures.
    matched = _PACKED_FUTURES.fullmatch(record.BuySell)
    if matched is None:
        failed_cancel = record.Type == "C" and record.OrderErr in {"Y", "T"}
        if failed_cancel and re.fullmatch(r"^0[YNO7][IRF][1-5]$", record.BuySell):
            return None
        raise _invalid("buy_sell_format")
    return LiveSide.BUY if matched.group("side") == "B" else LiveSide.SELL


def _correlation_status(record: CapitalOnNewDataRecord) -> CorrelationStatus:
    if record.KeyNo and record.SeqNo and record.KeyNo != record.SeqNo:
        return CorrelationStatus.AMBIGUOUS
    return CorrelationStatus.CANDIDATE


def _order_sequence(record: CapitalOnNewDataRecord) -> str | None:
    return record.SeqNo or record.KeyNo or None


def _fill_id(record: CapitalOnNewDataRecord) -> str | None:
    return record.ExecutionNo or record.OkSeq or None


def _event_id(record: CapitalOnNewDataRecord) -> str:
    """Hash only broker evidence, never adapter generation/sequence or local IDs."""

    evidence: tuple[str, ...]
    if record.Type == "D":
        evidence = (
            record.MarketType,
            record.Type,
            record.Broker,
            record.CustNo,
            record.ComId,
            record.SeqNo or record.KeyNo,
            record.OrderNo,
            record.ExecutionNo or record.OkSeq,
            record.Price1,
            record.Qty,
        )
        domain = f"{_EVENT_DOMAIN}.fill"
    else:
        evidence = (
            record.MarketType,
            record.Type,
            record.OrderErr,
            record.Broker,
            record.CustNo,
            record.ComId,
            record.SeqNo or record.KeyNo,
            record.OrderNo,
            record.MsgNo,
            record.Price,
            record.Qty,
        )
        domain = f"{_EVENT_DOMAIN}.order"
    framed = "\x1f".join(f"{len(item)}:{item}" for item in evidence)
    return f"capital-{sha256(f'{domain}\x1e{framed}'.encode()).hexdigest()}"


def _correlation(
    callback: CapitalRawReplyCallback,
    record: CapitalOnNewDataRecord,
) -> BrokerCorrelation:
    order_sequence = _order_sequence(record)
    broker_book_no = record.OrderNo or None
    fill_id = _fill_id(record) if record.Type == "D" else None
    if order_sequence is None and broker_book_no is None and fill_id is None:
        raise _invalid("correlation_missing")
    try:
        return BrokerCorrelation(
            broker_session_generation=callback.broker_session_generation,
            adapter_received_sequence=callback.adapter_received_sequence,
            status=_correlation_status(record),
            correlated_at=callback.received_at,
            broker_order_sequence=order_sequence,
            broker_book_no=broker_book_no,
            broker_fill_id=fill_id,
            execution_no=record.ExecutionNo or None,
            client_order_id=None,
        )
    except (TypeError, ValueError):
        raise _invalid("correlation_format") from None


def parse_domestic_futures_reply(
    callback: CapitalRawReplyCallback,
    *,
    account_id: str,
) -> NormalizedCapitalReply:
    """Parse one captured callback and normalize it to the fixed live contracts."""

    if type(callback) is not CapitalRawReplyCallback:
        raise TypeError("callback must be CapitalRawReplyCallback")
    record = CapitalOnNewDataRecord.from_fields(_parse_csv(callback.payload))
    if record.MarketType != "TF":
        raise _invalid("market_type")
    if record.Type not in _ORDER_TYPES:
        raise _invalid("reply_type")
    if record.OrderErr not in _ORDER_ERRORS:
        raise _invalid("order_error")

    side = _packed_side(record)
    occurred_at = _occurred_at(record, callback.received_at)
    account_id = _identifier(account_id, "account")
    instrument_id = _identifier(record.ComId, "instrument")
    correlation = _correlation(callback, record)
    event_id = _event_id(record)

    try:
        if record.Type == "D":
            if any((record.ComId2, record.YearMonth2, record.StrikePrice2, record.Price2)):
                raise _invalid("multi_leg_fill_unsupported")
            if record.OrderErr == "Y":
                raise _invalid("fill_failure")
        if record.Type == "D" and record.OrderErr == "N":
            if side is None:
                raise _invalid("fill_side_missing")
            quantity = _positive_decimal(record.Qty, "quantity", required=True)
            execution_price = _positive_decimal(record.Price1, "price1", required=True)
            if quantity is None or execution_price is None:
                raise _invalid("fill_numeric")
            if _fill_id(record) is None:
                raise _invalid("fill_id_missing")
            return NormalizedBrokerFillEvent(
                event_id=event_id,
                account_id=account_id,
                instrument_id=instrument_id,
                side=side,
                quantity=quantity,
                execution_price=execution_price,
                received_at=callback.received_at,
                broker_session_generation=callback.broker_session_generation,
                adapter_received_sequence=callback.adapter_received_sequence,
                correlation=correlation,
                occurred_at=occurred_at,
            )

        decreased_quantity: Decimal | None = None
        new_limit_price: Decimal | None = None
        event_type: BrokerOrderEventType
        failure_code: LiveFailureCode | None
        if record.OrderErr == "T":
            event_type = BrokerOrderEventType.OUTCOME_UNKNOWN
            failure_code = LiveFailureCode.BROKER_TIMEOUT
        elif record.Type == "N":
            event_type = (
                BrokerOrderEventType.NEW_ACCEPTED
                if record.OrderErr == "N"
                else BrokerOrderEventType.NEW_REJECTED
            )
            failure_code = None if record.OrderErr == "N" else LiveFailureCode.BROKER_REJECTED
        elif record.Type == "C":
            event_type = (
                BrokerOrderEventType.CANCELLED
                if record.OrderErr == "N"
                else BrokerOrderEventType.CANCEL_REJECTED
            )
            failure_code = None if record.OrderErr == "N" else LiveFailureCode.CANCEL_REJECTED
        elif record.Type == "S":
            event_type = BrokerOrderEventType.DYNAMIC_CANCELLED
            failure_code = None
        elif record.Type in {"U", "P", "B"} and record.OrderErr == "Y":
            event_type = BrokerOrderEventType.AMEND_REJECTED
            failure_code = LiveFailureCode.AMEND_REJECTED
        elif record.Type == "U":
            event_type = BrokerOrderEventType.QUANTITY_DECREASED
            failure_code = None
            decreased_quantity = _positive_decimal(record.Qty, "decreased_quantity", required=True)
        elif record.Type == "P":
            event_type = BrokerOrderEventType.PRICE_AMENDED
            failure_code = None
            new_limit_price = _positive_decimal(record.Price, "price", required=True)
        elif record.Type == "B":
            event_type = BrokerOrderEventType.PRICE_AND_QUANTITY_AMENDED
            failure_code = None
            decreased_quantity = _positive_decimal(record.Qty, "decreased_quantity", required=True)
            new_limit_price = _positive_decimal(record.Price, "price", required=True)
        else:
            raise _invalid("unsupported_semantics")
        return NormalizedBrokerOrderEvent(
            event_id=event_id,
            account_id=account_id,
            instrument_id=instrument_id,
            event_type=event_type,
            received_at=callback.received_at,
            broker_session_generation=callback.broker_session_generation,
            adapter_received_sequence=callback.adapter_received_sequence,
            correlation=correlation,
            occurred_at=occurred_at,
            failure_code=failure_code,
            decreased_quantity=decreased_quantity,
            new_limit_price=new_limit_price,
        )
    except CapitalReplyParseError:
        raise
    except (TypeError, ValueError):
        raise _invalid("normalized_contract") from None


class CapitalReplyParser:
    """Stateless parser object for dependency-injected adapter composition."""

    def parse(
        self,
        callback: CapitalRawReplyCallback,
        *,
        account_id: str,
    ) -> NormalizedCapitalReply:
        return parse_domestic_futures_reply(callback, account_id=account_id)


parse_on_new_data = parse_domestic_futures_reply

__all__ = [
    "CapitalReplyParseError",
    "CapitalReplyParser",
    "NormalizedCapitalReply",
    "parse_domestic_futures_reply",
    "parse_on_new_data",
]
