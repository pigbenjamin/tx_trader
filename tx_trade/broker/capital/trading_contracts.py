"""Side-effect-free contracts for Capital broker trading callbacks.

The callback payload is intentionally retained as private data so a future
journal can persist the original broker evidence without exposing it through
ordinary ``repr`` output.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Final

CAPITAL_ON_NEW_DATA_SCHEMA_VERSION: Final = "capital.on_new_data.v1"
CAPITAL_ON_NEW_DATA_FIELDS: Final = (
    "KeyNo",
    "MarketType",
    "Type",
    "OrderErr",
    "Broker",
    "CustNo",
    "BuySell",
    "ExchangeID",
    "ComId",
    "StrikePrice",
    "OrderNo",
    "Price",
    "Numerator",
    "Denominator",
    "Price1",
    "Numerator1",
    "Denominator1",
    "Price2",
    "Numerator2",
    "Denominator2",
    "Qty",
    "BeforeQty",
    "AfterQty",
    "Date",
    "Time",
    "OkSeq",
    "SubID",
    "SaleNo",
    "Agent",
    "TradeDate",
    "MsgNo",
    "PreOrder",
    "ComId1",
    "YearMonth1",
    "StrikePrice1",
    "ComId2",
    "YearMonth2",
    "StrikePrice2",
    "ExecutionNo",
    "PriceSymbol",
    "Reserved",
    "OrderEffective",
    "CallPut",
    "OrderSeq",
    "ErrorMsg",
    "CancelOrderMarkByExchange",
    "ExchangeTandemMsg",
    "SeqNo",
    "OFSTPFlag",
)
CAPITAL_ON_NEW_DATA_FIELD_COUNT: Final = len(CAPITAL_ON_NEW_DATA_FIELDS)
CAPITAL_TF_REPLY_ON_NEW_DATA_V1_FIELDS: Final = CAPITAL_ON_NEW_DATA_FIELDS
CAPITAL_TF_REPLY_ON_NEW_DATA_V1_FIELD_COUNT: Final = CAPITAL_ON_NEW_DATA_FIELD_COUNT


def _require_utc(value: object, name: str) -> None:
    if type(value) is not datetime:
        raise TypeError(f"{name} must be a datetime")
    assert isinstance(value, datetime)
    if value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value):
        raise ValueError(f"{name} must use UTC")


def _require_positive_int(value: object, name: str) -> None:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive integer")


@dataclass(frozen=True, slots=True)
class CapitalRawReplyCallback:
    """A captured ``OnNewData`` call with adapter-owned ordering metadata."""

    received_at: datetime
    broker_session_generation: int
    adapter_received_sequence: int
    user_id: str = field(repr=False)
    payload: str = field(repr=False)
    schema_version: str = CAPITAL_ON_NEW_DATA_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_utc(self.received_at, "received_at")
        _require_positive_int(self.broker_session_generation, "broker_session_generation")
        _require_positive_int(self.adapter_received_sequence, "adapter_received_sequence")
        if type(self.user_id) is not str or not self.user_id:
            raise ValueError("user_id must be a non-empty string")
        if type(self.payload) is not str or not self.payload:
            raise ValueError("payload must be a non-empty string")
        if self.schema_version != CAPITAL_ON_NEW_DATA_SCHEMA_VERSION:
            raise ValueError("unsupported Capital reply schema version")

    @property
    def generation(self) -> int:
        return self.broker_session_generation

    @property
    def sequence(self) -> int:
        return self.adapter_received_sequence


@dataclass(frozen=True, slots=True)
class CapitalOnNewDataRecord:
    """The exact 49-field layout documented by Capital's ``OnNewData``."""

    KeyNo: str = field(repr=False)
    MarketType: str = field(repr=False)
    Type: str = field(repr=False)
    OrderErr: str = field(repr=False)
    Broker: str = field(repr=False)
    CustNo: str = field(repr=False)
    BuySell: str = field(repr=False)
    ExchangeID: str = field(repr=False)
    ComId: str = field(repr=False)
    StrikePrice: str = field(repr=False)
    OrderNo: str = field(repr=False)
    Price: str = field(repr=False)
    Numerator: str = field(repr=False)
    Denominator: str = field(repr=False)
    Price1: str = field(repr=False)
    Numerator1: str = field(repr=False)
    Denominator1: str = field(repr=False)
    Price2: str = field(repr=False)
    Numerator2: str = field(repr=False)
    Denominator2: str = field(repr=False)
    Qty: str = field(repr=False)
    BeforeQty: str = field(repr=False)
    AfterQty: str = field(repr=False)
    Date: str = field(repr=False)
    Time: str = field(repr=False)
    OkSeq: str = field(repr=False)
    SubID: str = field(repr=False)
    SaleNo: str = field(repr=False)
    Agent: str = field(repr=False)
    TradeDate: str = field(repr=False)
    MsgNo: str = field(repr=False)
    PreOrder: str = field(repr=False)
    ComId1: str = field(repr=False)
    YearMonth1: str = field(repr=False)
    StrikePrice1: str = field(repr=False)
    ComId2: str = field(repr=False)
    YearMonth2: str = field(repr=False)
    StrikePrice2: str = field(repr=False)
    ExecutionNo: str = field(repr=False)
    PriceSymbol: str = field(repr=False)
    Reserved: str = field(repr=False)
    OrderEffective: str = field(repr=False)
    CallPut: str = field(repr=False)
    OrderSeq: str = field(repr=False)
    ErrorMsg: str = field(repr=False)
    CancelOrderMarkByExchange: str = field(repr=False)
    ExchangeTandemMsg: str = field(repr=False)
    SeqNo: str = field(repr=False)
    OFSTPFlag: str = field(repr=False)

    @classmethod
    def from_fields(cls, values: tuple[str, ...]) -> CapitalOnNewDataRecord:
        if len(values) != CAPITAL_ON_NEW_DATA_FIELD_COUNT:
            raise ValueError("Capital OnNewData schema mismatch")
        return cls(*values)


# Short aliases make the callback boundary pleasant to use while retaining
# explicit Capital names for integrations that handle more than one broker.
RawReplyCallback = CapitalRawReplyCallback
OnNewDataRecord = CapitalOnNewDataRecord
ON_NEW_DATA_FIELDS = CAPITAL_ON_NEW_DATA_FIELDS
ON_NEW_DATA_FIELD_COUNT = CAPITAL_ON_NEW_DATA_FIELD_COUNT
ON_NEW_DATA_SCHEMA_VERSION = CAPITAL_ON_NEW_DATA_SCHEMA_VERSION

__all__ = [
    "CAPITAL_ON_NEW_DATA_FIELD_COUNT",
    "CAPITAL_ON_NEW_DATA_FIELDS",
    "CAPITAL_ON_NEW_DATA_SCHEMA_VERSION",
    "CAPITAL_TF_REPLY_ON_NEW_DATA_V1_FIELD_COUNT",
    "CAPITAL_TF_REPLY_ON_NEW_DATA_V1_FIELDS",
    "CapitalOnNewDataRecord",
    "CapitalRawReplyCallback",
    "ON_NEW_DATA_FIELD_COUNT",
    "ON_NEW_DATA_FIELDS",
    "ON_NEW_DATA_SCHEMA_VERSION",
    "OnNewDataRecord",
    "RawReplyCallback",
]
