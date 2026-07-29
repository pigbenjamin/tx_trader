from dataclasses import fields
from datetime import datetime, timezone

import pytest

from tx_trade.broker.capital.trading_contracts import (
    CAPITAL_ON_NEW_DATA_FIELD_COUNT,
    CAPITAL_ON_NEW_DATA_FIELDS,
    CAPITAL_ON_NEW_DATA_SCHEMA_VERSION,
    CAPITAL_TF_REPLY_ON_NEW_DATA_V1_FIELD_COUNT,
    CAPITAL_TF_REPLY_ON_NEW_DATA_V1_FIELDS,
    CapitalOnNewDataRecord,
    CapitalRawReplyCallback,
)

NOW = datetime(2026, 7, 28, 16, 30, tzinfo=timezone.utc)


def test_official_schema_is_versioned_and_has_the_exact_ordered_49_fields():
    expected = (
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
    assert CAPITAL_ON_NEW_DATA_SCHEMA_VERSION == "capital.on_new_data.v1"
    assert CAPITAL_ON_NEW_DATA_FIELD_COUNT == 49
    assert CAPITAL_TF_REPLY_ON_NEW_DATA_V1_FIELD_COUNT == 49
    assert CAPITAL_TF_REPLY_ON_NEW_DATA_V1_FIELDS == expected
    assert CAPITAL_ON_NEW_DATA_FIELDS == expected
    assert tuple(item.name for item in fields(CapitalOnNewDataRecord)) == CAPITAL_ON_NEW_DATA_FIELDS
    assert CAPITAL_ON_NEW_DATA_FIELDS[0] == "KeyNo"
    assert CAPITAL_ON_NEW_DATA_FIELDS[-1] == "OFSTPFlag"


def test_raw_callback_metadata_is_positive_and_private_payload_has_safe_repr():
    callback = CapitalRawReplyCallback(
        received_at=NOW,
        broker_session_generation=3,
        adapter_received_sequence=7,
        user_id="secret-user",
        payload="secret-account,raw,csv",
    )

    assert callback.generation == 3
    assert callback.sequence == 7
    assert "secret-user" not in repr(callback)
    assert "secret-account" not in repr(callback)

    with pytest.raises(ValueError, match="positive integer"):
        CapitalRawReplyCallback(NOW, 0, 7, "user", "payload")
    with pytest.raises(ValueError, match="positive integer"):
        CapitalRawReplyCallback(NOW, 3, 0, "user", "payload")


def test_every_raw_broker_field_is_excluded_from_repr():
    values = [f"private-field-{index}" for index in range(CAPITAL_ON_NEW_DATA_FIELD_COUNT)]
    record = CapitalOnNewDataRecord.from_fields(tuple(values))

    rendered = repr(record)
    assert rendered == "CapitalOnNewDataRecord()"
    assert all(value not in rendered for value in values)


def test_record_rejects_wrong_field_count_without_echoing_values():
    secret = "account-should-not-leak"
    with pytest.raises(ValueError) as caught:
        CapitalOnNewDataRecord.from_fields((secret,))

    assert secret not in str(caught.value)
