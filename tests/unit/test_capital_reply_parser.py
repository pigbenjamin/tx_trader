from datetime import datetime, timezone
from decimal import Decimal

import pytest

from tx_trade.broker.capital.reply_parser import (
    CapitalReplyParseError,
    parse_domestic_futures_reply as _parse_domestic_futures_reply,
)
from tx_trade.broker.capital.trading_contracts import (
    CAPITAL_ON_NEW_DATA_FIELDS,
    CapitalRawReplyCallback,
)
from tx_trade.orders.live_contracts import (
    BrokerOrderEventType,
    CorrelationStatus,
    LiveFailureCode,
    LiveSide,
    NormalizedBrokerFillEvent,
    NormalizedBrokerOrderEvent,
)

NOW = datetime(2026, 7, 28, 16, 30, tzinfo=timezone.utc)
ACCOUNT_ID = "opaque-live-account"


def parse_domestic_futures_reply(callback_value, *, account_id: str = ACCOUNT_ID):
    return _parse_domestic_futures_reply(callback_value, account_id=account_id)


def raw_reply(**changes: str) -> str:
    values = dict.fromkeys(CAPITAL_ON_NEW_DATA_FIELDS, "")
    values.update(
        {
            "KeyNo": "KEY0000000001",
            "MarketType": "TF",
            "Type": "N",
            "OrderErr": "N",
            "Broker": "F123",
            "CustNo": "ACCT123",
            "BuySell": "BNR2",
            "ExchangeID": "TAIFEX",
            "ComId": "TXF",
            "OrderNo": "BOOK123",
            "Price": "23000",
            "Qty": "2",
            "Date": "20260729",
            "Time": "00:29:59",
            "MsgNo": "MSG123",
            "SeqNo": "KEY0000000001",
        }
    )
    values.update(changes)
    return ",".join(values[name] for name in CAPITAL_ON_NEW_DATA_FIELDS)


def callback(payload: str, *, generation: int = 2, sequence: int = 3):
    return CapitalRawReplyCallback(
        received_at=NOW,
        broker_session_generation=generation,
        adapter_received_sequence=sequence,
        user_id="login-secret",
        payload=payload,
    )


@pytest.mark.parametrize(
    ("reply_type", "expected"),
    [
        ("N", BrokerOrderEventType.NEW_ACCEPTED),
        ("C", BrokerOrderEventType.CANCELLED),
        ("U", BrokerOrderEventType.QUANTITY_DECREASED),
        ("P", BrokerOrderEventType.PRICE_AMENDED),
        ("B", BrokerOrderEventType.PRICE_AND_QUANTITY_AMENDED),
        ("S", BrokerOrderEventType.DYNAMIC_CANCELLED),
    ],
)
def test_maps_each_official_order_reply_type(reply_type, expected):
    event = parse_domestic_futures_reply(callback(raw_reply(Type=reply_type)))

    assert isinstance(event, NormalizedBrokerOrderEvent)
    assert event.event_type is expected
    assert event.failure_code is None
    assert event.decreased_quantity == (Decimal("2") if reply_type in {"U", "B"} else None)
    assert event.new_limit_price == (Decimal("23000") if reply_type in {"P", "B"} else None)
    assert event.occurred_at == datetime(2026, 7, 28, 16, 29, 59, tzinfo=timezone.utc)
    assert event.correlation.client_order_id is None
    assert event.correlation.status is CorrelationStatus.CANDIDATE


def test_new_rejection_is_distinct_from_timeout_unknown():
    event = parse_domestic_futures_reply(
        callback(raw_reply(OrderErr="Y", ErrorMsg="account ACCT123 failed"))
    )
    timeout = parse_domestic_futures_reply(callback(raw_reply(OrderErr="T")))

    assert isinstance(event, NormalizedBrokerOrderEvent)
    assert event.event_type is BrokerOrderEventType.NEW_REJECTED
    assert event.failure_code is LiveFailureCode.BROKER_REJECTED
    assert timeout.event_type is BrokerOrderEventType.OUTCOME_UNKNOWN
    assert timeout.failure_code is LiveFailureCode.BROKER_TIMEOUT
    assert "ACCT123" not in repr(event)


def test_fill_failure_is_invalid_but_fill_timeout_is_unknown_order_evidence():
    failed = callback(
        raw_reply(
            Type="D",
            OrderErr="Y",
            Price="",
            Qty="",
            ExecutionNo="EX-REJECTED",
        )
    )
    with pytest.raises(CapitalReplyParseError, match="fill_failure"):
        parse_domestic_futures_reply(failed)

    timeout = parse_domestic_futures_reply(
        callback(raw_reply(Type="D", OrderErr="T", ExecutionNo="EX-TIMEOUT"))
    )
    assert timeout.event_type is BrokerOrderEventType.OUTCOME_UNKNOWN
    assert timeout.failure_code is LiveFailureCode.BROKER_TIMEOUT


def test_cancel_does_not_validate_discarded_quantity_or_price():
    empty = parse_domestic_futures_reply(callback(raw_reply(Type="C", Qty="", Price="")))
    non_numeric = parse_domestic_futures_reply(
        callback(raw_reply(Type="C", Qty="not-a-number", Price="NaN"))
    )

    assert empty.event_type is BrokerOrderEventType.CANCELLED
    assert non_numeric.event_type is BrokerOrderEventType.CANCELLED


def test_cancel_rejection_and_timeout_accept_official_zero_side_without_synthesizing_side():
    rejected = parse_domestic_futures_reply(
        callback(raw_reply(Type="C", OrderErr="Y", BuySell="0NR2"))
    )
    timeout = parse_domestic_futures_reply(
        callback(raw_reply(Type="C", OrderErr="T", BuySell="0NR2"))
    )

    assert rejected.event_type is BrokerOrderEventType.CANCEL_REJECTED
    assert rejected.failure_code is LiveFailureCode.CANCEL_REJECTED
    assert timeout.event_type is BrokerOrderEventType.OUTCOME_UNKNOWN
    assert timeout.failure_code is LiveFailureCode.BROKER_TIMEOUT


@pytest.mark.parametrize(
    ("reply_type", "expected_type", "expected_failure"),
    [
        ("U", BrokerOrderEventType.AMEND_REJECTED, LiveFailureCode.AMEND_REJECTED),
        ("P", BrokerOrderEventType.AMEND_REJECTED, LiveFailureCode.AMEND_REJECTED),
        ("B", BrokerOrderEventType.AMEND_REJECTED, LiveFailureCode.AMEND_REJECTED),
        ("S", BrokerOrderEventType.DYNAMIC_CANCELLED, None),
    ],
)
def test_operation_specific_failure_mapping(reply_type, expected_type, expected_failure):
    event = parse_domestic_futures_reply(callback(raw_reply(Type=reply_type, OrderErr="Y")))

    assert event.event_type is expected_type
    assert event.failure_code is expected_failure


@pytest.mark.parametrize(
    ("reply_type", "expected_type"),
    [
        ("U", BrokerOrderEventType.QUANTITY_DECREASED),
        ("B", BrokerOrderEventType.PRICE_AND_QUANTITY_AMENDED),
    ],
)
def test_tf_quantity_change_uses_qty_decrement_when_after_qty_is_officially_empty(
    reply_type, expected_type
):
    event = parse_domestic_futures_reply(callback(raw_reply(Type=reply_type, Qty="3", AfterQty="")))

    assert event.event_type is expected_type
    assert event.decreased_quantity == Decimal("3")


@pytest.mark.parametrize("reply_type", ["C", "U", "P", "B", "S"])
def test_timeout_never_becomes_a_rejection(reply_type):
    event = parse_domestic_futures_reply(
        callback(
            raw_reply(
                Type=reply_type,
                OrderErr="T",
                BuySell="0NR2" if reply_type == "C" else "BNR2",
            )
        )
    )

    assert event.event_type is BrokerOrderEventType.OUTCOME_UNKNOWN
    assert event.failure_code is LiveFailureCode.BROKER_TIMEOUT


def test_fill_maps_side_quantity_price_and_broker_execution_identity():
    event = parse_domestic_futures_reply(
        callback(
            raw_reply(
                KeyNo="",
                Type="D",
                BuySell="SOR1",
                Price="99999",
                Price1="23001.5",
                Qty="3",
                OkSeq="OK-99",
                ExecutionNo="EXEC-99",
                SeqNo="SEQ0000000099",
            )
        )
    )

    assert isinstance(event, NormalizedBrokerFillEvent)
    assert event.side is LiveSide.SELL
    assert event.quantity == Decimal("3")
    assert event.execution_price == Decimal("23001.5")
    assert event.correlation.broker_fill_id == "EXEC-99"
    assert event.correlation.execution_no == "EXEC-99"


def test_account_identity_is_only_the_explicit_opaque_parser_input():
    first = parse_domestic_futures_reply(callback(raw_reply(Broker="F111", CustNo="SAME-CUSTOMER")))
    second = parse_domestic_futures_reply(
        callback(raw_reply(Broker="F222", CustNo="SAME-CUSTOMER")),
    )
    explicitly_other = parse_domestic_futures_reply(
        callback(raw_reply(Broker="F111", CustNo="SAME-CUSTOMER")),
        account_id="another-opaque-account",
    )

    assert first.account_id == second.account_id == ACCOUNT_ID
    assert explicitly_other.account_id != first.account_id
    assert "F111" not in repr(first)
    assert "SAME-CUSTOMER" not in repr(first)


def test_same_broker_fact_has_stable_id_independent_of_adapter_metadata():
    payload = raw_reply()
    first = parse_domestic_futures_reply(callback(payload, generation=1, sequence=1))
    repeated = parse_domestic_futures_reply(callback(payload, generation=8, sequence=991))

    assert first.event_id == repeated.event_id


@pytest.mark.parametrize(
    ("reply_type", "first_changes", "second_changes"),
    [
        ("U", {"Qty": "1"}, {"Qty": "2"}),
        ("P", {"Price": "23000"}, {"Price": "23001"}),
        (
            "B",
            {"Qty": "1", "Price": "23000"},
            {"Qty": "2", "Price": "23001"},
        ),
    ],
)
def test_distinct_amendments_without_message_number_do_not_collapse(
    reply_type, first_changes, second_changes
):
    first = parse_domestic_futures_reply(
        callback(raw_reply(Type=reply_type, MsgNo="", **first_changes))
    )
    second = parse_domestic_futures_reply(
        callback(raw_reply(Type=reply_type, MsgNo="", **second_changes))
    )

    assert first.event_id != second.event_id


def test_distinct_fills_do_not_collapse():
    first = parse_domestic_futures_reply(
        callback(
            raw_reply(
                KeyNo="",
                Type="D",
                Price1="23000",
                ExecutionNo="EX-1",
                OkSeq="1",
            )
        )
    )
    second = parse_domestic_futures_reply(
        callback(
            raw_reply(
                KeyNo="",
                Type="D",
                Price1="23000",
                ExecutionNo="EX-2",
                OkSeq="2",
            )
        )
    )

    assert first.event_id != second.event_id


@pytest.mark.parametrize("field_name", ["ComId2", "YearMonth2", "StrikePrice2", "Price2"])
def test_fill_rejects_any_second_leg_evidence(field_name):
    with pytest.raises(CapitalReplyParseError, match="multi_leg_fill_unsupported"):
        parse_domestic_futures_reply(
            callback(
                raw_reply(
                    Type="D",
                    Price1="23000",
                    ExecutionNo="EX-MULTI",
                    **{field_name: "SECOND-LEG"},
                )
            )
        )


def test_conflicting_key_and_sequence_are_ambiguous_without_guessed_client_id():
    event = parse_domestic_futures_reply(
        callback(raw_reply(KeyNo="KEY0000000001", SeqNo="SEQ0000000002"))
    )

    assert event.correlation.status is CorrelationStatus.AMBIGUOUS
    assert event.correlation.client_order_id is None


@pytest.mark.parametrize(
    "packed",
    [
        "XNR2",
        "BXR2",
        "BNX2",
        "BNR0",
        "BNR2 ",
        "BNR2X",
    ],
)
def test_each_buy_sell_position_and_any_fifth_character_fail_closed(packed):
    with pytest.raises(CapitalReplyParseError, match="buy_sell_format"):
        parse_domestic_futures_reply(callback(raw_reply(BuySell=packed)))


@pytest.mark.parametrize(
    "payload",
    [
        lambda: ",".join(raw_reply().split(",")[:-1]),
        lambda: raw_reply() + ",extra",
        lambda: raw_reply(BuySell="BR2"),
        lambda: raw_reply(BuySell="BNR2 "),
        lambda: raw_reply(BuySell="XNR2"),
        lambda: raw_reply(
            Type="D",
            Qty="two",
            Price1="23000",
            ExecutionNo="EX-BAD-QTY",
        ),
        lambda: raw_reply(Type="D", Price1="NaN", ExecutionNo="EX-NAN"),
        lambda: raw_reply(Time="25:99:99"),
        lambda: raw_reply(Date="2026-07-29"),
        lambda: raw_reply(MarketType="TO"),
    ],
)
def test_malformed_broker_data_fails_with_sanitized_error(payload):
    secret = "SECRET_ACCOUNT_987"
    source = payload().replace("ACCT123", secret)

    with pytest.raises(CapitalReplyParseError) as caught:
        parse_domestic_futures_reply(callback(source))

    assert secret not in str(caught.value)
    assert source not in str(caught.value)


def test_empty_broker_date_and_time_make_occurred_at_optional():
    event = parse_domestic_futures_reply(callback(raw_reply(Date="", Time="")))

    assert event.occurred_at is None
