from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from tx_trade.orders.live_contracts import (
    LiveOrderIntent,
    LiveOrderState,
    LiveOrderType,
    LiveSide,
    LiveTimeInForce,
)
from tx_trade.orders.live_journal_contracts import (
    CommandRegistrationResult,
    LiveJournalIdentity,
    LiveJournalRecoverySnapshot,
    ReceiptRecordDisposition,
    RegistrationDisposition,
    intent_fingerprint,
)
from tx_trade.orders.live_state_machine import EMPTY_EVENT_LEDGER, create_live_order

NOW = datetime(2026, 7, 30, tzinfo=timezone.utc)
SHA = "sha256:" + ("a" * 64)


def intent(client_order_id: str = "order-1") -> LiveOrderIntent:
    return LiveOrderIntent(
        client_order_id=client_order_id,
        strategy_id="strategy-1",
        account_id="account-1",
        instrument_id="TXF-202608",
        side=LiveSide.BUY,
        quantity=Decimal("1"),
        order_type=LiveOrderType.LIMIT,
        time_in_force=LiveTimeInForce.DAY,
        limit_price=Decimal("22000"),
        day_trade=False,
        created_at=NOW,
    )


def test_intent_fingerprint_is_stable_domain_separated_and_exact() -> None:
    value = intent()
    assert intent_fingerprint(value) == intent_fingerprint(value)
    assert intent_fingerprint(value) != intent_fingerprint(replace(value, quantity=Decimal("2")))
    assert intent_fingerprint(value).startswith("sha256:")
    with pytest.raises(TypeError):
        intent_fingerprint(object())  # type: ignore[arg-type]


def test_journal_identity_and_registration_are_strict() -> None:
    identity = LiveJournalIdentity("journal-1", 1, SHA, NOW)
    order = create_live_order(intent())
    result = CommandRegistrationResult(
        "command-1",
        RegistrationDisposition.REGISTERED,
        order,
    )
    assert result.order is order
    assert identity.schema_version == 1
    with pytest.raises(ValueError):
        CommandRegistrationResult(
            "command-1",
            RegistrationDisposition.EXACT_RETRY,
            order,
        )


def test_recovery_snapshot_rejects_duplicate_orders() -> None:
    identity = LiveJournalIdentity("journal-1", 1, SHA, NOW)
    order = create_live_order(intent())
    with pytest.raises(ValueError):
        LiveJournalRecoverySnapshot(
            identity,
            (order, order),
            (),
            (),
            (),
            (),
            (),
            EMPTY_EVENT_LEDGER,
            1,
        )


def test_receipt_disposition_values_are_stable() -> None:
    assert ReceiptRecordDisposition.RECORDED.value == "recorded"
    assert ReceiptRecordDisposition.TOKEN_MISMATCH.value == "token_mismatch"
    assert LiveOrderState.SUBMISSION_UNKNOWN.value == "submission_unknown"
