from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json

import pytest

from tx_trade.orders.live_contracts import (
    AmendOrderCommand,
    BrokerCorrelation,
    BrokerOrderEventType,
    CancelOrderCommand,
    CorrelationStatus,
    DecreaseOrderCommand,
    DispatchReceipt,
    DispatchState,
    FingerprintDomain,
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
    payload_fingerprint,
)
from tx_trade.orders.live_journal_codec import (
    LiveJournalCodecError,
    decode_journal_value,
    encode_journal_value,
    journal_digest,
)
from tx_trade.orders.live_journal_contracts import (
    DurableReconciliationRequirement,
    LiveJournalIdentity,
    LiveJournalRecoverySnapshot,
    OutstandingDispatchClaim,
)
from tx_trade.orders.live_ports import AmbiguousObservation, RawBrokerObservation
from tx_trade.orders.live_reconciliation_authorization_contracts import (
    ReconciliationAuthorizationAction,
    ReconciliationCommitAuthorization,
)
from tx_trade.orders.live_state_machine import AppliedEvent, AppliedEventLedger

NOW = datetime(2026, 7, 30, 1, 2, 3, 456789, tzinfo=timezone.utc)


def _intent() -> LiveOrderIntent:
    return LiveOrderIntent(
        strategy_id="strategy-1",
        client_order_id="order-1",
        account_id="account-secret",
        instrument_id="TXF",
        side=LiveSide.BUY,
        quantity=Decimal("2.00"),
        order_type=LiveOrderType.LIMIT,
        limit_price=Decimal("19800.0"),
        time_in_force=LiveTimeInForce.DAY,
        day_trade=False,
        created_at=NOW,
    )


def _commands() -> tuple[object, ...]:
    return (
        NewOrderCommand("command-new", _intent(), NOW),
        CancelOrderCommand("command-cancel", "order-1", NOW),
        AmendOrderCommand("command-amend", "order-1", Decimal("19801"), NOW),
        DecreaseOrderCommand(
            "command-decrease",
            "order-1",
            Decimal("2"),
            Decimal("1"),
            NOW,
        ),
    )


def _binding() -> PendingCommandBinding:
    command = _commands()[0]
    assert type(command) is NewOrderCommand
    return PendingCommandBinding(
        command,
        payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1),
    )


def _order() -> LiveOrder:
    return LiveOrder(
        intent=_intent(),
        state=LiveOrderState.SUBMITTING,
        total_quantity=Decimal("2"),
        filled_quantity=Decimal("0"),
        remaining_quantity=Decimal("2"),
        average_fill_price=None,
        working_limit_price=Decimal("19800"),
        version=3,
        updated_at=NOW,
        pending_command=_binding(),
    )


def _correlation(*, fill: bool = False) -> BrokerCorrelation:
    return BrokerCorrelation(
        broker_session_generation=2,
        adapter_received_sequence=9,
        status=CorrelationStatus.CONFIRMED,
        correlated_at=NOW,
        broker_order_sequence="broker-order-7",
        broker_fill_id="fill-7" if fill else None,
        client_order_id="order-1",
    )


def _observation() -> RawBrokerObservation:
    return RawBrokerObservation(
        observation_id="observation-1",
        source="reply",
        broker_session_generation=2,
        adapter_received_sequence=9,
        received_at=NOW,
        payload=b"\x00secret-account-payload\xff",
    )


def _durable_values() -> tuple[object, ...]:
    commands = _commands()
    receipt = DispatchReceipt(
        client_command_id="command-new",
        payload_fingerprint=_binding().payload_fingerprint,
        state=DispatchState.UNKNOWN,
        attempted_at=NOW,
        completed_at=None,
        failure_code=LiveFailureCode.DISPATCH_OUTCOME_UNKNOWN,
    )
    order_event = NormalizedBrokerOrderEvent(
        event_id="event-order-1",
        account_id="account-secret",
        instrument_id="TXF",
        event_type=BrokerOrderEventType.NEW_ACCEPTED,
        received_at=NOW,
        broker_session_generation=2,
        adapter_received_sequence=9,
        correlation=_correlation(),
    )
    fill_event = NormalizedBrokerFillEvent(
        event_id="event-fill-1",
        account_id="account-secret",
        instrument_id="TXF",
        side=LiveSide.BUY,
        quantity=Decimal("1.00"),
        execution_price=Decimal("19799.0"),
        received_at=NOW,
        broker_session_generation=2,
        adapter_received_sequence=9,
        correlation=_correlation(fill=True),
    )
    fill = LiveFill(
        fill_id="fill-7",
        client_order_id="order-1",
        strategy_id="strategy-1",
        account_id="account-secret",
        instrument_id="TXF",
        side=LiveSide.BUY,
        quantity=Decimal("1"),
        execution_price=Decimal("19799"),
        occurred_at=NOW,
    )
    observation = _observation()
    ambiguous = AmbiguousObservation(observation, ("order-1", "order-2"))
    ledger = AppliedEventLedger(
        (AppliedEvent("broker-event", "event-fill-1", f"sha256:{'a' * 64}"),)
    )
    identity = LiveJournalIdentity(
        "journal-1",
        1,
        f"sha256:{'b' * 64}",
        NOW,
    )
    outstanding = OutstandingDispatchClaim(
        command=commands[0],  # type: ignore[arg-type]
        claim_token="claim-1",
        claimant_id="dispatcher-1",
        expected_order_version=3,
        claimed_at=NOW,
    )
    requirement = DurableReconciliationRequirement(
        requirement_id=1,
        reason_code="dispatch-outcome-unknown",
        created_at=NOW,
        client_order_id="order-1",
    )
    snapshot = LiveJournalRecoverySnapshot(
        identity=identity,
        orders=(_order(),),
        outstanding_claims=(outstanding,),
        unresolved_observations=(observation,),
        conflict_observations=(observation,),
        ambiguous_observations=(ambiguous,),
        reconciliation_requirements=(requirement,),
        applied_event_ledger=ledger,
        journal_sequence=12,
    )
    return (
        _intent(),
        *commands,
        _binding(),
        _order(),
        receipt,
        observation,
        order_event,
        fill_event,
        fill,
        ledger,
        ambiguous,
        identity,
        outstanding,
        requirement,
        snapshot,
    )


@pytest.mark.parametrize("value", _durable_values(), ids=lambda value: type(value).__name__)
def test_all_durable_contracts_round_trip_canonically(value: object) -> None:
    encoded = encode_journal_value(value)

    decoded = decode_journal_value(encoded, type(value))

    assert decoded == value
    assert encode_journal_value(decoded) == encoded
    assert b"2.00" not in encoded
    assert b"19800.0" not in encoded


def test_command_union_expected_type_is_supported() -> None:
    command = _commands()[2]
    command_union = NewOrderCommand | CancelOrderCommand | AmendOrderCommand | DecreaseOrderCommand

    assert decode_journal_value(encode_journal_value(command), command_union) == command


def test_bytes_are_base64_and_round_trip_without_utf8_assumption() -> None:
    payload = encode_journal_value(_observation())

    assert b"secret-account-payload" not in payload
    assert decode_journal_value(payload, RawBrokerObservation) == _observation()


def test_digest_is_deterministic_domain_separated_and_verified() -> None:
    payload = encode_journal_value(_order())
    digest = journal_digest("tx_trade.live.journal.order.v1", payload)

    assert digest == journal_digest("tx_trade.live.journal.order.v1", payload)
    assert digest != journal_digest("tx_trade.live.journal.event.v1", payload)
    assert (
        decode_journal_value(
            payload,
            LiveOrder,
            domain="tx_trade.live.journal.order.v1",
            expected_digest=digest,
        )
        == _order()
    )
    with pytest.raises(LiveJournalCodecError):
        decode_journal_value(
            payload,
            LiveOrder,
            domain="tx_trade.live.journal.order.v1",
            expected_digest=f"sha256:{'0' * 64}",
        )


@pytest.mark.parametrize(
    "mutation",
    (
        lambda value: value.update(extra="unexpected"),
        lambda value: value.pop("schema_version"),
        lambda value: value.update(schema_version=2),
        lambda value: value.update(type="UnknownType"),
        lambda value: value.update(value={"unexpected": 1.5}),
    ),
)
def test_envelope_rejects_extra_missing_unknown_and_float(mutation: object) -> None:
    document = json.loads(encode_journal_value(_intent()))
    mutation(document)  # type: ignore[operator]

    with pytest.raises(LiveJournalCodecError):
        decode_journal_value(
            json.dumps(document, separators=(",", ":"), sort_keys=True).encode(),
        )


def test_noncanonical_json_decimal_datetime_and_base64_are_rejected() -> None:
    cases: list[bytes] = []
    for value, needle, replacement in (
        (_intent(), b'"$decimal":"2"', b'"$decimal":"2.0"'),
        (
            _intent(),
            b'"$datetime":"2026-07-30T01:02:03.456789Z"',
            b'"$datetime":"2026-07-30T01:02:03.456789+00:00"',
        ),
        (_observation(), b'"$bytes":"', b'"$bytes":"@@'),
    ):
        payload = encode_journal_value(value)
        assert needle in payload
        cases.append(payload.replace(needle, replacement, 1))

    for payload in cases:
        with pytest.raises(LiveJournalCodecError):
            decode_journal_value(payload)


@pytest.mark.parametrize(
    "hostile_decimal",
    (
        "1e1000000000",
        "1" * 10_000,
    ),
)
def test_hostile_decimal_is_rejected_before_decimal_normalization(
    hostile_decimal: str,
) -> None:
    payload = encode_journal_value(_intent()).replace(
        b'"$decimal":"2"',
        f'"$decimal":"{hostile_decimal}"'.encode(),
        1,
    )

    with pytest.raises(LiveJournalCodecError) as caught:
        decode_journal_value(payload)

    assert hostile_decimal not in str(caught.value)
    assert caught.value.__cause__ is None


def test_wrong_expected_type_and_naive_datetime_fail_closed() -> None:
    with pytest.raises(LiveJournalCodecError):
        decode_journal_value(encode_journal_value(_intent()), LiveOrder)
    payload = encode_journal_value(_intent()).replace(
        b"2026-07-30T01:02:03.456789Z",
        b"2026-07-30T01:02:03.456789",
    )
    with pytest.raises(LiveJournalCodecError):
        decode_journal_value(payload)


def test_errors_never_echo_raw_payload_or_secret() -> None:
    secret = b"account-password-super-secret"

    with pytest.raises(LiveJournalCodecError) as caught:
        decode_journal_value(secret)

    assert secret.decode() not in str(caught.value)
    assert secret.decode() not in repr(caught.value)


def test_reconciliation_authorization_round_trips_canonically() -> None:
    value = ReconciliationCommitAuthorization(
        authorization_id="authorization-1",
        principal_id="principal-secret",
        authority_context_digest=f"sha256:{'1' * 64}",
        action=ReconciliationAuthorizationAction.RECONCILIATION_COMMIT,
        journal_id="journal-secret",
        account_id="account-secret",
        source_inspection_digest=f"sha256:{'2' * 64}",
        operator_plan_digest=f"sha256:{'3' * 64}",
        commit_id="commit-1",
        request_digest=f"sha256:{'4' * 64}",
        broker_snapshot_id="snapshot-secret",
        expected_journal_sequence=4,
        authorized_at=NOW,
        expires_at=NOW + timedelta(seconds=60),
        reason_code="operator-approved",
    )

    payload = encode_journal_value(value)
    decoded = decode_journal_value(payload, ReconciliationCommitAuthorization)

    assert decoded == value
    assert encode_journal_value(decoded) == payload
    assert b"ReconciliationAuthorizationAction" in payload
