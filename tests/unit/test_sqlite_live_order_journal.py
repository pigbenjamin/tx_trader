from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import sqlite3

import pytest

from tx_trade.orders.live_contracts import (
    BrokerCorrelation,
    BrokerOrderEventType,
    CorrelationStatus,
    DispatchReceipt,
    DispatchState,
    FingerprintDomain,
    LiveOrderIntent,
    LiveOrderState,
    LiveOrderType,
    LiveSide,
    LiveTimeInForce,
    NewOrderCommand,
    NormalizedBrokerOrderEvent,
    PendingCommandBinding,
    payload_fingerprint,
)
from tx_trade.orders.live_journal_contracts import (
    JournalOpenMode,
    LiveJournalClosedError,
    LiveJournalConflictError,
    LiveJournalIntegrityError,
    ReceiptRecordDisposition,
    RegistrationDisposition,
    intent_fingerprint,
)
from tx_trade.orders.live_ports import (
    DispatchClaimDisposition,
    JournalAppendDisposition,
    RawBrokerObservation,
)
from tx_trade.orders.live_state_machine import advance_local, create_live_order
from tx_trade.orders.sqlite_live_order_journal import SqliteLiveOrderJournal

NOW = datetime(2026, 7, 30, tzinfo=timezone.utc)


def _intent(order_id: str = "order-1") -> LiveOrderIntent:
    return LiveOrderIntent(
        strategy_id="strategy-1",
        client_order_id=order_id,
        account_id="account-1",
        instrument_id="TXF",
        side=LiveSide.BUY,
        quantity=Decimal("1"),
        order_type=LiveOrderType.LIMIT,
        limit_price=Decimal("22000"),
        time_in_force=LiveTimeInForce.DAY,
        day_trade=False,
        created_at=NOW,
    )


def _submitting() -> tuple[NewOrderCommand, object]:
    intent = _intent()
    command = NewOrderCommand("command-1", intent, NOW + timedelta(seconds=1))
    fingerprint = payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1)
    order = create_live_order(intent)
    order = advance_local(order, LiveOrderState.VALIDATED, NOW + timedelta(milliseconds=1))
    order = advance_local(
        order,
        LiveOrderState.SUBMITTING,
        NOW + timedelta(seconds=1),
        PendingCommandBinding(command, fingerprint),
    )
    return command, order


def _journal(
    path: Path,
    mode: JournalOpenMode,
    *,
    journal_id: str | None = None,
    clock=lambda: NOW + timedelta(seconds=10),
    claim_token_factory=lambda: "claim-token-1",
) -> SqliteLiveOrderJournal:
    return SqliteLiveOrderJournal(
        path,
        mode,
        clock=clock,
        claim_token_factory=claim_token_factory,
        journal_id=journal_id,
    )


def test_create_resume_and_create_resume_boundaries(tmp_path: Path) -> None:
    path = tmp_path / "live.sqlite3"
    journal = _journal(path, JournalOpenMode.CREATE_NEW, journal_id="journal-1")
    assert journal.identity.journal_id == "journal-1"
    assert journal.load_recovery_snapshot().journal_sequence == 1
    journal.close()
    with pytest.raises(LiveJournalConflictError):
        _journal(path, JournalOpenMode.CREATE_NEW, journal_id="journal-2")
    resumed = _journal(path, JournalOpenMode.RESUME)
    assert resumed.identity.journal_id == "journal-1"
    resumed.close()
    with pytest.raises(LiveJournalIntegrityError):
        _journal(tmp_path / "missing.sqlite3", JournalOpenMode.RESUME)


def test_register_claim_receipt_and_reopen(tmp_path: Path) -> None:
    path = tmp_path / "live.sqlite3"
    command, order = _submitting()
    journal = _journal(path, JournalOpenMode.CREATE_NEW, journal_id="journal-1")
    registered = journal.register_new_order(
        command, order, intent_fingerprint=intent_fingerprint(command.intent)
    )
    assert registered.disposition is RegistrationDisposition.REGISTERED
    assert journal.load_recovery_snapshot().journal_sequence == 3
    retry = journal.register_new_order(
        command, order, intent_fingerprint=intent_fingerprint(command.intent)
    )
    assert retry.disposition is RegistrationDisposition.EXACT_RETRY
    assert journal.load_recovery_snapshot().journal_sequence == 3
    fingerprint = payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1)
    claim = journal.claim_dispatch(
        command.client_command_id,
        fingerprint,
        expected_order_version=order.version,
        claimant_id="dispatcher-1",
    )
    assert claim.disposition is DispatchClaimDisposition.ACQUIRED
    assert claim.claim_token is not None
    assert journal.load_recovery_snapshot().journal_sequence == 4
    journal.close()

    journal = _journal(path, JournalOpenMode.RESUME)
    snapshot = journal.load_recovery_snapshot()
    assert snapshot.orders == (order,)
    assert len(snapshot.outstanding_claims) == 1
    assert snapshot.outstanding_claims[0].command == command
    already = journal.claim_dispatch(
        command.client_command_id,
        fingerprint,
        expected_order_version=order.version,
        claimant_id="dispatcher-2",
    )
    assert already.disposition is DispatchClaimDisposition.ALREADY_CLAIMED
    assert journal.load_recovery_snapshot().journal_sequence == 4
    receipt = DispatchReceipt(
        command.client_command_id,
        fingerprint,
        DispatchState.SUCCEEDED,
        NOW + timedelta(seconds=2),
        NOW + timedelta(seconds=3),
    )
    result = journal.record_dispatch_receipt(
        receipt,
        claim_token=claim.claim_token,
        expected_order_version=order.version,
    )
    assert result.disposition is ReceiptRecordDisposition.RECORDED
    assert journal.load_recovery_snapshot().journal_sequence == 5
    retry_result = journal.record_dispatch_receipt(
        receipt,
        claim_token=claim.claim_token,
        expected_order_version=order.version,
    )
    assert retry_result.disposition is ReceiptRecordDisposition.EXACT_RETRY
    assert journal.load_recovery_snapshot().journal_sequence == 5
    assert journal.load_recovery_snapshot().outstanding_claims == ()
    journal.close()


def test_raw_observation_exact_retry_conflict_and_reopen(tmp_path: Path) -> None:
    path = tmp_path / "live.sqlite3"
    journal = _journal(path, JournalOpenMode.CREATE_NEW, journal_id="journal-1")
    raw = RawBrokerObservation("raw-1", "capital", 1, 1, NOW, b"payload")
    assert journal.append_raw_observation(raw).disposition is JournalAppendDisposition.APPENDED
    assert journal.load_recovery_snapshot().journal_sequence == 2
    assert (
        journal.append_raw_observation(raw).disposition is JournalAppendDisposition.EXACT_DUPLICATE
    )
    assert journal.load_recovery_snapshot().journal_sequence == 2
    changed = RawBrokerObservation("raw-1", "capital", 1, 2, NOW, b"changed")
    assert (
        journal.append_raw_observation(changed).disposition is JournalAppendDisposition.ID_CONFLICT
    )
    journal.close()
    resumed = _journal(path, JournalOpenMode.RESUME)
    assert resumed.load_recovery_snapshot().unresolved_observations == (raw,)
    resumed.close()


def test_schema_and_payload_tampering_fail_closed(tmp_path: Path) -> None:
    path = tmp_path / "live.sqlite3"
    command, order = _submitting()
    journal = _journal(path, JournalOpenMode.CREATE_NEW, journal_id="journal-1")
    journal.register_new_order(
        command, order, intent_fingerprint=intent_fingerprint(command.intent)
    )
    journal.close()
    connection = sqlite3.connect(path)
    connection.execute(
        "UPDATE live_orders SET payload_digest = ?",
        ("sha256:" + "0" * 64,),
    )
    connection.commit()
    connection.close()
    with pytest.raises(LiveJournalIntegrityError):
        _journal(path, JournalOpenMode.RESUME)

    schema_path = tmp_path / "schema.sqlite3"
    journal = _journal(schema_path, JournalOpenMode.CREATE_NEW, journal_id="journal-2")
    journal.close()
    connection = sqlite3.connect(schema_path)
    connection.execute("PRAGMA user_version = 99")
    connection.commit()
    connection.close()
    with pytest.raises(LiveJournalIntegrityError):
        _journal(schema_path, JournalOpenMode.RESUME)


def test_global_journal_sequence_gap_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "live.sqlite3"
    command, order = _submitting()
    journal = _journal(path, JournalOpenMode.CREATE_NEW, journal_id="journal-1")
    journal.register_new_order(
        command, order, intent_fingerprint=intent_fingerprint(command.intent)
    )
    assert journal.load_recovery_snapshot().journal_sequence == 3
    journal.close()

    connection = sqlite3.connect(path)
    connection.execute("DELETE FROM live_journal_records WHERE journal_sequence = 2")
    connection.commit()
    connection.close()
    with pytest.raises(LiveJournalIntegrityError):
        _journal(path, JournalOpenMode.RESUME)


def test_injected_clock_and_claim_token_are_validated_at_use(tmp_path: Path) -> None:
    with pytest.raises(LiveJournalIntegrityError):
        _journal(
            tmp_path / "bad-clock.sqlite3",
            JournalOpenMode.CREATE_NEW,
            journal_id="journal-1",
            clock=lambda: datetime(2026, 7, 30),
        )

    path = tmp_path / "bad-token.sqlite3"
    command, order = _submitting()
    journal = _journal(
        path,
        JournalOpenMode.CREATE_NEW,
        journal_id="journal-2",
        claim_token_factory=lambda: "contains spaces",
    )
    journal.register_new_order(
        command, order, intent_fingerprint=intent_fingerprint(command.intent)
    )
    fingerprint = payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1)
    with pytest.raises(ValueError, match="claim_token_factory"):
        journal.claim_dispatch(
            command.client_command_id,
            fingerprint,
            expected_order_version=order.version,
            claimant_id="dispatcher-1",
        )
    snapshot = journal.load_recovery_snapshot()
    assert snapshot.outstanding_claims == ()
    assert snapshot.journal_sequence == 3
    journal.close()


def test_storage_integrity_failure_poisons_open_instance(tmp_path: Path) -> None:
    path = tmp_path / "poison.sqlite3"
    command, order = _submitting()
    journal = _journal(path, JournalOpenMode.CREATE_NEW, journal_id="journal-poison")
    journal.register_new_order(
        command, order, intent_fingerprint=intent_fingerprint(command.intent)
    )
    connection = sqlite3.connect(path)
    connection.execute(
        "UPDATE live_orders SET payload_digest = ?",
        ("sha256:" + "0" * 64,),
    )
    connection.commit()
    connection.close()
    with pytest.raises(LiveJournalIntegrityError):
        journal.get_order(order.intent.client_order_id)
    with pytest.raises(LiveJournalClosedError):
        journal.load_recovery_snapshot()


def test_normalized_application_uses_stable_namespace_across_raw_sources(
    tmp_path: Path,
) -> None:
    path = tmp_path / "event.sqlite3"
    command, order = _submitting()
    journal = _journal(path, JournalOpenMode.CREATE_NEW, journal_id="journal-event")
    journal.register_new_order(
        command, order, intent_fingerprint=intent_fingerprint(command.intent)
    )
    received_at = NOW + timedelta(seconds=2)
    first_raw = RawBrokerObservation("raw-event-1", "capital-primary", 1, 1, received_at, b"first")
    journal.append_raw_observation(first_raw)
    first_event = NormalizedBrokerOrderEvent(
        event_id="broker-event-1",
        account_id=order.intent.account_id,
        instrument_id=order.intent.instrument_id,
        event_type=BrokerOrderEventType.NEW_ACCEPTED,
        received_at=received_at,
        broker_session_generation=1,
        adapter_received_sequence=1,
        correlation=BrokerCorrelation(
            1,
            1,
            CorrelationStatus.CONFIRMED,
            received_at,
            broker_order_sequence="broker-order-1",
            client_order_id=order.intent.client_order_id,
        ),
    )
    applied = journal.apply_normalized_event(
        first_event,
        raw_observation_id=first_raw.observation_id,
        expected_order_version=order.version,
    )
    assert applied.disposition.value == "applied"

    redelivered_at = received_at + timedelta(seconds=1)
    second_raw = RawBrokerObservation(
        "raw-event-2", "capital-backfill", 2, 1, redelivered_at, b"second"
    )
    journal.append_raw_observation(second_raw)
    second_event = NormalizedBrokerOrderEvent(
        event_id=first_event.event_id,
        account_id=first_event.account_id,
        instrument_id=first_event.instrument_id,
        event_type=first_event.event_type,
        received_at=redelivered_at,
        broker_session_generation=2,
        adapter_received_sequence=1,
        correlation=BrokerCorrelation(
            2,
            1,
            CorrelationStatus.CONFIRMED,
            redelivered_at,
            broker_order_sequence="broker-order-1",
            client_order_id=order.intent.client_order_id,
        ),
    )
    duplicate = journal.apply_normalized_event(
        second_event,
        raw_observation_id=second_raw.observation_id,
        expected_order_version=applied.order.version if applied.order is not None else None,
    )
    assert duplicate.disposition.value == "exact_duplicate"
    snapshot = journal.load_recovery_snapshot()
    assert snapshot.unresolved_observations == ()
    assert snapshot.conflict_observations == ()
    assert len(snapshot.applied_event_ledger.events) == 1
    assert snapshot.applied_event_ledger.events[0].source == "broker-event"
    journal.close()
    connection = sqlite3.connect(path)
    connection.execute("UPDATE live_event_applications SET disposition = 'unresolved'")
    connection.commit()
    connection.close()
    with pytest.raises(LiveJournalIntegrityError):
        _journal(path, JournalOpenMode.RESUME)


def test_reducer_reconciliation_is_durable_and_blocks_clean_recovery(
    tmp_path: Path,
) -> None:
    path = tmp_path / "reconcile.sqlite3"
    command, order = _submitting()
    journal = _journal(path, JournalOpenMode.CREATE_NEW, journal_id="journal-reconcile")
    journal.register_new_order(
        command, order, intent_fingerprint=intent_fingerprint(command.intent)
    )
    received_at = NOW + timedelta(seconds=2)
    raw = RawBrokerObservation("raw-conflict-1", "capital", 1, 1, received_at, b"conflict")
    journal.append_raw_observation(raw)
    event = NormalizedBrokerOrderEvent(
        event_id="broker-conflict-1",
        account_id="different-account",
        instrument_id=order.intent.instrument_id,
        event_type=BrokerOrderEventType.NEW_ACCEPTED,
        received_at=received_at,
        broker_session_generation=1,
        adapter_received_sequence=1,
        correlation=BrokerCorrelation(
            1,
            1,
            CorrelationStatus.CONFIRMED,
            received_at,
            broker_order_sequence="broker-order-conflict",
            client_order_id=order.intent.client_order_id,
        ),
    )
    result = journal.apply_normalized_event(
        event,
        raw_observation_id=raw.observation_id,
        expected_order_version=order.version,
    )
    assert result.disposition.value == "unresolved"
    snapshot = journal.load_recovery_snapshot()
    assert snapshot.conflict_observations == (raw,)
    assert len(snapshot.reconciliation_requirements) == 1
    assert snapshot.reconciliation_requirements[0].observation_id == raw.observation_id
    journal.close()
    resumed = _journal(path, JournalOpenMode.RESUME)
    assert resumed.load_recovery_snapshot().conflict_observations == (raw,)
    resumed.close()
    connection = sqlite3.connect(path)
    connection.execute(
        "UPDATE live_reconciliation_requirements SET resolved_at = ?",
        (NOW.isoformat().replace("+00:00", "Z"),),
    )
    connection.commit()
    connection.close()
    with pytest.raises(LiveJournalIntegrityError):
        _journal(path, JournalOpenMode.RESUME)


def test_mismatched_incoming_raw_provenance_is_durable_conflict(
    tmp_path: Path,
) -> None:
    path = tmp_path / "provenance.sqlite3"
    command, order = _submitting()
    journal = _journal(path, JournalOpenMode.CREATE_NEW, journal_id="journal-provenance")
    journal.register_new_order(
        command, order, intent_fingerprint=intent_fingerprint(command.intent)
    )
    received_at = NOW + timedelta(seconds=2)
    raw = RawBrokerObservation("raw-provenance", "capital", 1, 1, received_at, b"raw")
    journal.append_raw_observation(raw)
    event = NormalizedBrokerOrderEvent(
        event_id="broker-provenance",
        account_id=order.intent.account_id,
        instrument_id=order.intent.instrument_id,
        event_type=BrokerOrderEventType.NEW_ACCEPTED,
        received_at=received_at,
        broker_session_generation=2,
        adapter_received_sequence=1,
        correlation=BrokerCorrelation(
            2,
            1,
            CorrelationStatus.CONFIRMED,
            received_at,
            broker_order_sequence="broker-provenance",
            client_order_id=order.intent.client_order_id,
        ),
    )
    result = journal.apply_normalized_event(
        event,
        raw_observation_id=raw.observation_id,
        expected_order_version=order.version,
    )
    assert result.disposition.value == "event_conflict"
    snapshot = journal.load_recovery_snapshot()
    assert snapshot.conflict_observations == (raw,)
    assert len(snapshot.reconciliation_requirements) == 1
    journal.close()


def test_prior_unresolved_event_cannot_be_promoted_by_improved_correlation(
    tmp_path: Path,
) -> None:
    path = tmp_path / "prior-unresolved.sqlite3"
    command, order = _submitting()
    journal = _journal(path, JournalOpenMode.CREATE_NEW, journal_id="journal-unresolved")
    journal.register_new_order(
        command, order, intent_fingerprint=intent_fingerprint(command.intent)
    )
    first_at = NOW + timedelta(seconds=2)
    first_raw = RawBrokerObservation("raw-unresolved", "capital", 1, 1, first_at, b"unresolved")
    journal.append_raw_observation(first_raw)
    first_event = NormalizedBrokerOrderEvent(
        event_id="broker-same-semantic",
        account_id=order.intent.account_id,
        instrument_id=order.intent.instrument_id,
        event_type=BrokerOrderEventType.NEW_ACCEPTED,
        received_at=first_at,
        broker_session_generation=1,
        adapter_received_sequence=1,
        correlation=BrokerCorrelation(
            1,
            1,
            CorrelationStatus.CANDIDATE,
            first_at,
            broker_order_sequence="broker-same",
        ),
    )
    first = journal.apply_normalized_event(
        first_event,
        raw_observation_id=first_raw.observation_id,
        expected_order_version=order.version,
    )
    assert first.disposition.value == "unresolved"

    second_at = first_at + timedelta(seconds=1)
    second_raw = RawBrokerObservation(
        "raw-improved", "capital-backfill", 2, 1, second_at, b"improved"
    )
    journal.append_raw_observation(second_raw)
    improved_event = NormalizedBrokerOrderEvent(
        event_id=first_event.event_id,
        account_id=first_event.account_id,
        instrument_id=first_event.instrument_id,
        event_type=first_event.event_type,
        received_at=second_at,
        broker_session_generation=2,
        adapter_received_sequence=1,
        correlation=BrokerCorrelation(
            2,
            1,
            CorrelationStatus.CONFIRMED,
            second_at,
            broker_order_sequence="broker-same",
            client_order_id=order.intent.client_order_id,
        ),
    )
    improved = journal.apply_normalized_event(
        improved_event,
        raw_observation_id=second_raw.observation_id,
        expected_order_version=order.version,
    )
    assert improved.disposition.value == "event_conflict"
    snapshot = journal.load_recovery_snapshot()
    assert snapshot.unresolved_observations == (first_raw,)
    assert snapshot.conflict_observations == (second_raw,)
    assert len(snapshot.reconciliation_requirements) == 1
    journal.close()
