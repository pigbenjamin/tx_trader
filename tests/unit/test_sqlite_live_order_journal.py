from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, localcontext
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
from tx_trade.orders.live_journal_contracts import (
    DurableOrderJournalPort,
    JournalOpenMode,
    LiveJournalClosedError,
    LiveJournalConflictError,
    LiveJournalIntegrityError,
    ReceiptRecordDisposition,
    RegistrationDisposition,
    intent_fingerprint,
)
from tx_trade.orders.live_reconciliation_contracts import LocalReconciliationSourcePort
from tx_trade.orders.live_ports import (
    DispatchClaimDisposition,
    JournalAppendDisposition,
    RawBrokerObservation,
)
from tx_trade.orders.live_state_machine import advance_local, create_live_order
from tx_trade.orders.sqlite_live_order_journal import SqliteLiveOrderJournal

NOW = datetime(2026, 7, 30, tzinfo=timezone.utc)


def _intent(
    order_id: str = "order-1",
    *,
    account_id: str = "account-1",
    strategy_id: str = "strategy-1",
    instrument_id: str = "TXF",
    side: LiveSide = LiveSide.BUY,
    quantity: Decimal = Decimal("1"),
) -> LiveOrderIntent:
    return LiveOrderIntent(
        strategy_id=strategy_id,
        client_order_id=order_id,
        account_id=account_id,
        instrument_id=instrument_id,
        side=side,
        quantity=quantity,
        order_type=LiveOrderType.LIMIT,
        limit_price=Decimal("22000"),
        time_in_force=LiveTimeInForce.DAY,
        day_trade=False,
        created_at=NOW,
    )


def _submitting(
    intent: LiveOrderIntent | None = None,
) -> tuple[NewOrderCommand, object]:
    intent = intent or _intent()
    command_id = (
        "command-1" if intent.client_order_id == "order-1" else f"command-{intent.client_order_id}"
    )
    command = NewOrderCommand(command_id, intent, NOW + timedelta(seconds=1))
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


def _register_and_fill(
    journal: SqliteLiveOrderJournal,
    intent: LiveOrderIntent,
    *,
    sequence: int,
    occurred_at: datetime,
) -> None:
    command, order = _submitting(intent)
    journal.register_new_order(command, order, intent_fingerprint=intent_fingerprint(intent))
    accepted_at = occurred_at - timedelta(milliseconds=1)
    accepted_raw = RawBrokerObservation(
        f"raw-accepted-{sequence}",
        "capital-primary",
        1,
        sequence * 2 - 1,
        accepted_at,
        b"accepted",
    )
    journal.append_raw_observation(accepted_raw)
    correlation = BrokerCorrelation(
        1,
        sequence * 2 - 1,
        CorrelationStatus.CONFIRMED,
        accepted_at,
        broker_order_sequence=f"broker-order-{sequence}",
        client_order_id=intent.client_order_id,
    )
    accepted = NormalizedBrokerOrderEvent(
        event_id=f"accepted-{sequence}",
        account_id=intent.account_id,
        instrument_id=intent.instrument_id,
        event_type=BrokerOrderEventType.NEW_ACCEPTED,
        received_at=accepted_at,
        broker_session_generation=1,
        adapter_received_sequence=sequence * 2 - 1,
        correlation=correlation,
    )
    result = journal.apply_normalized_event(
        accepted,
        raw_observation_id=accepted_raw.observation_id,
        expected_order_version=order.version,
    )
    assert result.order is not None

    fill_raw = RawBrokerObservation(
        f"raw-fill-{sequence}",
        "capital-primary",
        1,
        sequence * 2,
        occurred_at,
        b"fill",
    )
    journal.append_raw_observation(fill_raw)
    fill_correlation = BrokerCorrelation(
        1,
        sequence * 2,
        CorrelationStatus.CONFIRMED,
        occurred_at,
        broker_order_sequence=f"broker-order-{sequence}",
        broker_fill_id=f"broker-fill-{sequence}",
        client_order_id=intent.client_order_id,
    )
    fill = NormalizedBrokerFillEvent(
        event_id=f"fill-{sequence}",
        account_id=intent.account_id,
        instrument_id=intent.instrument_id,
        side=intent.side,
        quantity=intent.quantity,
        execution_price=Decimal("22100"),
        received_at=occurred_at,
        broker_session_generation=1,
        adapter_received_sequence=sequence * 2,
        correlation=fill_correlation,
        occurred_at=occurred_at,
    )
    applied = journal.apply_normalized_event(
        fill,
        raw_observation_id=fill_raw.observation_id,
        expected_order_version=result.order.version,
    )
    assert applied.order is not None


def _register_and_accept(
    journal: SqliteLiveOrderJournal,
    intent: LiveOrderIntent,
    *,
    sequence: int,
) -> LiveOrder:
    command, order = _submitting(intent)
    journal.register_new_order(command, order, intent_fingerprint=intent_fingerprint(intent))
    accepted_at = NOW + timedelta(seconds=2)
    raw = RawBrokerObservation(
        f"raw-only-accepted-{sequence}",
        "capital-primary",
        1,
        sequence,
        accepted_at,
        b"accepted",
    )
    journal.append_raw_observation(raw)
    event = NormalizedBrokerOrderEvent(
        event_id=f"only-accepted-{sequence}",
        account_id=intent.account_id,
        instrument_id=intent.instrument_id,
        event_type=BrokerOrderEventType.NEW_ACCEPTED,
        received_at=accepted_at,
        broker_session_generation=1,
        adapter_received_sequence=sequence,
        correlation=BrokerCorrelation(
            1,
            sequence,
            CorrelationStatus.CONFIRMED,
            accepted_at,
            broker_order_sequence=f"broker-only-accepted-{sequence}",
            client_order_id=intent.client_order_id,
        ),
    )
    result = journal.apply_normalized_event(
        event,
        raw_observation_id=raw.observation_id,
        expected_order_version=order.version,
    )
    assert result.order is not None
    assert result.order.state is LiveOrderState.ACCEPTED
    assert result.order.pending_command is None
    return result.order


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
    assert journal.load_recovery_snapshot().journal_sequence == 2
    connection = sqlite3.connect(path)
    initial_records = connection.execute(
        """SELECT record_kind, record_id FROM live_journal_records
           ORDER BY journal_sequence"""
    ).fetchall()
    connection.close()
    assert initial_records == [("identity", "journal-1"), ("schema-migration", "2")]
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
    assert journal.load_recovery_snapshot().journal_sequence == 4
    retry = journal.register_new_order(
        command, order, intent_fingerprint=intent_fingerprint(command.intent)
    )
    assert retry.disposition is RegistrationDisposition.EXACT_RETRY
    assert journal.load_recovery_snapshot().journal_sequence == 4
    fingerprint = payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1)
    claim = journal.claim_dispatch(
        command.client_command_id,
        fingerprint,
        expected_order_version=order.version,
        claimant_id="dispatcher-1",
    )
    assert claim.disposition is DispatchClaimDisposition.ACQUIRED
    assert claim.claim_token is not None
    assert journal.load_recovery_snapshot().journal_sequence == 5
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
    assert journal.load_recovery_snapshot().journal_sequence == 5
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
    assert journal.load_recovery_snapshot().journal_sequence == 6
    retry_result = journal.record_dispatch_receipt(
        receipt,
        claim_token=claim.claim_token,
        expected_order_version=order.version,
    )
    assert retry_result.disposition is ReceiptRecordDisposition.EXACT_RETRY
    assert journal.load_recovery_snapshot().journal_sequence == 6
    assert journal.load_recovery_snapshot().outstanding_claims == ()
    journal.close()


def test_raw_observation_exact_retry_conflict_and_reopen(tmp_path: Path) -> None:
    path = tmp_path / "live.sqlite3"
    journal = _journal(path, JournalOpenMode.CREATE_NEW, journal_id="journal-1")
    raw = RawBrokerObservation("raw-1", "capital", 1, 1, NOW, b"payload")
    assert journal.append_raw_observation(raw).disposition is JournalAppendDisposition.APPENDED
    assert journal.load_recovery_snapshot().journal_sequence == 3
    assert (
        journal.append_raw_observation(raw).disposition is JournalAppendDisposition.EXACT_DUPLICATE
    )
    assert journal.load_recovery_snapshot().journal_sequence == 3
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
    assert journal.load_recovery_snapshot().journal_sequence == 4
    journal.close()

    connection = sqlite3.connect(path)
    connection.execute("DELETE FROM live_journal_records WHERE journal_sequence = 3")
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
    assert snapshot.journal_sequence == 4
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


def test_account_snapshot_is_read_only_isolated_and_recomputed_on_resume(
    tmp_path: Path,
) -> None:
    path = tmp_path / "account-snapshot.sqlite3"
    as_of = NOW + timedelta(seconds=10)
    journal = _journal(
        path,
        JournalOpenMode.CREATE_NEW,
        journal_id="journal-snapshot",
        clock=lambda: as_of,
    )
    assert isinstance(journal, DurableOrderJournalPort)
    assert isinstance(journal, LocalReconciliationSourcePort)
    with pytest.raises(TypeError, match="string"):
        journal.load_account_snapshot(1)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="bounded ASCII"):
        journal.load_account_snapshot("bad account")

    _register_and_fill(
        journal,
        _intent(
            "order-buy",
            quantity=Decimal("2"),
            instrument_id="TXF-202608",
        ),
        sequence=1,
        occurred_at=NOW + timedelta(seconds=3),
    )
    _register_and_fill(
        journal,
        _intent(
            "order-sell",
            side=LiveSide.SELL,
            instrument_id="TXF-202608",
        ),
        sequence=2,
        occurred_at=NOW + timedelta(seconds=2),
    )
    _register_and_fill(
        journal,
        _intent(
            "order-flat-buy",
            strategy_id="strategy-flat",
            instrument_id="MXF-202608",
        ),
        sequence=3,
        occurred_at=NOW + timedelta(seconds=5),
    )
    _register_and_fill(
        journal,
        _intent(
            "order-flat-sell",
            strategy_id="strategy-flat",
            instrument_id="MXF-202608",
            side=LiveSide.SELL,
        ),
        sequence=4,
        occurred_at=NOW + timedelta(seconds=4),
    )
    _register_and_fill(
        journal,
        _intent("order-other", account_id="account-2"),
        sequence=5,
        occurred_at=NOW + timedelta(seconds=6),
    )

    sequence_before = journal.load_recovery_snapshot().journal_sequence
    snapshot = journal.load_account_snapshot("account-1")
    assert tuple(order.intent.client_order_id for order in snapshot.orders) == (
        "order-buy",
        "order-flat-buy",
        "order-flat-sell",
        "order-sell",
    )
    assert tuple(fill.fill_id for fill in snapshot.fills) == (
        "broker-fill-2",
        "broker-fill-1",
        "broker-fill-4",
        "broker-fill-3",
    )
    assert tuple(
        (item.strategy_id, item.instrument_id, item.attributed_quantity)
        for item in snapshot.position_attributions
    ) == (("strategy-1", "TXF-202608", Decimal("1")),)
    assert snapshot.as_of == as_of
    assert journal.load_account_snapshot("empty-account").orders == ()
    assert journal.load_account_snapshot("account-2").orders[0].intent.account_id == "account-2"
    assert journal.load_recovery_snapshot().journal_sequence == sequence_before

    journal.close()
    resumed = _journal(path, JournalOpenMode.RESUME, clock=lambda: as_of)
    assert resumed.load_account_snapshot("account-1") == snapshot
    resumed.close()
    with pytest.raises(LiveJournalClosedError):
        resumed.load_account_snapshot("account-1")


def test_account_snapshot_rejects_old_clock_without_writing(tmp_path: Path) -> None:
    path = tmp_path / "old-snapshot-clock.sqlite3"
    journal = _journal(
        path,
        JournalOpenMode.CREATE_NEW,
        journal_id="journal-old-clock",
        clock=lambda: NOW + timedelta(seconds=10),
    )
    _register_and_fill(
        journal,
        _intent("order-clock"),
        sequence=1,
        occurred_at=NOW + timedelta(seconds=3),
    )
    before = journal.load_recovery_snapshot().journal_sequence
    journal._clock = lambda: NOW + timedelta(seconds=2)
    with pytest.raises(ValueError, match="predates"):
        journal.load_account_snapshot("account-1")
    journal._clock = lambda: NOW + timedelta(seconds=10)
    assert journal.load_recovery_snapshot().journal_sequence == before


def test_legacy_journal_fake_does_not_need_reconciliation_method() -> None:
    class LegacyJournalFake:
        def register_new_order(self, *args: object, **kwargs: object) -> None:
            pass

        def register_command(self, *args: object, **kwargs: object) -> None:
            pass

        def claim_dispatch(self, *args: object, **kwargs: object) -> None:
            pass

        def record_dispatch_receipt(self, *args: object, **kwargs: object) -> None:
            pass

        def append_raw_observation(self, *args: object, **kwargs: object) -> None:
            pass

        def apply_normalized_event(self, *args: object, **kwargs: object) -> None:
            pass

        def get_order(self, *args: object, **kwargs: object) -> None:
            pass

        def list_active_orders(self, *args: object, **kwargs: object) -> None:
            pass

        def load_recovery_snapshot(self, *args: object, **kwargs: object) -> None:
            pass

        def close(self) -> None:
            pass

    legacy = LegacyJournalFake()
    assert isinstance(legacy, DurableOrderJournalPort)
    assert not isinstance(legacy, LocalReconciliationSourcePort)


def test_account_snapshot_blocks_outstanding_claim_for_own_account_only(
    tmp_path: Path,
) -> None:
    journal = _journal(
        tmp_path / "claim-blocker.sqlite3",
        JournalOpenMode.CREATE_NEW,
        journal_id="journal-claim-blocker",
    )
    command, order = _submitting()
    journal.register_new_order(
        command, order, intent_fingerprint=intent_fingerprint(command.intent)
    )
    journal.claim_dispatch(
        command.client_command_id,
        payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1),
        expected_order_version=order.version,
        claimant_id="dispatcher-1",
    )

    blockers = journal.load_account_snapshot("account-1").recovery_blockers
    assert len(blockers) == 1
    assert blockers[0].startswith("recovery:claim:")
    assert command.client_command_id not in blockers[0]
    assert journal.load_account_snapshot("account-2").recovery_blockers == ()


def test_unattributed_unresolved_observation_blocks_every_account_and_restart(
    tmp_path: Path,
) -> None:
    path = tmp_path / "global-blocker.sqlite3"
    journal = _journal(
        path,
        JournalOpenMode.CREATE_NEW,
        journal_id="journal-global-blocker",
    )
    raw = RawBrokerObservation("secret-raw-id", "capital", 1, 1, NOW, b"opaque")
    journal.append_raw_observation(raw)
    first = journal.load_account_snapshot("account-1").recovery_blockers
    assert len(first) == 1
    assert first[0].startswith("recovery:observation-unresolved:")
    assert raw.observation_id not in first[0]
    assert journal.load_account_snapshot("account-2").recovery_blockers == first
    journal.close()

    resumed = _journal(path, JournalOpenMode.RESUME)
    assert resumed.load_account_snapshot("account-1").recovery_blockers == first


def test_accepted_order_conflict_and_requirement_are_durable_account_blockers(
    tmp_path: Path,
) -> None:
    path = tmp_path / "account-conflict-blocker.sqlite3"
    journal = _journal(
        path,
        JournalOpenMode.CREATE_NEW,
        journal_id="journal-account-conflict",
    )
    accepted = _register_and_accept(journal, _intent(), sequence=1)
    conflict_at = NOW + timedelta(seconds=3)
    raw = RawBrokerObservation("raw-account-conflict", "capital", 1, 2, conflict_at, b"conflict")
    journal.append_raw_observation(raw)
    event = NormalizedBrokerOrderEvent(
        event_id="account-conflict",
        account_id="account-1",
        instrument_id="different-instrument",
        event_type=BrokerOrderEventType.NEW_ACCEPTED,
        received_at=conflict_at,
        broker_session_generation=1,
        adapter_received_sequence=2,
        correlation=BrokerCorrelation(
            1,
            2,
            CorrelationStatus.CONFIRMED,
            conflict_at,
            broker_order_sequence="broker-conflict",
            client_order_id=accepted.intent.client_order_id,
        ),
    )
    result = journal.apply_normalized_event(
        event,
        raw_observation_id=raw.observation_id,
        expected_order_version=accepted.version,
    )
    assert result.disposition.value == "unresolved"
    recovery = journal.load_recovery_snapshot()
    assert recovery.conflict_observations == (raw,)
    assert len(recovery.reconciliation_requirements) == 1
    blockers = journal.load_account_snapshot("account-1").recovery_blockers
    assert len(blockers) == 2
    assert journal.load_account_snapshot("account-2").recovery_blockers == ()
    journal.close()

    resumed = _journal(path, JournalOpenMode.RESUME)
    assert resumed.load_account_snapshot("account-1").recovery_blockers == blockers


def test_account_attribution_sum_ignores_ambient_precision_and_overflow_writes_nothing(
    tmp_path: Path,
) -> None:
    path = tmp_path / "exact-attribution.sqlite3"
    journal = _journal(
        path,
        JournalOpenMode.CREATE_NEW,
        journal_id="journal-exact-attribution",
        clock=lambda: NOW + timedelta(seconds=20),
    )
    maximum = Decimal("9999999999999999999999999999999999")
    one_less = Decimal("9999999999999999999999999999999998")
    with localcontext() as setup_context:
        setup_context.prec = 34
        _register_and_fill(
            journal,
            _intent("order-exact-buy", quantity=maximum),
            sequence=1,
            occurred_at=NOW + timedelta(seconds=3),
        )
        _register_and_fill(
            journal,
            _intent(
                "order-exact-sell",
                side=LiveSide.SELL,
                quantity=one_less,
            ),
            sequence=2,
            occurred_at=NOW + timedelta(seconds=4),
        )
    with localcontext() as verification_context:
        verification_context.prec = 34
        sequence = journal.load_recovery_snapshot().journal_sequence
    with localcontext() as context:
        context.prec = 6
        attribution = journal.load_account_snapshot("account-1").position_attributions
    assert attribution[0].attributed_quantity == Decimal(1)
    with localcontext() as verification_context:
        verification_context.prec = 34
        assert journal.load_recovery_snapshot().journal_sequence == sequence

    with localcontext() as setup_context:
        setup_context.prec = 34
        _register_and_fill(
            journal,
            _intent("order-overflow", strategy_id="overflow", quantity=maximum),
            sequence=3,
            occurred_at=NOW + timedelta(seconds=5),
        )
        _register_and_fill(
            journal,
            _intent("order-overflow-2", strategy_id="overflow", quantity=maximum),
            sequence=4,
            occurred_at=NOW + timedelta(seconds=6),
        )
    with localcontext() as verification_context:
        verification_context.prec = 34
        sequence = journal.load_recovery_snapshot().journal_sequence
    with pytest.raises(ValueError, match="exact sum exceeds"):
        journal.load_account_snapshot("account-1")
    with localcontext() as verification_context:
        verification_context.prec = 34
        assert journal.load_recovery_snapshot().journal_sequence == sequence


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
