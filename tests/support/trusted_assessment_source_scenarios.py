"""Real-journal and fake-broker fixtures for trusted assessment integration tests."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

from tx_trade.orders.live_contracts import (
    BrokerCorrelation,
    BrokerOpenOrderObservation,
    BrokerOrderEventType,
    CorrelationStatus,
    DispatchReceipt,
    DispatchState,
    FingerprintDomain,
    LiveOrderIntent,
    LiveOrderState,
    LiveOrderType,
    LiveFailureCode,
    LiveSide,
    LiveTimeInForce,
    NewOrderCommand,
    NormalizedBrokerOrderEvent,
    PendingCommandBinding,
    payload_fingerprint,
)
from tx_trade.orders.live_journal_contracts import (
    JournalOpenMode,
    LiveJournalRecoverySnapshot,
    intent_fingerprint,
)
from tx_trade.orders.live_ports import (
    BrokerFillsSnapshot,
    BrokerPositionsSnapshot,
    CompletenessEvidence,
    EvidenceCompleteness,
    EvidenceQueryKind,
    OpenOrdersSnapshot,
    RawBrokerObservation,
)
from tx_trade.orders.live_reconciliation_contracts import BrokerReconciliationSnapshot
from tx_trade.orders.live_state_machine import advance_local, create_live_order
from tx_trade.orders.sqlite_live_order_journal import SqliteLiveOrderJournal

ACCOUNT_ID = "account-trusted-source"
ORDER_ID = "order-trusted-source"
COMMAND_ID = "command-trusted-source"
INSTRUMENT_ID = "TXF-202608"
SNAPSHOT_ID = "snapshot-trusted-source"
CREATED_AT = datetime(2026, 8, 12, tzinfo=timezone.utc)
ACCEPTED_AT = CREATED_AT + timedelta(seconds=2)
CAPTURED_AT = CREATED_AT + timedelta(minutes=2)
RECONCILED_AT = CAPTURED_AT + timedelta(seconds=1)


@dataclass(slots=True)
class AtomicBrokerSource:
    snapshot: object
    expected_account_id: str = ACCOUNT_ID
    failure: BaseException | None = None
    calls: list[str] = field(default_factory=list)

    def query_reconciliation_snapshot(self, account_id: str) -> object:
        assert account_id == self.expected_account_id
        self.calls.append(account_id)
        if self.failure is not None:
            raise self.failure
        return self.snapshot


@dataclass(slots=True)
class CountingClock:
    values: tuple[datetime, ...] = (CAPTURED_AT, RECONCILED_AT)
    calls: int = 0

    def now(self) -> datetime:
        value = self.values[self.calls % len(self.values)]
        self.calls += 1
        return value


def directory_snapshot(directory: Path) -> dict[str, tuple[bytes, int, int, str]]:
    """Capture content and metadata for every regular file in a fixture directory."""

    return {
        item.name: (
            (content := item.read_bytes()),
            item.stat().st_size,
            item.stat().st_mtime_ns,
            sha256(content).hexdigest(),
        )
        for item in sorted(directory.iterdir())
        if item.is_file()
    }


def _submitting_order() -> tuple[NewOrderCommand, object]:
    intent = LiveOrderIntent(
        strategy_id="strategy-trusted-source",
        client_order_id=ORDER_ID,
        account_id=ACCOUNT_ID,
        instrument_id=INSTRUMENT_ID,
        side=LiveSide.BUY,
        quantity=Decimal("2"),
        order_type=LiveOrderType.LIMIT,
        limit_price=Decimal("22000"),
        time_in_force=LiveTimeInForce.DAY,
        day_trade=False,
        created_at=CREATED_AT,
    )
    command = NewOrderCommand(COMMAND_ID, intent, CREATED_AT + timedelta(seconds=1))
    order = advance_local(create_live_order(intent), LiveOrderState.VALIDATED, CREATED_AT)
    order = advance_local(
        order,
        LiveOrderState.SUBMITTING,
        command.requested_at,
        PendingCommandBinding(
            command,
            payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1),
        ),
    )
    return command, order


def create_sealed_v3(
    path: Path,
    *,
    submission_unknown: bool = False,
) -> LiveJournalRecoverySnapshot:
    """Create and cleanly close a real v3 journal for the requested scenario."""

    command, order = _submitting_order()
    journal = SqliteLiveOrderJournal(
        path,
        JournalOpenMode.CREATE_NEW,
        journal_id="journal-trusted-source",
        clock=lambda: CREATED_AT + timedelta(minutes=1),
        claim_token_factory=lambda: "claim-token-trusted-source",
    )
    try:
        journal.register_new_order(
            command,
            order,
            intent_fingerprint=intent_fingerprint(command.intent),
        )
        if submission_unknown:
            claim = journal.claim_dispatch(
                command.client_command_id,
                payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1),
                expected_order_version=order.version,
                claimant_id="claimant-trusted-source",
            )
            assert claim.claim_token is not None
            journal.record_dispatch_receipt(
                DispatchReceipt(
                    command.client_command_id,
                    payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1),
                    DispatchState.UNKNOWN,
                    CREATED_AT + timedelta(seconds=3),
                    None,
                    LiveFailureCode.DISPATCH_OUTCOME_UNKNOWN,
                ),
                claim_token=claim.claim_token,
                expected_order_version=order.version,
            )
        else:
            raw = RawBrokerObservation(
                "raw-accepted-trusted-source",
                "fake-broker",
                1,
                1,
                ACCEPTED_AT,
                b"accepted",
            )
            journal.append_raw_observation(raw)
            event = NormalizedBrokerOrderEvent(
                "accepted-trusted-source",
                ACCOUNT_ID,
                INSTRUMENT_ID,
                BrokerOrderEventType.NEW_ACCEPTED,
                ACCEPTED_AT,
                1,
                1,
                BrokerCorrelation(
                    1,
                    1,
                    CorrelationStatus.CONFIRMED,
                    ACCEPTED_AT,
                    broker_order_sequence="broker-order-trusted-source",
                    client_order_id=ORDER_ID,
                ),
            )
            journal.apply_normalized_event(
                event,
                raw_observation_id=raw.observation_id,
                expected_order_version=order.version,
            )
        return journal.load_recovery_snapshot()
    finally:
        journal.close()


def complete_broker_snapshot(
    *,
    account_id: str = ACCOUNT_ID,
    snapshot_id: str = SNAPSHOT_ID,
    evidence_account_id: str | None = None,
    evidence_cursor: str | None = None,
    evidence_status: EvidenceCompleteness = EvidenceCompleteness.COMPLETE,
    correlation_status: CorrelationStatus = CorrelationStatus.CONFIRMED,
    evidence_at: datetime = CAPTURED_AT,
    captured_at: datetime = CAPTURED_AT,
) -> BrokerReconciliationSnapshot:
    evidence_account = evidence_account_id or account_id
    cursor = evidence_cursor or snapshot_id

    def evidence(kind: EvidenceQueryKind) -> CompletenessEvidence:
        return CompletenessEvidence(
            kind,
            evidence_account,
            evidence_status,
            evidence_at,
            cursor,
            None
            if evidence_status is EvidenceCompleteness.COMPLETE
            else "fake-incomplete-evidence",
        )

    broker_order = BrokerOpenOrderObservation(
        "broker-open-trusted-source",
        account_id,
        INSTRUMENT_ID,
        LiveSide.BUY,
        Decimal("2"),
        Decimal("2"),
        Decimal("22000"),
        BrokerCorrelation(
            1,
            1,
            correlation_status,
            ACCEPTED_AT,
            broker_order_sequence="broker-order-trusted-source",
            client_order_id=ORDER_ID,
        ),
        evidence_at,
    )
    return BrokerReconciliationSnapshot(
        snapshot_id,
        account_id,
        OpenOrdersSnapshot((broker_order,), evidence(EvidenceQueryKind.OPEN_ORDERS)),
        BrokerFillsSnapshot((), evidence(EvidenceQueryKind.FILLS)),
        BrokerPositionsSnapshot((), evidence(EvidenceQueryKind.POSITIONS)),
        captured_at,
    )


def forged_snapshot(
    snapshot: BrokerReconciliationSnapshot,
    *,
    account_id: str | None = None,
    open_orders: OpenOrdersSnapshot | None = None,
) -> BrokerReconciliationSnapshot:
    """Bypass constructor validation to exercise a hostile source boundary."""

    forged = object.__new__(BrokerReconciliationSnapshot)
    for name, value in (
        ("snapshot_id", snapshot.snapshot_id),
        ("account_id", account_id or snapshot.account_id),
        ("open_orders", open_orders or snapshot.open_orders),
        ("fills", snapshot.fills),
        ("positions", snapshot.positions),
        ("captured_at", snapshot.captured_at),
    ):
        object.__setattr__(forged, name, value)
    return forged
