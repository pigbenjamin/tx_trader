from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
import sqlite3

from tx_trade.orders.live_journal_codec import encode_journal_value, journal_digest
from tx_trade.orders.live_contracts import (
    FingerprintDomain,
    LiveOrderIntent,
    LiveOrderState,
    LiveOrderType,
    LiveSide,
    LiveTimeInForce,
    NewOrderCommand,
    PendingCommandBinding,
    payload_fingerprint,
)
from tx_trade.orders.live_journal_contracts import JournalOpenMode, intent_fingerprint
from tx_trade.orders.live_operator_recovery import (
    build_operator_reconciliation_request,
    plan_operator_recovery,
)
from tx_trade.orders.live_operator_recovery_contracts import (
    ExplicitOperatorRecoverySelection,
    ExplicitOperatorRecoveryTargetSelection,
    OperatorRecoveryResolution,
)
from tx_trade.orders.live_reconciliation_authorization import (
    authorize_reconciliation_commit,
)
from tx_trade.orders.live_reconciliation_authorization_contracts import (
    AuthorizedReconciliationCommitRequest,
    ReconciliationAuthorizationAction,
    ReconciliationCommitAuthorization,
)
from tx_trade.orders.live_reconciliation_commit_contracts import (
    DurableReconciliationCommitRequest,
)
from tx_trade.orders.sqlite_live_order_journal import SqliteLiveOrderJournal
from tx_trade.orders.sqlite_live_reconciliation_assessment import (
    assess_sqlite_live_order_journal,
)
from tx_trade.orders.live_state_machine import advance_local, create_live_order

from tests.support.trusted_assessment_source_scenarios import (
    ACCOUNT_ID,
    COMMAND_ID,
    ORDER_ID,
    AtomicBrokerSource,
    CountingClock,
    complete_broker_snapshot,
)


_COMMIT_REQUEST_DOMAIN = "tx_trade.live.journal.reconciliation-commit-request.v2"
_TEST_DIGEST = f"sha256:{'a' * 64}"
_AUTHORIZATIONS: dict[tuple[str, str, str], AuthorizedReconciliationCommitRequest] = {}

FLOW_NOW = datetime(2026, 8, 12, tzinfo=timezone.utc)
FLOW_ORDER_ID = ORDER_ID
FLOW_COMMAND_ID = COMMAND_ID
FLOW_JOURNAL_ID = "journal-authorization-flow"


@dataclass(frozen=True, slots=True)
class AuthorizedFlow:
    journal: SqliteLiveOrderJournal
    authorized: AuthorizedReconciliationCommitRequest
    broker_calls: tuple[str, ...]


def database_state(path: Path) -> tuple[tuple[str, tuple[tuple[object, ...], ...]], ...]:
    """Return every durable table row in deterministic order."""

    connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro&immutable=1", uri=True)
    try:
        tables = tuple(
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
            )
            if not str(row[0]).startswith("sqlite_")
        )
        return tuple(
            (table, tuple(connection.execute(f'SELECT * FROM "{table}" ORDER BY rowid')))
            for table in tables
        )
    finally:
        connection.close()


def create_sealed_authorization_flow(path: Path) -> None:
    """Create a clean v3 journal containing one unresolved dispatch claim."""

    intent = LiveOrderIntent(
        "strategy-authorization-flow",
        FLOW_ORDER_ID,
        ACCOUNT_ID,
        "TXF-202608",
        LiveSide.BUY,
        Decimal("2"),
        LiveOrderType.LIMIT,
        Decimal("22000"),
        LiveTimeInForce.DAY,
        False,
        FLOW_NOW,
    )
    command = NewOrderCommand(FLOW_COMMAND_ID, intent, FLOW_NOW + timedelta(seconds=1))
    order = advance_local(create_live_order(intent), LiveOrderState.VALIDATED, FLOW_NOW)
    order = advance_local(
        order,
        LiveOrderState.SUBMITTING,
        command.requested_at,
        PendingCommandBinding(
            command, payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1)
        ),
    )
    order = replace(order, state=LiveOrderState.SUBMISSION_UNKNOWN)
    journal = SqliteLiveOrderJournal(
        path,
        JournalOpenMode.CREATE_NEW,
        journal_id=FLOW_JOURNAL_ID,
        clock=lambda: FLOW_NOW + timedelta(minutes=1),
        claim_token_factory=lambda: "claim-token-authorization-flow",
    )
    try:
        journal.register_new_order(
            command, order, intent_fingerprint=intent_fingerprint(command.intent)
        )
        journal.claim_dispatch(
            command.client_command_id,
            payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1),
            expected_order_version=order.version,
            claimant_id="offline-authorization-flow",
        )
    finally:
        journal.close()


def prepare_authorized_flow(
    path: Path,
    *,
    commit_id: str = "commit-authorization-flow",
    authorization_id: str = "auth-authorization-flow",
    clock_at: datetime | None = None,
    journal_id: str | None = None,
) -> AuthorizedFlow:
    """Run trusted assessment, operator planning, selection, and authorization."""

    source = AtomicBrokerSource(complete_broker_snapshot())
    inspected = assess_sqlite_live_order_journal(
        path,
        account_id=ACCOUNT_ID,
        broker_snapshot_source=source,
        clock=CountingClock(),
    )
    journal = SqliteLiveOrderJournal(
        path,
        JournalOpenMode.RESUME,
        clock=lambda: clock_at or FLOW_NOW + timedelta(minutes=3),
        claim_token_factory=lambda: "unused-authorization-flow",
    )
    recovery = journal.load_recovery_snapshot()
    plan = plan_operator_recovery(recovery, inspected.assessment)
    selection = ExplicitOperatorRecoverySelection(
        commit_id,
        ACCOUNT_ID,
        plan.journal_sequence,
        plan.inspection_digest,
        tuple(
            ExplicitOperatorRecoveryTargetSelection(
                target.kind,
                target.target_id,
                OperatorRecoveryResolution.BROKER_ORDER_CONFIRMED,
            )
            for target in plan.targets
        ),
    )
    request = build_operator_reconciliation_request(plan, selection, recovery, inspected.assessment)
    now = clock_at or FLOW_NOW + timedelta(minutes=3)
    authorized = authorize_reconciliation_commit(
        request=request,
        authorization_id=authorization_id,
        principal_id="offline-test-principal",
        authority_context_digest=_TEST_DIGEST,
        journal_id=journal_id or journal.identity.journal_id,
        source_inspection_digest=inspected.inspection.inspection_digest,
        operator_plan_digest="sha256:" + sha256(repr(plan).encode("utf-8")).hexdigest(),
        authorized_at=now - timedelta(seconds=1),
        expires_at=now + timedelta(minutes=4),
        reason_code="offline-recovery-approved",
    )
    return AuthorizedFlow(journal, authorized, tuple(source.calls))


def authorize(
    journal: SqliteLiveOrderJournal,
    request: DurableReconciliationCommitRequest,
    *,
    authorization_id: str | None = None,
) -> AuthorizedReconciliationCommitRequest:
    request_digest = journal_digest(_COMMIT_REQUEST_DOMAIN, encode_journal_value(request))
    key = (str(journal._path), request.commit_id, request_digest)
    existing = _AUTHORIZATIONS.get(key)
    if existing is not None and authorization_id is None:
        return existing
    now = journal._now()
    authorization = ReconciliationCommitAuthorization(
        authorization_id or f"auth-{request.commit_id}",
        "test-principal",
        _TEST_DIGEST,
        ReconciliationAuthorizationAction.RECONCILIATION_COMMIT,
        journal.identity.journal_id,
        request.account_id,
        _TEST_DIGEST,
        _TEST_DIGEST,
        request.commit_id,
        request_digest,
        request.assessment.broker_snapshot.snapshot_id,
        request.expected_journal_sequence,
        now,
        now + timedelta(minutes=5),
        "test-authorized",
    )
    authorized = AuthorizedReconciliationCommitRequest(authorization, request)
    if authorization_id is None:
        _AUTHORIZATIONS[key] = authorized
    return authorized


def commit_authorized(
    journal: SqliteLiveOrderJournal,
    request: DurableReconciliationCommitRequest,
):
    return journal.commit_authorized_reconciliation(authorize(journal, request))
