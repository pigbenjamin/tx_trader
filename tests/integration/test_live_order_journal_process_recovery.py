from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import os
from pathlib import Path
import subprocess
import sys

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
from tx_trade.orders.live_journal_contracts import (
    JournalOpenMode,
    RegistrationDisposition,
    intent_fingerprint,
)
from tx_trade.orders.live_journal_recovery import (
    PendingRecoveryKind,
    RecoveryIssueCode,
    RecoveryReadiness,
    verify_recovery_snapshot,
)
from tx_trade.orders.live_ports import (
    DispatchClaimDisposition,
    JournalAppendDisposition,
    RawBrokerObservation,
)
from tx_trade.orders.live_state_machine import advance_local, create_live_order
from tx_trade.orders.sqlite_live_order_journal import SqliteLiveOrderJournal

NOW = datetime(2026, 7, 30, tzinfo=timezone.utc)


def _clock() -> datetime:
    return NOW + timedelta(seconds=10)


def _claim_token_factory(prefix: str):
    sequence = iter(range(1, 100))
    return lambda: f"{prefix}-{next(sequence)}"


def _submission() -> tuple[NewOrderCommand, object]:
    intent = LiveOrderIntent(
        strategy_id="strategy-integration",
        client_order_id="order-integration-1",
        account_id="account-integration",
        instrument_id="TXF",
        side=LiveSide.BUY,
        quantity=Decimal("1"),
        order_type=LiveOrderType.LIMIT,
        limit_price=Decimal("22000"),
        time_in_force=LiveTimeInForce.DAY,
        day_trade=False,
        created_at=NOW,
    )
    command = NewOrderCommand("command-integration-1", intent, NOW + timedelta(seconds=1))
    fingerprint = payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1)
    order = create_live_order(intent)
    order = advance_local(order, LiveOrderState.VALIDATED, NOW + timedelta(milliseconds=1))
    order = advance_local(
        order,
        LiveOrderState.SUBMITTING,
        command.requested_at,
        PendingCommandBinding(command, fingerprint),
    )
    return command, order


def test_close_resume_preserves_exact_retries_and_raw_before_normalize(
    tmp_path: Path,
) -> None:
    path = tmp_path / "close-resume.sqlite3"
    command, order = _submission()
    observation = RawBrokerObservation(
        "observation-integration-1",
        "fake-reply",
        1,
        1,
        NOW + timedelta(seconds=2),
        b"opaque-fake-reply",
    )

    journal = SqliteLiveOrderJournal(
        path,
        JournalOpenMode.CREATE_NEW,
        journal_id="journal-integration-1",
        clock=_clock,
        claim_token_factory=_claim_token_factory("claim-close-resume"),
    )
    registration = journal.register_new_order(
        command,
        order,
        intent_fingerprint=intent_fingerprint(command.intent),
    )
    assert registration.disposition is RegistrationDisposition.REGISTERED
    assert (
        journal.append_raw_observation(observation).disposition is JournalAppendDisposition.APPENDED
    )
    journal.close()

    resumed = SqliteLiveOrderJournal(
        path,
        JournalOpenMode.RESUME,
        clock=_clock,
        claim_token_factory=_claim_token_factory("claim-resumed"),
    )
    retry = resumed.register_new_order(
        command,
        order,
        intent_fingerprint=intent_fingerprint(command.intent),
    )
    assert retry.disposition is RegistrationDisposition.EXACT_RETRY
    assert (
        resumed.append_raw_observation(observation).disposition
        is JournalAppendDisposition.EXACT_DUPLICATE
    )
    snapshot = resumed.load_recovery_snapshot()
    resumed.close()

    assert snapshot.orders == (order,)
    assert snapshot.unresolved_observations == (observation,)
    verification = verify_recovery_snapshot(snapshot)
    assert verification.readiness is RecoveryReadiness.RECONCILIATION_REQUIRED
    assert RecoveryIssueCode.UNRESOLVED_OBSERVATION in verification.issues
    assert RecoveryIssueCode.PENDING_BROKER_EVIDENCE in verification.issues
    assert not verification.may_dispatch


def test_committed_claim_survives_abrupt_process_exit_and_never_authorizes_resend(
    tmp_path: Path,
) -> None:
    path = tmp_path / "abrupt-exit.sqlite3"
    child = r"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import os
import sys

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
from tx_trade.orders.live_journal_contracts import (
    JournalOpenMode,
    intent_fingerprint,
)
from tx_trade.orders.live_ports import RawBrokerObservation
from tx_trade.orders.live_state_machine import advance_local, create_live_order
from tx_trade.orders.sqlite_live_order_journal import SqliteLiveOrderJournal

now = datetime(2026, 7, 30, tzinfo=timezone.utc)
claim_tokens = iter(range(1, 100))
intent = LiveOrderIntent(
    strategy_id="strategy-integration",
    client_order_id="order-integration-1",
    account_id="account-integration",
    instrument_id="TXF",
    side=LiveSide.BUY,
    quantity=Decimal("1"),
    order_type=LiveOrderType.LIMIT,
    limit_price=Decimal("22000"),
    time_in_force=LiveTimeInForce.DAY,
    day_trade=False,
    created_at=now,
)
command = NewOrderCommand("command-integration-1", intent, now + timedelta(seconds=1))
fingerprint = payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1)
order = create_live_order(intent)
order = advance_local(order, LiveOrderState.VALIDATED, now + timedelta(milliseconds=1))
order = advance_local(
    order,
    LiveOrderState.SUBMITTING,
    command.requested_at,
    PendingCommandBinding(command, fingerprint),
)
journal = SqliteLiveOrderJournal(
    sys.argv[1],
    JournalOpenMode.CREATE_NEW,
    journal_id="journal-abrupt-1",
    clock=lambda: now + timedelta(seconds=10),
    claim_token_factory=lambda: f"claim-child-{next(claim_tokens)}",
)
journal.register_new_order(
    command,
    order,
    intent_fingerprint=intent_fingerprint(intent),
)
journal.claim_dispatch(
    command.client_command_id,
    fingerprint,
    expected_order_version=order.version,
    claimant_id="fake-dispatcher-child",
)
journal.append_raw_observation(
    RawBrokerObservation(
        "observation-abrupt-1",
        "fake-reply",
        1,
        1,
        now + timedelta(seconds=2),
        b"committed-before-abrupt-exit",
    )
)
os._exit(0)
"""
    completed = subprocess.run(
        [sys.executable, "-B", "-c", child, str(path)],
        cwd=Path.cwd(),
        env=os.environ.copy(),
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr

    command, order = _submission()
    fingerprint = payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1)
    resumed = SqliteLiveOrderJournal(
        path,
        JournalOpenMode.RESUME,
        clock=_clock,
        claim_token_factory=_claim_token_factory("claim-parent"),
    )
    snapshot = resumed.load_recovery_snapshot()
    second_claim = resumed.claim_dispatch(
        command.client_command_id,
        fingerprint,
        expected_order_version=order.version,
        claimant_id="fake-dispatcher-parent",
    )
    resumed.close()

    assert snapshot.orders == (order,)
    assert len(snapshot.outstanding_claims) == 1
    assert snapshot.outstanding_claims[0].command == command
    assert len(snapshot.unresolved_observations) == 1
    assert second_claim.disposition is DispatchClaimDisposition.ALREADY_CLAIMED
    assert second_claim.claim_token is None

    verification = verify_recovery_snapshot(snapshot)
    assert verification.readiness is RecoveryReadiness.RECONCILIATION_REQUIRED
    assert verification.pending[0].kind is PendingRecoveryKind.CLAIMED_OUTCOME_UNKNOWN
    assert not verification.pending[0].may_redispatch
    assert not verification.may_dispatch
    assert RecoveryIssueCode.OUTSTANDING_DISPATCH in verification.issues
