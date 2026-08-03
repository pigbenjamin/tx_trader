from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import os
from pathlib import Path
import sqlite3
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

from tests.integration.test_live_reconciliation_commit_fake import (
    ACCOUNT_ID as COMMIT_ACCOUNT_ID,
    _claim_request as _commit_claim_request,
    _open as _open_commit_journal,
)
from tx_trade.orders.live_reconciliation_commit_contracts import (
    ReconciliationCommitDisposition,
)

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


def _run_child(source: str, path: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-B", "-c", source, str(path), *arguments],
        cwd=Path.cwd(),
        env=os.environ.copy(),
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_abrupt_exit_before_or_after_reconciliation_commit_is_transactional(
    tmp_path: Path,
) -> None:
    child = r"""
import os
import sys

from tests.integration.test_live_reconciliation_commit_fake import (
    _claim_request,
    _open,
    _register_claim_and_accept,
)
from tx_trade.orders.live_journal_contracts import JournalOpenMode

journal = _open(sys.argv[1], JournalOpenMode.CREATE_NEW)
_register_claim_and_accept(journal)
request = _claim_request(journal, commit_id="commit-process-integration")
if sys.argv[2] == "after":
    result = journal.commit_reconciliation(request)
    assert result.disposition.value == "committed"
os._exit(0)
"""

    for phase in ("before", "after"):
        path = tmp_path / f"abrupt-commit-{phase}.sqlite3"
        completed = _run_child(child, path, phase)
        assert completed.returncode == 0, completed.stderr

        resumed = _open_commit_journal(path, JournalOpenMode.RESUME)
        recovered = resumed.load_recovery_snapshot()
        verification = verify_recovery_snapshot(recovered)
        assert not verification.may_dispatch
        with sqlite3.connect(path) as connection:
            sequences = tuple(
                int(row[0])
                for row in connection.execute(
                    "SELECT journal_sequence FROM live_journal_records ORDER BY journal_sequence"
                )
            )
        assert sequences == tuple(range(1, recovered.journal_sequence + 1))

        if phase == "before":
            assert len(recovered.outstanding_claims) == 1
            committed = resumed.commit_reconciliation(
                _commit_claim_request(resumed, commit_id="commit-process-parent")
            )
            assert committed.disposition is ReconciliationCommitDisposition.COMMITTED
        else:
            assert recovered.outstanding_claims == ()
            assert verification.readiness is RecoveryReadiness.READY
            assert resumed.load_account_snapshot(COMMIT_ACCOUNT_ID).recovery_blockers == ()

        after = resumed.load_recovery_snapshot()
        assert after.outstanding_claims == ()
        assert all(item.source != "dispatch" for item in after.applied_event_ledger.events)
        with sqlite3.connect(path) as connection:
            final_sequences = tuple(
                int(row[0])
                for row in connection.execute(
                    "SELECT journal_sequence FROM live_journal_records ORDER BY journal_sequence"
                )
            )
        assert final_sequences == tuple(range(1, after.journal_sequence + 1))
        resumed.close()


def test_abrupt_exit_at_v1_migration_boundary_recovers_whole_schema(tmp_path: Path) -> None:
    child = r'''
from datetime import datetime, timezone
from hashlib import sha256
import os
from pathlib import Path
import sqlite3
import sys

from tx_trade.orders.live_journal_codec import encode_journal_value, journal_digest
from tx_trade.orders.live_journal_contracts import JournalOpenMode, LiveJournalIdentity
from tx_trade.orders.sqlite_live_order_journal import SqliteLiveOrderJournal

path = Path(sys.argv[1])
schema_path = Path("tx_trade/orders/live_journal_schema_v1.sql")
schema = schema_path.read_text(encoding="utf-8")
fingerprint = f"sha256:{sha256(schema.encode('utf-8')).hexdigest()}"
created_at = datetime(2026, 8, 3, tzinfo=timezone.utc)
created_text = created_at.isoformat().replace("+00:00", "Z")
identity = LiveJournalIdentity("journal-v1-process", 1, fingerprint, created_at)
payload = encode_journal_value(identity)
digest = journal_digest("tx_trade.live.journal.identity.v1", payload)

connection = sqlite3.connect(path, isolation_level=None)
connection.executescript(schema)
connection.execute("BEGIN IMMEDIATE")
connection.execute(
    "INSERT INTO live_journal_migrations(version, schema_fingerprint) VALUES (1, ?)",
    (fingerprint,),
)
connection.execute(
    """INSERT INTO live_journal_identity(
           singleton, journal_id, schema_version, schema_fingerprint, created_at
       ) VALUES (1, ?, 1, ?, ?)""",
    (identity.journal_id, fingerprint, created_text),
)
connection.execute(
    """INSERT INTO live_journal_records(
           record_kind, record_id, payload_digest, recorded_at
       ) VALUES ('identity', ?, ?, ?)""",
    (identity.journal_id, digest, created_text),
)
connection.execute("COMMIT")
if sys.argv[2] == "after":
    journal = SqliteLiveOrderJournal(
        path,
        JournalOpenMode.RESUME,
        clock=lambda: datetime(2026, 8, 3, 1, tzinfo=timezone.utc),
        claim_token_factory=lambda: "unused-migration-claim",
    )
    assert journal.load_recovery_snapshot().journal_sequence == 2
os._exit(0)
'''

    for phase in ("before", "after"):
        path = tmp_path / f"migration-{phase}.sqlite3"
        completed = _run_child(child, path, phase)
        assert completed.returncode == 0, completed.stderr

        with sqlite3.connect(path) as connection:
            version_before_resume = int(connection.execute("PRAGMA user_version").fetchone()[0])
            commit_tables = int(
                connection.execute(
                    """SELECT count(*) FROM sqlite_master
                       WHERE type = 'table' AND name = 'live_reconciliation_commits'"""
                ).fetchone()[0]
            )
        if phase == "before":
            assert (version_before_resume, commit_tables) == (1, 0)
        else:
            assert (version_before_resume, commit_tables) == (2, 1)

        resumed = SqliteLiveOrderJournal(
            path,
            JournalOpenMode.RESUME,
            clock=_clock,
            claim_token_factory=_claim_token_factory("migration-parent"),
        )
        snapshot = resumed.load_recovery_snapshot()
        assert resumed.identity.schema_version == 1
        assert snapshot.journal_sequence == 2
        assert snapshot.orders == ()
        assert snapshot.outstanding_claims == ()
        assert not verify_recovery_snapshot(snapshot).may_dispatch
        resumed.close()
