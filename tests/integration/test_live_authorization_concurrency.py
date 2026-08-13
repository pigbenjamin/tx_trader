from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
import sqlite3
from threading import Barrier

import pytest

from tests.support.live_authorization_audit_scenarios import (
    FLOW_NOW,
    database_state,
    create_sealed_authorization_flow,
    prepare_authorized_flow,
)
from tx_trade.orders.live_journal_contracts import JournalOpenMode
from tx_trade.orders.live_reconciliation_commit_contracts import (
    ReconciliationCommitDisposition,
)
from tx_trade.orders.sqlite_live_order_journal import (
    SqliteLiveOrderJournal,
    _ConnectionBoundJournalReadView,
)

from tests.unit.test_live_journal_v3_migration import _create_v2


def _race(path: Path, authorized_requests: tuple[object, object]):
    gate = Barrier(2)

    def commit(authorized: object):
        journal = SqliteLiveOrderJournal(
            path,
            JournalOpenMode.RESUME,
            clock=lambda: authorized.authorization.authorized_at,  # type: ignore[attr-defined]
            claim_token_factory=lambda: "unused-concurrent-token",
        )
        try:
            gate.wait(timeout=10)
            return journal.commit_authorized_reconciliation(authorized)  # type: ignore[arg-type]
        finally:
            journal.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = tuple(executor.submit(commit, item) for item in authorized_requests)
        return tuple(future.result(timeout=20) for future in futures)


def test_two_synchronized_v2_resume_openers_share_one_migration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "concurrent-v2-resume.sqlite3"
    _create_v2(path)
    transaction_gate = Barrier(2)
    original_transaction = SqliteLiveOrderJournal._transaction

    @contextmanager
    def synchronized_transaction(journal: SqliteLiveOrderJournal):
        transaction_gate.wait(timeout=20)
        with original_transaction(journal):
            yield

    monkeypatch.setattr(SqliteLiveOrderJournal, "_transaction", synchronized_transaction)

    connections = tuple(
        sqlite3.connect(path, isolation_level=None, check_same_thread=False) for _ in range(2)
    )
    views = tuple(_ConnectionBoundJournalReadView(connection) for connection in connections)
    for connection, view in zip(connections, views):
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 20000")
        view._clock = lambda: FLOW_NOW

    def resume(view: _ConnectionBoundJournalReadView) -> int:
        view._resume_or_migrate()
        view._identity = view._load_identity()
        return view.load_recovery_snapshot().journal_sequence

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = tuple(executor.submit(resume, view) for view in views)
            assert tuple(future.result(timeout=20) for future in futures) == (3, 3)
    finally:
        for connection in connections:
            connection.close()

    monkeypatch.undo()
    state = dict(database_state(path))
    assert [row[2] for row in state["live_journal_records"]].count("3") == 1
    reopened = SqliteLiveOrderJournal(
        path,
        JournalOpenMode.RESUME,
        clock=lambda: FLOW_NOW,
        claim_token_factory=lambda: "unused-v2-reopen-token",
    )
    try:
        assert reopened.load_recovery_snapshot().journal_sequence == 3
    finally:
        reopened.close()


@pytest.mark.parametrize("iteration", range(3))
def test_two_handles_same_exact_authorization_linearize_once(
    tmp_path: Path, iteration: int
) -> None:
    path = tmp_path / f"same-{iteration}.sqlite3"
    create_sealed_authorization_flow(path)
    flow = prepare_authorized_flow(path)
    flow.journal.close()

    results = _race(path, (flow.authorized, flow.authorized))

    assert sorted(item.disposition.value for item in results) == ["committed", "exact_retry"]
    assert results[0].committed_at == results[1].committed_at
    assert results[0].resulting_journal_sequence == results[1].resulting_journal_sequence
    state = dict(database_state(path))
    assert len(state["live_reconciliation_commits"]) == 1
    assert len(state["live_reconciliation_commit_authorizations"]) == 1
    assert sum(row[1] == "reconciliation-commit" for row in state["live_journal_records"]) == 1


@pytest.mark.parametrize("iteration", range(3))
def test_two_distinct_authorizations_for_same_cut_commit_at_most_once(
    tmp_path: Path, iteration: int
) -> None:
    path = tmp_path / f"different-{iteration}.sqlite3"
    create_sealed_authorization_flow(path)
    first = prepare_authorized_flow(
        path, commit_id=f"commit-a-{iteration}", authorization_id=f"auth-a-{iteration}"
    )
    first.journal.close()
    second = prepare_authorized_flow(
        path, commit_id=f"commit-b-{iteration}", authorization_id=f"auth-b-{iteration}"
    )
    second.journal.close()

    results = _race(path, (first.authorized, second.authorized))

    dispositions = {item.disposition for item in results}
    assert ReconciliationCommitDisposition.COMMITTED in dispositions
    assert (
        len(
            [
                item
                for item in results
                if item.disposition is ReconciliationCommitDisposition.COMMITTED
            ]
        )
        == 1
    )
    assert dispositions <= {
        ReconciliationCommitDisposition.COMMITTED,
        ReconciliationCommitDisposition.STALE_SNAPSHOT,
        ReconciliationCommitDisposition.ID_CONFLICT,
    }
    state = dict(database_state(path))
    assert len(state["live_reconciliation_commits"]) == 1
    assert len(state["live_reconciliation_commit_authorizations"]) == 1
