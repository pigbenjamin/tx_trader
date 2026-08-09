from __future__ import annotations

import os
from pathlib import Path
import sqlite3
import subprocess
import sys

from tx_trade.orders.live_contracts import LiveOrderState
from tx_trade.orders.live_journal_contracts import JournalOpenMode
from tx_trade.orders.live_operator_recovery import build_operator_reconciliation_request
from tx_trade.orders.live_reconciliation_commit_contracts import (
    ReconciliationCommitDisposition,
)

from tests.integration.test_live_operator_projection_fake import (
    ACCOUNT_ID,
    ORDER_ID,
    _create_claim,
    _open,
    _plan,
    _selection,
)

MID_WRITE_CRASH_EXIT = 47
PROJECTION_WRITE_FAILED_EXIT = 48


def _child(path: Path, stage: str) -> None:
    journal = _open(path, JournalOpenMode.CREATE_NEW)
    _, order = _create_claim(journal)
    recovery, assessment, plan = _plan(journal, order)
    request = build_operator_reconciliation_request(
        plan,
        _selection(plan, "commit-process-recovery"),
        recovery,
        assessment,
    )
    if stage == "before":
        os._exit(0)
    if stage == "mid-write":
        write_order = journal._write_order

        def write_order_then_crash(order: object, expected_version: int | None) -> bool:
            if not write_order(order, expected_version):
                os._exit(PROJECTION_WRITE_FAILED_EXIT)
            os._exit(MID_WRITE_CRASH_EXIT)

        journal._write_order = write_order_then_crash
    committed = journal.commit_reconciliation(request)
    if committed.disposition is not ReconciliationCommitDisposition.COMMITTED:
        os._exit(3)
    os._exit(0)


def _run_child(path: Path, stage: str, *, expected_exit: int = 0) -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "-B",
            "-m",
            "tests.integration.test_live_operator_projection_process_recovery",
            "--child",
            stage,
            str(path),
        ],
        cwd=Path(__file__).parents[2],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=20,
        check=False,
    )
    if completed.returncode != expected_exit:
        raise AssertionError(
            f"offline child exited with code {completed.returncode}, expected {expected_exit}"
        )
    assert completed.stdout == b""
    assert completed.stderr == b""


def _facts(path: Path) -> tuple[object, ...]:
    connection = sqlite3.connect(path)
    try:
        return (
            connection.execute("SELECT count(*) FROM live_reconciliation_commits").fetchone()[0],
            connection.execute("SELECT count(*) FROM live_dispatch_claim_resolutions").fetchone()[
                0
            ],
            connection.execute("SELECT count(*) FROM live_order_history").fetchone()[0],
            connection.execute("SELECT count(*) FROM live_dispatch_receipts").fetchone()[0],
            connection.execute("SELECT count(*) FROM live_normalized_events").fetchone()[0],
            connection.execute("SELECT count(*) FROM live_fills").fetchone()[0],
            connection.execute(
                """SELECT record_kind FROM live_journal_records
                   ORDER BY journal_sequence DESC LIMIT 1"""
            ).fetchone()[0],
            connection.execute(
                "SELECT coalesce(max(journal_sequence), 0) FROM live_journal_records"
            ).fetchone()[0],
        )
    finally:
        connection.close()


def test_abrupt_exit_after_durable_commit_resumes_complete_projection(tmp_path: Path) -> None:
    path = tmp_path / "operator-process-after.sqlite3"
    _run_child(path, "after")

    resumed = _open(path, JournalOpenMode.RESUME)
    recovery = resumed.load_recovery_snapshot()
    order = resumed.get_order(ORDER_ID)
    assert order is not None and order.state is LiveOrderState.ACCEPTED
    assert recovery.orders == (order,)
    assert recovery.outstanding_claims == ()
    assert recovery.applied_event_ledger.events == ()
    assert resumed.load_account_snapshot(ACCOUNT_ID).recovery_blockers == ()
    assert resumed.load_account_snapshot(ACCOUNT_ID).fills == ()
    assert _facts(path) == (1, 1, 2, 0, 0, 0, "reconciliation-commit", 7)
    resumed.close()


def test_abrupt_exit_before_commit_preserves_original_blocked_cut(tmp_path: Path) -> None:
    path = tmp_path / "operator-process-before.sqlite3"
    _run_child(path, "before")

    resumed = _open(path, JournalOpenMode.RESUME)
    recovery = resumed.load_recovery_snapshot()
    order = resumed.get_order(ORDER_ID)
    assert order is not None and order.state is LiveOrderState.SUBMISSION_UNKNOWN
    assert len(recovery.outstanding_claims) == 1
    assert resumed.load_account_snapshot(ACCOUNT_ID).recovery_blockers
    assert _facts(path) == (0, 0, 1, 0, 0, 0, "dispatch-claim", 5)
    resumed.close()


def test_abrupt_exit_during_projection_write_rolls_back_entire_commit(
    tmp_path: Path,
) -> None:
    path = tmp_path / "operator-process-mid-write.sqlite3"
    _run_child(path, "mid-write", expected_exit=MID_WRITE_CRASH_EXIT)

    resumed = _open(path, JournalOpenMode.RESUME)
    recovery = resumed.load_recovery_snapshot()
    order = resumed.get_order(ORDER_ID)
    assert order is not None and order.state is LiveOrderState.SUBMISSION_UNKNOWN
    assert recovery.orders == (order,)
    assert len(recovery.outstanding_claims) == 1
    account = resumed.load_account_snapshot(ACCOUNT_ID)
    assert account.recovery_blockers
    _, assessment, plan = _plan(resumed, order)
    assert not assessment.may_dispatch
    assert not plan.may_dispatch
    assert _facts(path) == (0, 0, 1, 0, 0, 0, "dispatch-claim", 5)
    resumed.close()


if __name__ == "__main__" and len(sys.argv) == 4 and sys.argv[1] == "--child":
    _child(Path(sys.argv[3]), sys.argv[2])
