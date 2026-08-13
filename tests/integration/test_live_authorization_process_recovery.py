from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from tests.support.live_authorization_audit_scenarios import (
    database_state,
    create_sealed_authorization_flow,
    prepare_authorized_flow,
)
from tx_trade.orders.live_journal_contracts import JournalOpenMode
from tx_trade.orders.sqlite_live_order_journal import SqliteLiveOrderJournal


_CHILD = r"""
import json
import os
from pathlib import Path
import socket
import sys

from tests.support.live_authorization_audit_scenarios import database_state, prepare_authorized_flow
from tx_trade.orders.live_journal_contracts import JournalOpenMode
from tx_trade.orders.sqlite_live_order_journal import SqliteLiveOrderJournal

path = Path(sys.argv[1])
mode = sys.argv[2]

def poison(*args, **kwargs):
    raise AssertionError("forbidden online/runtime capability invoked")

socket.socket = poison
SqliteLiveOrderJournal.claim_dispatch = poison
SqliteLiveOrderJournal.record_dispatch_receipt = poison
flow = prepare_authorized_flow(path)
if mode == "before":
    os._exit(41)
if mode == "mid-resolution":
    assert len(flow.authorized.request.claim_resolutions) == 1
    original = SqliteLiveOrderJournal._append_record
    def abrupt(self, kind, *args, **kwargs):
        result = original(self, kind, *args, **kwargs)
        if kind == "dispatch-claim-resolution":
            os._exit(42)
        return result
    SqliteLiveOrderJournal._append_record = abrupt
    flow.journal.commit_authorized_reconciliation(flow.authorized)
if mode == "commit-reopen":
    committed = flow.journal.commit_authorized_reconciliation(flow.authorized)
    flow.journal.close()
    reopened = SqliteLiveOrderJournal(
        path,
        JournalOpenMode.RESUME,
        clock=lambda: flow.authorized.authorization.expires_at,
        claim_token_factory=lambda: "forbidden-token-factory",
    )
    before_retry = database_state(path)
    retry = reopened.commit_authorized_reconciliation(flow.authorized)
    snapshot = reopened.load_recovery_snapshot()
    after_retry = database_state(path)
    reopened.close()
    print(json.dumps({
        "first": committed.disposition.value,
        "retry": retry.disposition.value,
        "first_sequence": committed.resulting_journal_sequence,
        "retry_sequence": retry.resulting_journal_sequence,
        "same_time": committed.committed_at == retry.committed_at,
        "claims": len(snapshot.outstanding_claims),
        "sequence": snapshot.journal_sequence,
        "may_dispatch": flow.authorized.may_dispatch,
        "retry_no_write": before_retry == after_retry,
    }, sort_keys=True))
"""


def _run(path: Path, mode: str) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment.update(
        {
            "PYTHONDONTWRITEBYTECODE": "1",
            "TX_TRADE_CONFIG": "POISON-MUST-NOT-BE-READ",
            "TX_TRADE_CREDENTIALS": "POISON-MUST-NOT-BE-READ",
            "TX_TRADE_LIVE": "POISON-MUST-NOT-BE-READ",
        }
    )
    return subprocess.run(
        [sys.executable, "-c", _CHILD, str(path), mode],
        cwd=Path(__file__).parents[2],
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )


def _assert_reopens_with_state(path: Path, expected: object) -> None:
    journal = SqliteLiveOrderJournal(
        path,
        JournalOpenMode.RESUME,
        clock=lambda: __import__("datetime").datetime.datetime(
            2026, 8, 12, 1, tzinfo=__import__("datetime").datetime.timezone.utc
        ),
        claim_token_factory=lambda: "unused-process-check",
    )
    journal.load_recovery_snapshot()
    journal.close()
    assert database_state(path) == expected


def test_process_exit_before_authorized_call_preserves_exact_pre_state(tmp_path: Path) -> None:
    path = tmp_path / "exit-before.sqlite3"
    create_sealed_authorization_flow(path)
    before = database_state(path)

    child = _run(path, "before")

    assert child.returncode == 41, child.stderr
    _assert_reopens_with_state(path, before)


def test_abrupt_exit_after_resolution_fact_rolls_back_entire_transaction(
    tmp_path: Path,
) -> None:
    path = tmp_path / "exit-after-resolution.sqlite3"
    create_sealed_authorization_flow(path)
    before = database_state(path)

    child = _run(path, "mid-resolution")

    assert child.returncode == 42, child.stderr
    _assert_reopens_with_state(path, before)
    state = dict(database_state(path))
    assert state["live_reconciliation_commit_authorizations"] == ()
    assert state["live_reconciliation_commits"] == ()
    assert state["live_dispatch_claim_resolutions"] == ()
    assert state["live_observation_reconciliation_resolutions"] == ()
    assert state["live_reconciliation_requirement_resolutions"] == ()


def test_exception_after_resolution_fact_rolls_back_every_overlay_and_fact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "exception-after-resolution.sqlite3"
    create_sealed_authorization_flow(path)
    flow = prepare_authorized_flow(path)
    assert len(flow.authorized.request.claim_resolutions) == 1
    before = database_state(path)
    original = SqliteLiveOrderJournal._append_record

    def fail_after_resolution_fact(self, kind, *args, **kwargs):
        result = original(self, kind, *args, **kwargs)
        if kind == "dispatch-claim-resolution":
            raise RuntimeError("injected transaction failure")
        return result

    monkeypatch.setattr(SqliteLiveOrderJournal, "_append_record", fail_after_resolution_fact)
    with pytest.raises(RuntimeError, match="injected transaction failure"):
        flow.journal.commit_authorized_reconciliation(flow.authorized)
    flow.journal.close()

    _assert_reopens_with_state(path, before)


def test_committed_process_reopens_and_expired_exact_retry_is_no_write(
    tmp_path: Path,
) -> None:
    path = tmp_path / "commit-reopen.sqlite3"
    create_sealed_authorization_flow(path)

    child = _run(path, "commit-reopen")

    assert child.returncode == 0, child.stderr
    result = json.loads(child.stdout)
    assert result == {
        "claims": 0,
        "first": "committed",
        "first_sequence": 9,
        "may_dispatch": False,
        "retry": "exact_retry",
        "retry_no_write": True,
        "retry_sequence": 9,
        "same_time": True,
        "sequence": 9,
    }
    state = database_state(path)
    _assert_reopens_with_state(path, state)
