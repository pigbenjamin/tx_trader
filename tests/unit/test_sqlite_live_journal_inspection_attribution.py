from __future__ import annotations

from pathlib import Path
import sqlite3

import pytest

from tests.support.live_journal_inspection_scenarios import (
    AttributionBlocker,
    AttributionState,
    create_attribution_scenario,
)
from tx_trade.orders.live_journal_inspection_contracts import (
    LiveJournalInspectionDisposition,
    LiveJournalInspectionIssueCode,
)
from tx_trade.orders.sqlite_live_journal_inspection import (
    inspect_sqlite_live_order_journal,
)


BLOCKER_ISSUES = frozenset(
    {
        LiveJournalInspectionIssueCode.UNRESOLVED_OBSERVATION,
        LiveJournalInspectionIssueCode.CONFLICT_OBSERVATION,
        LiveJournalInspectionIssueCode.AMBIGUOUS_OBSERVATION,
        LiveJournalInspectionIssueCode.DURABLE_RECONCILIATION_REQUIREMENT,
        LiveJournalInspectionIssueCode.GLOBAL_RECOVERY_BLOCKER,
    }
)


def _artifact_snapshot(path: Path) -> dict[str, tuple[bytes, int, int]]:
    result: dict[str, tuple[bytes, int, int]] = {}
    for artifact in (path, Path(f"{path}-wal"), Path(f"{path}-shm"), Path(f"{path}-journal")):
        if artifact.exists():
            stat = artifact.stat()
            result[artifact.name] = (artifact.read_bytes(), stat.st_size, stat.st_mtime_ns)
    return result


def _assert_cross_account_global_requirement_shape(path: Path, selected_account: str) -> None:
    connection = sqlite3.connect(
        f"{path.resolve().as_uri()}?mode=ro&immutable=1",
        uri=True,
    )
    try:
        rows = connection.execute(
            """SELECT o.account_id, r.observation_id, raw.resolution_status,
                      a.client_order_id, a.disposition
               FROM live_reconciliation_requirements r
               JOIN live_orders o ON o.client_order_id = r.client_order_id
               JOIN live_raw_observations raw ON raw.observation_id = r.observation_id
               JOIN live_normalized_events n
                 ON n.raw_observation_id = r.observation_id
               JOIN live_event_applications a
                 ON a.source = n.source AND a.event_id = n.event_id
               WHERE r.resolved_at IS NULL"""
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == selected_account
        assert type(rows[0][1]) is str
        assert rows[0][2:] == ("unresolved", None, "unresolved")
    finally:
        connection.close()


def _assert_resolved_ambiguity_history(path: Path, selected_account: str) -> None:
    connection = sqlite3.connect(
        f"{path.resolve().as_uri()}?mode=ro&immutable=1",
        uri=True,
    )
    try:
        history = connection.execute(
            """SELECT raw.observation_id, raw.resolution_status,
                      a.disposition, rr.expected_resolution_status,
                      rr.normalized_event_id
               FROM live_raw_observations raw
               JOIN live_normalized_events n
                 ON n.raw_observation_id = raw.observation_id
               JOIN live_event_applications a
                 ON a.source = n.source AND a.event_id = n.event_id
               JOIN live_observation_reconciliation_resolutions rr
                 ON rr.observation_id = raw.observation_id
               WHERE raw.resolution_status = 'ambiguous'"""
        ).fetchall()
        assert len(history) == 1
        observation_id, status, disposition, expected_status, event_id = history[0]
        assert type(observation_id) is str
        assert type(event_id) is str
        assert (status, disposition, expected_status) == (
            "ambiguous",
            "unresolved",
            "ambiguous",
        )
        candidates = connection.execute(
            """SELECT o.account_id
               FROM live_observation_ambiguity a
               JOIN live_orders o
                 ON o.client_order_id = a.candidate_client_order_id
               WHERE a.observation_id = ?
               ORDER BY a.candidate_client_order_id""",
            (observation_id,),
        ).fetchall()
        assert candidates == [(selected_account,), (selected_account,)]
        assert connection.execute(
            """SELECT count(*) FROM live_journal_records
               WHERE record_kind = 'observation-resolution' AND record_id = ?""",
            (observation_id,),
        ).fetchone() == (1,)
    finally:
        connection.close()


@pytest.mark.parametrize("blocker", tuple(AttributionBlocker), ids=lambda item: item.value)
@pytest.mark.parametrize("state", tuple(AttributionState), ids=lambda item: item.value)
def test_public_inspector_attribution_matrix_is_scoped_redacted_and_read_only(
    tmp_path: Path,
    blocker: AttributionBlocker,
    state: AttributionState,
) -> None:
    path = tmp_path / f"{blocker.value}-{state.value}.sqlite3"
    selected_account, foreign_account = create_attribution_scenario(
        path,
        blocker=blocker,
        state=state,
    )
    if blocker is AttributionBlocker.REQUIREMENT and state is AttributionState.GLOBAL:
        _assert_cross_account_global_requirement_shape(path, selected_account)
    if blocker is AttributionBlocker.AMBIGUOUS and state is AttributionState.RESOLVED:
        _assert_resolved_ambiguity_history(path, selected_account)
    before_artifacts = _artifact_snapshot(path)

    first = inspect_sqlite_live_order_journal(path, account_id=selected_account)
    second = inspect_sqlite_live_order_journal(path, account_id=selected_account)

    assert first == second
    assert first.inspection_digest == second.inspection_digest
    assert _artifact_snapshot(path) == before_artifacts
    assert set(before_artifacts) == {path.name}

    relevant = set(first.issue_codes) & BLOCKER_ISSUES
    if state is AttributionState.SELECTED:
        expected = {
            AttributionBlocker.UNRESOLVED: {
                LiveJournalInspectionIssueCode.UNRESOLVED_OBSERVATION,
            },
            AttributionBlocker.CONFLICT: {
                LiveJournalInspectionIssueCode.CONFLICT_OBSERVATION,
                LiveJournalInspectionIssueCode.DURABLE_RECONCILIATION_REQUIREMENT,
            },
            AttributionBlocker.AMBIGUOUS: {
                LiveJournalInspectionIssueCode.AMBIGUOUS_OBSERVATION,
            },
            AttributionBlocker.REQUIREMENT: {
                LiveJournalInspectionIssueCode.DURABLE_RECONCILIATION_REQUIREMENT,
            },
        }[blocker]
        assert relevant == expected
        assert first.disposition is LiveJournalInspectionDisposition.RECOVERY_REQUIRED
    elif state is AttributionState.GLOBAL:
        assert relevant == {LiveJournalInspectionIssueCode.GLOBAL_RECOVERY_BLOCKER}
        assert first.disposition is LiveJournalInspectionDisposition.RECOVERY_REQUIRED
    else:
        assert relevant == set()
        assert first.disposition is LiveJournalInspectionDisposition.READY_NO_ACTION

    rendered = f"{first!r} {first!s} {second!r} {second!s}"
    for secret in (
        selected_account,
        foreign_account,
        "foreign-order-secret",
        "foreign-command-secret",
        "observation-secret",
        "payload-secret",
        "broker-secret",
    ):
        assert secret not in rendered


def test_resolved_ambiguity_history_assertion_detects_missing_resolution(
    tmp_path: Path,
) -> None:
    path = tmp_path / "ambiguous-resolution-evidence.sqlite3"
    selected_account, _ = create_attribution_scenario(
        path,
        blocker=AttributionBlocker.AMBIGUOUS,
        state=AttributionState.RESOLVED,
    )
    _assert_resolved_ambiguity_history(path, selected_account)

    connection = sqlite3.connect(path)
    try:
        connection.execute("DELETE FROM live_observation_reconciliation_resolutions")
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(AssertionError):
        _assert_resolved_ambiguity_history(path, selected_account)
