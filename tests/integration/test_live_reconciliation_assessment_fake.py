from __future__ import annotations

from datetime import timedelta
import os
from pathlib import Path
import sqlite3
import sys

import pytest

from tx_trade.orders.live_contracts import CorrelationStatus
from tx_trade.orders.live_journal_inspection_contracts import (
    LiveJournalInspectionDisposition,
)
from tx_trade.orders.live_operator_recovery import plan_operator_recovery
from tx_trade.orders.live_operator_recovery_contracts import OperatorRecoveryDisposition
from tx_trade.orders.live_ports import EvidenceCompleteness, OpenOrdersSnapshot
from tx_trade.orders.live_reconciliation import assess_reconciliation
from tx_trade.orders.live_reconciliation_assessment_contracts import (
    InspectedReconciliationAssessment,
    MAX_TRUSTED_BROKER_OBSERVATIONS,
    TrustedAssessmentSourceError,
    TrustedAssessmentSourceFailureCode,
)
from tx_trade.orders.sqlite_live_journal_inspection import (
    MAX_INSPECTION_MAIN_DATABASE_BYTES,
    inspect_sqlite_live_order_journal,
)
from tx_trade.orders.sqlite_live_order_journal import SqliteLiveOrderJournal
from tx_trade.orders.sqlite_live_reconciliation_assessment import (
    assess_sqlite_live_order_journal,
)

from tests.support.live_journal_inspection_scenarios import (
    create_frozen_v1,
    create_frozen_v2,
    create_semantically_blocked_v2,
)
from tests.support.trusted_assessment_source_scenarios import (
    ACCEPTED_AT,
    ACCOUNT_ID,
    CREATED_AT,
    AtomicBrokerSource,
    CountingClock,
    complete_broker_snapshot,
    create_sealed_v3,
    directory_snapshot,
    forged_snapshot,
)


def _assess(path: Path, source: AtomicBrokerSource, clock: CountingClock):
    return assess_sqlite_live_order_journal(
        path,
        account_id=ACCOUNT_ID,
        broker_snapshot_source=source,
        clock=clock,
    )


def _forbid_mutating_journal_calls(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("trusted assessment attempted a mutating journal call")

    for name in (
        "claim_dispatch",
        "record_dispatch_receipt",
        "commit_authorized_reconciliation",
    ):
        monkeypatch.setattr(SqliteLiveOrderJournal, name, forbidden)


def test_clean_close_assessment_has_provenance_is_recomputed_and_never_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "clean.sqlite3"
    recovery = create_sealed_v3(path)
    expected_inspection = inspect_sqlite_live_order_journal(path, account_id=ACCOUNT_ID)
    before = directory_snapshot(tmp_path)
    source = AtomicBrokerSource(complete_broker_snapshot())
    clock = CountingClock()
    _forbid_mutating_journal_calls(monkeypatch)

    result = _assess(path, source, clock)

    assert type(result) is InspectedReconciliationAssessment
    assert result.inspection == expected_inspection
    assert result.inspection.disposition is LiveJournalInspectionDisposition.READY_NO_ACTION
    assert result.assessment == assess_reconciliation(
        result.assessment.local_snapshot,
        source.snapshot,  # type: ignore[arg-type]
        result.assessment.result.reconciled_at,
    )
    assert result.assessment.broker_snapshot == source.snapshot
    assert result.assessment.result.is_authoritative
    assert result.assessment.result.discrepancies == ()
    assert not result.may_dispatch
    assert not result.commit_allowed
    clean_plan = plan_operator_recovery(recovery, result.assessment)
    assert clean_plan.disposition is OperatorRecoveryDisposition.READY_NO_ACTION
    assert not clean_plan.may_dispatch
    assert not clean_plan.commit_allowed
    assert source.calls == [ACCOUNT_ID]
    assert clock.calls == 2
    assert directory_snapshot(tmp_path) == before
    assert set(before) == {path.name}


def test_repeated_one_shot_calls_are_deterministic_and_do_not_reuse_evidence(
    tmp_path: Path,
) -> None:
    path = tmp_path / "repeat.sqlite3"
    create_sealed_v3(path)
    before = directory_snapshot(tmp_path)
    source = AtomicBrokerSource(complete_broker_snapshot())
    clock = CountingClock()

    first = _assess(path, source, clock)
    middle = directory_snapshot(tmp_path)
    second = _assess(path, source, clock)

    assert first == second
    assert source.calls == [ACCOUNT_ID, ACCOUNT_ID]
    assert clock.calls == 4
    assert directory_snapshot(tmp_path) == middle == before


def test_local_clock_before_durable_order_event_fails_before_broker_and_never_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "local-clock-before-durable-event.sqlite3"
    create_sealed_v3(path)
    before = directory_snapshot(tmp_path)
    source = AtomicBrokerSource(complete_broker_snapshot())
    clock = CountingClock((CREATED_AT + timedelta(seconds=1),))
    _forbid_mutating_journal_calls(monkeypatch)

    with pytest.raises(TrustedAssessmentSourceError) as captured:
        _assess(path, source, clock)

    assert captured.value.code is TrustedAssessmentSourceFailureCode.INVALID_REQUEST
    assert source.calls == []
    assert clock.calls == 1
    assert directory_snapshot(tmp_path) == before
    assert set(before) == {path.name}


def test_submission_unknown_flows_to_operator_plan_without_committing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "submission-unknown.sqlite3"
    recovery = create_sealed_v3(path, submission_unknown=True)
    before = directory_snapshot(tmp_path)
    source = AtomicBrokerSource(complete_broker_snapshot())
    clock = CountingClock()
    _forbid_mutating_journal_calls(monkeypatch)

    result = _assess(path, source, clock)
    plan = plan_operator_recovery(recovery, result.assessment)

    assert result.inspection.disposition is LiveJournalInspectionDisposition.RECOVERY_REQUIRED
    assert plan.disposition is OperatorRecoveryDisposition.UNSUPPORTED_REQUIRES_ESCALATION
    assert not result.may_dispatch
    assert not result.commit_allowed
    assert not plan.may_dispatch
    assert not plan.commit_allowed
    assert directory_snapshot(tmp_path) == before


@pytest.mark.parametrize(
    ("scenario", "expected_code"),
    (
        ("v1", TrustedAssessmentSourceFailureCode.SCHEMA_UPGRADE_REQUIRED),
        ("v2", TrustedAssessmentSourceFailureCode.SCHEMA_UPGRADE_REQUIRED),
        ("missing-account", TrustedAssessmentSourceFailureCode.ACCOUNT_NOT_FOUND),
        ("blocked", TrustedAssessmentSourceFailureCode.INTEGRITY_FAILURE),
        ("sidecar", TrustedAssessmentSourceFailureCode.ACTIVE_OR_UNCLEAN_SOURCE),
        ("corrupt", TrustedAssessmentSourceFailureCode.INTEGRITY_FAILURE),
        ("missing", TrustedAssessmentSourceFailureCode.SOURCE_UNAVAILABLE),
        ("capacity", TrustedAssessmentSourceFailureCode.CAPACITY_EXCEEDED),
    ),
)
def test_local_source_failures_precede_broker_query_and_are_zero_write(
    tmp_path: Path,
    scenario: str,
    expected_code: TrustedAssessmentSourceFailureCode,
) -> None:
    path = tmp_path / f"{scenario}.sqlite3"
    account_id = ACCOUNT_ID
    if scenario == "v1":
        create_frozen_v1(path)
    elif scenario == "v2":
        create_frozen_v2(path)
    elif scenario == "blocked":
        create_semantically_blocked_v2(path)
        account_id = "account-a"
    elif scenario == "corrupt":
        path.write_bytes(b"not a sqlite database")
    elif scenario == "missing":
        pass
    else:
        create_sealed_v3(path)
        if scenario == "missing-account":
            account_id = "account-not-present"
        elif scenario == "sidecar":
            Path(f"{path}-wal").write_bytes(b"active-sidecar")
        elif scenario == "capacity":
            with path.open("r+b") as stream:
                stream.truncate(MAX_INSPECTION_MAIN_DATABASE_BYTES + 1)
    before = directory_snapshot(tmp_path)
    source = AtomicBrokerSource(complete_broker_snapshot(), expected_account_id=account_id)

    with pytest.raises(TrustedAssessmentSourceError) as captured:
        assess_sqlite_live_order_journal(
            path,
            account_id=account_id,
            broker_snapshot_source=source,
            clock=CountingClock(),
        )

    assert captured.value.code is expected_code
    assert source.calls == []
    assert directory_snapshot(tmp_path) == before


@pytest.mark.parametrize(
    "kind",
    ("account", "cursor", "stale", "wrong-type", "capacity", "broker-failure"),
)
def test_hostile_broker_sources_fail_closed_without_writing(
    tmp_path: Path,
    kind: str,
) -> None:
    path = tmp_path / f"broker-{kind}.sqlite3"
    create_sealed_v3(path)
    snapshot = complete_broker_snapshot()
    expected = TrustedAssessmentSourceFailureCode.MALFORMED_EVIDENCE
    failure: BaseException | None = None
    source_value: object = snapshot
    if kind == "account":
        source_value = forged_snapshot(snapshot, account_id="account-other")
        expected = TrustedAssessmentSourceFailureCode.ACCOUNT_SCOPE_MISMATCH
    elif kind == "cursor":
        bad_evidence = object.__new__(type(snapshot.open_orders.evidence))
        for name, value in (
            ("query_kind", snapshot.open_orders.evidence.query_kind),
            ("account_id", ACCOUNT_ID),
            ("status", snapshot.open_orders.evidence.status),
            ("observed_at", snapshot.open_orders.evidence.observed_at),
            ("source_cursor", "mixed-cursor"),
            ("reason", None),
        ):
            object.__setattr__(bad_evidence, name, value)
        bad_open = object.__new__(OpenOrdersSnapshot)
        object.__setattr__(bad_open, "orders", snapshot.open_orders.orders)
        object.__setattr__(bad_open, "evidence", bad_evidence)
        source_value = forged_snapshot(snapshot, open_orders=bad_open)
    elif kind == "stale":
        source_value = complete_broker_snapshot(evidence_at=ACCEPTED_AT + timedelta(seconds=1))
    elif kind == "wrong-type":
        source_value = object()
    elif kind == "capacity":
        repeated = snapshot.open_orders.orders * (MAX_TRUSTED_BROKER_OBSERVATIONS + 1)
        source_value = forged_snapshot(
            snapshot,
            open_orders=OpenOrdersSnapshot(repeated, snapshot.open_orders.evidence),
        )
        expected = TrustedAssessmentSourceFailureCode.CAPACITY_EXCEEDED
    else:
        failure = RuntimeError("fake broker secret")
        expected = TrustedAssessmentSourceFailureCode.BROKER_SOURCE_FAILURE
    before = directory_snapshot(tmp_path)
    source = AtomicBrokerSource(source_value, failure=failure)

    with pytest.raises(TrustedAssessmentSourceError) as captured:
        _assess(path, source, CountingClock())

    assert captured.value.code is expected
    assert source.calls == [ACCOUNT_ID]
    assert directory_snapshot(tmp_path) == before
    assert "fake broker secret" not in str(captured.value)


@pytest.mark.parametrize(
    ("status", "correlation", "expected_status"),
    (
        (EvidenceCompleteness.INCOMPLETE, CorrelationStatus.CONFIRMED, "incomplete"),
        (EvidenceCompleteness.COMPLETE, CorrelationStatus.CANDIDATE, "ambiguous"),
    ),
)
def test_non_authoritative_evidence_remains_fail_closed(
    tmp_path: Path,
    status: EvidenceCompleteness,
    correlation: CorrelationStatus,
    expected_status: str,
) -> None:
    path = tmp_path / f"{expected_status}.sqlite3"
    create_sealed_v3(path)
    before = directory_snapshot(tmp_path)
    source = AtomicBrokerSource(
        complete_broker_snapshot(evidence_status=status, correlation_status=correlation)
    )

    result = _assess(path, source, CountingClock())

    assert result.assessment.result.status.value == expected_status
    assert not result.assessment.result.is_authoritative
    assert not result.assessment.may_resume
    assert not result.may_dispatch
    assert not result.commit_allowed
    assert directory_snapshot(tmp_path) == before


def test_trusted_path_does_not_consult_runtime_integrations_or_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "poison.sqlite3"
    create_sealed_v3(path)
    source = AtomicBrokerSource(complete_broker_snapshot())
    clock = CountingClock()
    _forbid_mutating_journal_calls(monkeypatch)

    forbidden = ("pythoncom", "comtypes", "win32com", "dotenv", "keyring", "config")
    real_import = __import__

    def guarded_import(name: str, *args: object, **kwargs: object):
        if any(name == item or name.startswith(f"{item}.") for item in forbidden):
            raise AssertionError("forbidden runtime integration import")
        return real_import(name, *args, **kwargs)

    before = directory_snapshot(tmp_path)
    with monkeypatch.context() as poison:
        poison.setattr("builtins.__import__", guarded_import)
        poison.setattr(
            os,
            "getenv",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("environment read attempted")
            ),
        )
        poison.setattr(
            sys,
            "stdin",
            type(
                "PoisonStdin",
                (),
                {
                    "read": lambda *_args, **_kwargs: (_ for _ in ()).throw(
                        AssertionError("stdin evidence read attempted")
                    )
                },
            )(),
        )
        result = _assess(path, source, clock)

    assert result.assessment.result.is_authoritative
    assert directory_snapshot(tmp_path) == before


def test_schema_v1_is_not_migrated(tmp_path: Path) -> None:
    path = tmp_path / "frozen-v1.sqlite3"
    create_frozen_v1(path)
    with sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True) as connection:
        version_before = int(connection.execute("PRAGMA user_version").fetchone()[0])
        migrations_before = tuple(connection.execute("SELECT * FROM live_journal_migrations"))
    before = directory_snapshot(tmp_path)

    with pytest.raises(TrustedAssessmentSourceError) as captured:
        _assess(path, AtomicBrokerSource(complete_broker_snapshot()), CountingClock())

    assert captured.value.code is TrustedAssessmentSourceFailureCode.SCHEMA_UPGRADE_REQUIRED
    with sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True) as connection:
        assert int(connection.execute("PRAGMA user_version").fetchone()[0]) == version_before
        assert (
            tuple(connection.execute("SELECT * FROM live_journal_migrations")) == migrations_before
        )
    assert directory_snapshot(tmp_path) == before
