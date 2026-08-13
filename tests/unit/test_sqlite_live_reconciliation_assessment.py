from __future__ import annotations

import ast
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from tx_trade.orders.live_journal_inspection_contracts import (
    LiveJournalInspectionDisposition as Disposition,
    LiveJournalInspectionError,
    LiveJournalInspectionFailureCode,
    LiveJournalInspectionIssueCode,
    LiveJournalInspectionReport,
)
from tx_trade.orders.live_contracts import (
    BrokerCorrelation,
    BrokerFillObservation,
    BrokerOpenOrderObservation,
    BrokerPosition,
    CorrelationStatus,
    LiveSide,
)
from tx_trade.orders.live_ports import (
    BrokerFillsSnapshot,
    BrokerPositionsSnapshot,
    CompletenessEvidence,
    EvidenceCompleteness,
    EvidenceQueryKind,
    OpenOrdersSnapshot,
)
from tx_trade.orders.live_reconciliation_assessment_contracts import (
    MAX_TRUSTED_BROKER_OBSERVATIONS,
    TrustedAssessmentSourceError,
    TrustedAssessmentSourceFailureCode as FailureCode,
)
from tx_trade.orders.live_reconciliation_contracts import (
    BrokerReconciliationSnapshot,
    LocalReconciliationSnapshot,
)
from tx_trade.orders import sqlite_live_reconciliation_assessment as subject

LOCAL_TIME = datetime(2026, 8, 13, 1, tzinfo=timezone.utc)
BROKER_TIME = LOCAL_TIME + timedelta(seconds=1)
RECONCILED_TIME = BROKER_TIME + timedelta(seconds=1)
DIGEST = f"sha256:{'0' * 64}"


def inspection(
    disposition: Disposition = Disposition.READY_NO_ACTION,
    *,
    schema_version: int | None = None,
) -> LiveJournalInspectionReport:
    issues = {
        Disposition.READY_NO_ACTION: (),
        Disposition.RECOVERY_REQUIRED: (LiveJournalInspectionIssueCode.OUTSTANDING_DISPATCH,),
        Disposition.SCHEMA_UPGRADE_REQUIRED: (
            LiveJournalInspectionIssueCode.SCHEMA_UPGRADE_REQUIRED,
        ),
        Disposition.ACCOUNT_NOT_FOUND: (LiveJournalInspectionIssueCode.ACCOUNT_NOT_FOUND,),
        Disposition.BLOCKED_INTEGRITY_FAILURE: (LiveJournalInspectionIssueCode.INTEGRITY_FAILURE,),
    }[disposition]
    return LiveJournalInspectionReport(
        "account-1",
        schema_version
        if schema_version is not None
        else (1 if disposition is Disposition.SCHEMA_UPGRADE_REQUIRED else 3),
        7,
        disposition,
        issues,
        (),
        DIGEST,
    )


def local() -> LocalReconciliationSnapshot:
    return LocalReconciliationSnapshot("account-1", (), (), (), LOCAL_TIME, journal_sequence=7)


def evidence(kind: EvidenceQueryKind) -> CompletenessEvidence:
    return CompletenessEvidence(
        kind,
        "account-1",
        EvidenceCompleteness.COMPLETE,
        BROKER_TIME,
        "snapshot-1",
    )


def broker() -> BrokerReconciliationSnapshot:
    return BrokerReconciliationSnapshot(
        "snapshot-1",
        "account-1",
        OpenOrdersSnapshot((), evidence(EvidenceQueryKind.OPEN_ORDERS)),
        BrokerFillsSnapshot((), evidence(EvidenceQueryKind.FILLS)),
        BrokerPositionsSnapshot((), evidence(EvidenceQueryKind.POSITIONS)),
        BROKER_TIME,
    )


def correlation(*, fill: bool = False) -> BrokerCorrelation:
    return BrokerCorrelation(
        1,
        1,
        CorrelationStatus.CONFIRMED,
        BROKER_TIME,
        broker_order_sequence=None if fill else "sequence-1",
        broker_fill_id="fill-1" if fill else None,
        client_order_id="order-1",
    )


def broker_with_observation(kind: str) -> BrokerReconciliationSnapshot:
    open_orders: tuple[BrokerOpenOrderObservation, ...] = ()
    fills: tuple[BrokerFillObservation, ...] = ()
    positions: tuple[BrokerPosition, ...] = ()
    if kind in {"open", "correlation"}:
        open_orders = (
            BrokerOpenOrderObservation(
                "open-1",
                "account-1",
                "TXF",
                LiveSide.BUY,
                Decimal(1),
                Decimal(1),
                Decimal("22000"),
                correlation(),
                BROKER_TIME,
            ),
        )
    elif kind == "fill":
        fills = (
            BrokerFillObservation(
                "fill-observation-1",
                "account-1",
                "TXF",
                LiveSide.BUY,
                Decimal(1),
                Decimal("22000"),
                correlation(fill=True),
                BROKER_TIME,
                BROKER_TIME,
            ),
        )
    elif kind == "position":
        positions = (
            BrokerPosition(
                "account-1",
                "TXF",
                Decimal(1),
                Decimal("22000"),
                BROKER_TIME,
            ),
        )
    return BrokerReconciliationSnapshot(
        "snapshot-1",
        "account-1",
        OpenOrdersSnapshot(open_orders, evidence(EvidenceQueryKind.OPEN_ORDERS)),
        BrokerFillsSnapshot(fills, evidence(EvidenceQueryKind.FILLS)),
        BrokerPositionsSnapshot(positions, evidence(EvidenceQueryKind.POSITIONS)),
        BROKER_TIME,
    )


class Clock:
    def __init__(self, values: list[object], calls: list[str] | None = None) -> None:
        self.values = values
        self.calls = calls

    def now(self) -> datetime:
        if self.calls is not None:
            self.calls.append("clock")
        value = self.values.pop(0)
        if isinstance(value, BaseException):
            raise value
        return value  # type: ignore[return-value]


class Source:
    def __init__(self, value: object, calls: list[str] | None = None) -> None:
        self.value = value
        self.calls = calls
        self.count = 0

    def query_reconciliation_snapshot(self, account_id: str) -> BrokerReconciliationSnapshot:
        assert account_id == "account-1"
        self.count += 1
        if self.calls is not None:
            self.calls.append("broker")
        if isinstance(self.value, BaseException):
            raise self.value
        return self.value  # type: ignore[return-value]


def invoke(monkeypatch: pytest.MonkeyPatch, source: Source | None = None) -> object:
    monkeypatch.setattr(
        subject,
        "_inspect_sqlite_live_order_journal_with_account_snapshot",
        lambda path, *, account_id, as_of: (inspection(), local()),
    )
    return subject.assess_sqlite_live_order_journal(
        "journal.sqlite3",
        account_id="account-1",
        broker_snapshot_source=source or Source(broker()),
        clock=Clock([LOCAL_TIME, RECONCILED_TIME]),
    )


def assert_code(expected: FailureCode, call: object) -> None:
    with pytest.raises(TrustedAssessmentSourceError) as caught:
        call()  # type: ignore[operator]
    assert caught.value.code is expected
    assert "secret" not in str(caught.value)


def test_success_is_one_shot_ordered_deterministic_and_never_authorizes_side_effects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def inspect_once(path: str | Path, *, account_id: str, as_of: datetime) -> object:
        calls.append("inspect")
        assert (path, account_id, as_of) == ("journal.sqlite3", "account-1", LOCAL_TIME)
        return inspection(), local()

    monkeypatch.setattr(
        subject, "_inspect_sqlite_live_order_journal_with_account_snapshot", inspect_once
    )
    source = Source(broker(), calls)
    result = subject.assess_sqlite_live_order_journal(
        "journal.sqlite3",
        account_id="account-1",
        broker_snapshot_source=source,
        clock=Clock([LOCAL_TIME, RECONCILED_TIME], calls),
    )
    assert calls == ["clock", "inspect", "broker", "clock"]
    assert source.count == 1
    assert result == invoke(monkeypatch)
    assert result.inspection == inspection()
    assert not result.may_dispatch
    assert not result.commit_allowed
    assert not result.assessment.may_dispatch


@pytest.mark.parametrize(
    ("disposition", "code"),
    [
        (Disposition.SCHEMA_UPGRADE_REQUIRED, FailureCode.SCHEMA_UPGRADE_REQUIRED),
        (Disposition.ACCOUNT_NOT_FOUND, FailureCode.ACCOUNT_NOT_FOUND),
        (Disposition.BLOCKED_INTEGRITY_FAILURE, FailureCode.INTEGRITY_FAILURE),
    ],
)
def test_terminal_inspection_dispositions_never_query_broker(
    monkeypatch: pytest.MonkeyPatch, disposition: Disposition, code: FailureCode
) -> None:
    monkeypatch.setattr(
        subject,
        "_inspect_sqlite_live_order_journal_with_account_snapshot",
        lambda *args, **kwargs: (inspection(disposition), None),
    )
    source = Source(broker())
    assert_code(
        code,
        lambda: subject.assess_sqlite_live_order_journal(
            "journal.sqlite3",
            account_id="account-1",
            broker_snapshot_source=source,
            clock=Clock([LOCAL_TIME]),
        ),
    )
    assert source.count == 0


def test_schema_v2_upgrade_never_queries_broker(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        subject,
        "_inspect_sqlite_live_order_journal_with_account_snapshot",
        lambda *args, **kwargs: (
            inspection(Disposition.SCHEMA_UPGRADE_REQUIRED, schema_version=2),
            None,
        ),
    )
    source = Source(broker())
    assert_code(
        FailureCode.SCHEMA_UPGRADE_REQUIRED,
        lambda: subject.assess_sqlite_live_order_journal(
            "journal.sqlite3",
            account_id="account-1",
            broker_snapshot_source=source,
            clock=Clock([LOCAL_TIME]),
        ),
    )
    assert source.count == 0


def test_recovery_disposition_may_assess(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        subject,
        "_inspect_sqlite_live_order_journal_with_account_snapshot",
        lambda *args, **kwargs: (inspection(Disposition.RECOVERY_REQUIRED), local()),
    )
    result = subject.assess_sqlite_live_order_journal(
        "journal.sqlite3",
        account_id="account-1",
        broker_snapshot_source=Source(broker()),
        clock=Clock([LOCAL_TIME, RECONCILED_TIME]),
    )
    assert result.inspection.disposition is Disposition.RECOVERY_REQUIRED


@pytest.mark.parametrize("inspection_code", list(LiveJournalInspectionFailureCode))
def test_inspection_errors_are_mapped_and_broker_is_not_called(
    monkeypatch: pytest.MonkeyPatch, inspection_code: LiveJournalInspectionFailureCode
) -> None:
    def fail(*args: object, **kwargs: object) -> object:
        raise LiveJournalInspectionError(inspection_code)

    monkeypatch.setattr(subject, "_inspect_sqlite_live_order_journal_with_account_snapshot", fail)
    source = Source(broker())
    expected = FailureCode(inspection_code.value)
    assert_code(
        expected,
        lambda: subject.assess_sqlite_live_order_journal(
            "journal.sqlite3",
            account_id="account-1",
            broker_snapshot_source=source,
            clock=Clock([LOCAL_TIME]),
        ),
    )
    assert source.count == 0


@pytest.mark.parametrize("bad", [object(), None, datetime(2026, 1, 1)])
def test_invalid_clock_values_are_internal_failure(
    monkeypatch: pytest.MonkeyPatch, bad: object
) -> None:
    source = Source(broker())
    monkeypatch.setattr(
        subject,
        "_inspect_sqlite_live_order_journal_with_account_snapshot",
        lambda *args, **kwargs: (inspection(), local()),
    )
    assert_code(
        FailureCode.INTERNAL_FAILURE,
        lambda: subject.assess_sqlite_live_order_journal(
            "journal.sqlite3",
            account_id="account-1",
            broker_snapshot_source=source,
            clock=Clock([bad]),
        ),
    )
    assert source.count == 0


@pytest.mark.parametrize(
    ("failure", "code"),
    [
        (RuntimeError("secret"), FailureCode.INTERNAL_FAILURE),
        (KeyboardInterrupt("secret"), FailureCode.INTERNAL_FAILURE),
        (MemoryError("secret"), FailureCode.CAPACITY_EXCEEDED),
    ],
)
def test_second_clock_failure_occurs_after_exactly_one_broker_query(
    monkeypatch: pytest.MonkeyPatch, failure: BaseException, code: FailureCode
) -> None:
    monkeypatch.setattr(
        subject,
        "_inspect_sqlite_live_order_journal_with_account_snapshot",
        lambda *args, **kwargs: (inspection(), local()),
    )
    source = Source(broker())
    assert_code(
        code,
        lambda: subject.assess_sqlite_live_order_journal(
            "journal.sqlite3",
            account_id="account-1",
            broker_snapshot_source=source,
            clock=Clock([LOCAL_TIME, failure]),
        ),
    )
    assert source.count == 1


@pytest.mark.parametrize(
    ("failure", "code"),
    [
        (ValueError("secret"), FailureCode.BROKER_SOURCE_FAILURE),
        (TypeError("secret"), FailureCode.BROKER_SOURCE_FAILURE),
        (RuntimeError("secret"), FailureCode.BROKER_SOURCE_FAILURE),
        (KeyboardInterrupt("secret"), FailureCode.BROKER_SOURCE_FAILURE),
        (SystemExit("secret"), FailureCode.BROKER_SOURCE_FAILURE),
        (MemoryError("secret"), FailureCode.CAPACITY_EXCEEDED),
    ],
)
def test_broker_failures_are_sanitized(
    monkeypatch: pytest.MonkeyPatch, failure: BaseException, code: FailureCode
) -> None:
    assert_code(code, lambda: invoke(monkeypatch, Source(failure)))


def test_wrong_broker_type_and_invalid_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    assert_code(FailureCode.MALFORMED_EVIDENCE, lambda: invoke(monkeypatch, Source(object())))
    assert_code(
        FailureCode.INVALID_REQUEST,
        lambda: subject.assess_sqlite_live_order_journal(
            "journal.sqlite3",
            account_id="account-1",
            broker_snapshot_source=object(),  # type: ignore[arg-type]
            clock=Clock([LOCAL_TIME]),
        ),
    )


def test_account_mismatch_is_distinct(monkeypatch: pytest.MonkeyPatch) -> None:
    wrong = broker()
    object.__setattr__(wrong, "account_id", "account-2")
    assert_code(FailureCode.ACCOUNT_SCOPE_MISMATCH, lambda: invoke(monkeypatch, Source(wrong)))


@pytest.mark.parametrize("failure_type", [RuntimeError, KeyboardInterrupt, SystemExit])
def test_hostile_forged_account_never_executes_magic(
    monkeypatch: pytest.MonkeyPatch, failure_type: type[BaseException]
) -> None:
    class HostileAccount:
        def __ne__(self, other: object) -> bool:
            raise failure_type("secret-ne")

        def __str__(self) -> str:
            raise failure_type("secret-str")

    forged = broker()
    object.__setattr__(forged, "account_id", HostileAccount())
    assert_code(FailureCode.MALFORMED_EVIDENCE, lambda: invoke(monkeypatch, Source(forged)))


def test_forged_snapshot_missing_account_is_sanitized_before_second_clock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    forged = broker()
    object.__delattr__(forged, "account_id")
    monkeypatch.setattr(
        subject,
        "_inspect_sqlite_live_order_journal_with_account_snapshot",
        lambda *args, **kwargs: (inspection(), local()),
    )
    assessment_calls = 0

    def reject_assessment(*args: object) -> object:
        nonlocal assessment_calls
        assessment_calls += 1
        raise AssertionError("assessment must not run for malformed evidence")

    monkeypatch.setattr(subject, "assess_reconciliation", reject_assessment)
    calls: list[str] = []
    clock = Clock([LOCAL_TIME, RECONCILED_TIME], calls)
    source = Source(forged, calls)
    assert_code(
        FailureCode.MALFORMED_EVIDENCE,
        lambda: subject.assess_sqlite_live_order_journal(
            "journal.sqlite3",
            account_id="account-1",
            broker_snapshot_source=source,
            clock=clock,
        ),
    )
    assert source.count == 1
    assert calls == ["clock", "broker"]
    assert clock.values == [RECONCILED_TIME]
    assert assessment_calls == 0


def test_cap_allows_exact_limit_and_rejects_plus_one(monkeypatch: pytest.MonkeyPatch) -> None:
    base = broker()
    marker = BrokerPosition("account-1", "TXF", Decimal(0), None, BROKER_TIME)

    def broker_with_positions(count: int) -> BrokerReconciliationSnapshot:
        return replace(
            base,
            positions=BrokerPositionsSnapshot(
                (marker,) * count,
                evidence(EvidenceQueryKind.POSITIONS),
            ),
        )

    exact = broker_with_positions(MAX_TRUSTED_BROKER_OBSERVATIONS)
    canonical = subject.assess_reconciliation(local(), base, RECONCILED_TIME)
    monkeypatch.setattr(subject, "assess_reconciliation", lambda *args: canonical)
    assert invoke(monkeypatch, Source(exact)).assessment is canonical

    plus_one = broker_with_positions(MAX_TRUSTED_BROKER_OBSERVATIONS + 1)
    reconstruction_calls = 0

    def reject_reconstruction(*args: object, **kwargs: object) -> object:
        nonlocal reconstruction_calls
        reconstruction_calls += 1
        raise AssertionError("observation reconstruction must not start")

    monkeypatch.setattr(subject, "replace", reject_reconstruction)
    assert_code(FailureCode.CAPACITY_EXCEEDED, lambda: invoke(monkeypatch, Source(plus_one)))
    assert reconstruction_calls == 0


@pytest.mark.parametrize("tampering", ["cursor", "account", "nested_type"])
def test_forged_nested_broker_contract_is_revalidated(
    monkeypatch: pytest.MonkeyPatch, tampering: str
) -> None:
    value = broker()
    if tampering == "cursor":
        object.__setattr__(value.open_orders.evidence, "source_cursor", "other-cut")
    elif tampering == "account":
        object.__setattr__(value.open_orders.evidence, "account_id", "account-2")
    else:
        object.__setattr__(value.open_orders, "orders", (object(),))
    assert_code(FailureCode.MALFORMED_EVIDENCE, lambda: invoke(monkeypatch, Source(value)))


@pytest.mark.parametrize("kind", ["open", "fill", "position", "correlation"])
def test_forged_exact_observation_is_canonically_revalidated_before_second_clock(
    monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    value = broker_with_observation(kind)
    if kind == "open":
        object.__setattr__(value.open_orders.orders[0], "observed_at", "secret-open-time")
    elif kind == "fill":
        object.__setattr__(value.fills.fills[0], "quantity", "secret-fill-quantity")
    elif kind == "position":
        object.__setattr__(
            value.positions.positions[0],
            "average_open_price",
            "secret-position-price",
        )
    else:
        object.__setattr__(
            value.open_orders.orders[0].correlation,
            "client_order_id",
            "secret-correlation!",
        )

    monkeypatch.setattr(
        subject,
        "_inspect_sqlite_live_order_journal_with_account_snapshot",
        lambda *args, **kwargs: (inspection(), local()),
    )
    assessment_calls = 0

    def reject_assessment(*args: object) -> object:
        nonlocal assessment_calls
        assessment_calls += 1
        raise AssertionError("assessment must not run for malformed evidence")

    monkeypatch.setattr(subject, "assess_reconciliation", reject_assessment)
    calls: list[str] = []
    clock = Clock([LOCAL_TIME, RECONCILED_TIME], calls)
    source = Source(value, calls)
    assert_code(
        FailureCode.MALFORMED_EVIDENCE,
        lambda: subject.assess_sqlite_live_order_journal(
            "journal.sqlite3",
            account_id="account-1",
            broker_snapshot_source=source,
            clock=clock,
        ),
    )
    assert source.count == 1
    assert calls == ["clock", "broker"]
    assert clock.values == [RECONCILED_TIME]
    assert assessment_calls == 0


@pytest.mark.parametrize("failure", [ValueError("stale"), TypeError("malformed")])
def test_assessment_contract_failures_are_malformed(
    monkeypatch: pytest.MonkeyPatch, failure: Exception
) -> None:
    monkeypatch.setattr(
        subject, "assess_reconciliation", lambda *args: (_ for _ in ()).throw(failure)
    )
    assert_code(FailureCode.MALFORMED_EVIDENCE, lambda: invoke(monkeypatch))


def test_module_has_tiny_public_surface_and_no_forbidden_capabilities() -> None:
    assert subject.__all__ == ["assess_sqlite_live_order_journal"]
    tree = ast.parse(Path(subject.__file__).read_text(encoding="utf-8"))
    imports = {
        alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    assert imports.isdisjoint(
        {"argparse", "json", "os", "sqlite3", "subprocess", "socket", "requests", "win32com"}
    )
