from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from tx_trade.orders import live_journal_inspection as inspection
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
    LiveJournalIdentity,
    LiveJournalRecoverySnapshot,
    OutstandingDispatchClaim,
)
from tx_trade.orders.live_journal_codec import LiveJournalCodecError, encode_journal_value
from tx_trade.orders.live_journal_inspection import (
    build_live_journal_inspection_report,
    build_schema_upgrade_required_inspection_report,
)
from tx_trade.orders.live_journal_inspection_contracts import (
    LiveJournalInspectionDisposition,
    LiveJournalInspectionIssueCode,
    LiveJournalInspectionTargetKind,
)
from tx_trade.orders.live_ports import RawBrokerObservation
from tx_trade.orders.live_state_machine import AppliedEventLedger, create_live_order

NOW = datetime(2026, 8, 9, tzinfo=timezone.utc)
SHA = "sha256:" + "a" * 64


def _identity(version: int = 2) -> LiveJournalIdentity:
    return LiveJournalIdentity("journal-1", version, SHA, NOW)


def _intent(account_id: str, order_id: str) -> LiveOrderIntent:
    return LiveOrderIntent(
        strategy_id="strategy-1",
        client_order_id=order_id,
        account_id=account_id,
        instrument_id="TXF",
        side=LiveSide.BUY,
        quantity=Decimal("1"),
        order_type=LiveOrderType.LIMIT,
        limit_price=Decimal("22000"),
        time_in_force=LiveTimeInForce.DAY,
        day_trade=False,
        created_at=NOW,
    )


def _order(account_id: str, order_id: str, command_id: str | None = None):
    intent = _intent(account_id, order_id)
    if command_id is None:
        return create_live_order(intent)
    command = NewOrderCommand(command_id, intent, NOW)
    binding = PendingCommandBinding(
        command,
        payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1),
    )
    return replace(
        create_live_order(intent),
        state=LiveOrderState.SUBMITTING,
        version=2,
        pending_command=binding,
    )


def _snapshot(
    *,
    orders=(),
    claims=(),
    unresolved=(),
    identity: LiveJournalIdentity | None = None,
):
    return LiveJournalRecoverySnapshot(
        identity or _identity(),
        tuple(orders),
        tuple(claims),
        tuple(unresolved),
        (),
        (),
        (),
        AppliedEventLedger(),
        11,
    )


def _claim(order, token: str = "secret-token") -> OutstandingDispatchClaim:
    assert order.pending_command is not None
    return OutstandingDispatchClaim(
        order.pending_command.command,
        token,
        "private-claimant",
        order.version,
        NOW,
    )


def test_ready_and_account_not_found_are_account_scoped() -> None:
    snapshot = _snapshot(orders=(_order("account-b", "order-b"),))

    found = build_live_journal_inspection_report(snapshot, "account-b", database_schema_version=2)
    missing = build_live_journal_inspection_report(snapshot, "account-a", database_schema_version=2)

    assert found.disposition is LiveJournalInspectionDisposition.READY_NO_ACTION
    assert found.issue_codes == ()
    assert missing.disposition is LiveJournalInspectionDisposition.ACCOUNT_NOT_FOUND
    assert missing.issue_codes == (LiveJournalInspectionIssueCode.ACCOUNT_NOT_FOUND,)


def test_pending_and_claim_targets_are_redacted_and_canonical() -> None:
    pending = _order("account-a", "durable-order", "durable-command")
    snapshot = _snapshot(orders=(pending,), claims=(_claim(pending),))

    report = build_live_journal_inspection_report(snapshot, "account-a", database_schema_version=2)

    assert report.disposition is LiveJournalInspectionDisposition.RECOVERY_REQUIRED
    assert report.issue_codes == (
        LiveJournalInspectionIssueCode.OUTSTANDING_DISPATCH,
        LiveJournalInspectionIssueCode.PENDING_BROKER_EVIDENCE,
    )
    assert tuple(item.kind for item in report.targets) == (
        LiveJournalInspectionTargetKind.CLAIM,
        LiveJournalInspectionTargetKind.PENDING_COMMAND,
    )
    representation = repr(report)
    for secret in (
        "account-a",
        "durable-order",
        "durable-command",
        "secret-token",
        "private-claimant",
    ):
        assert secret not in representation
    assert not report.may_dispatch
    assert not report.commit_allowed


def test_permutations_and_claim_secrets_do_not_change_report() -> None:
    first = _order("account-a", "order-1", "command-1")
    second = _order("account-a", "order-2", "command-2")
    claims = (_claim(first, "token-1"), _claim(second, "token-2"))

    report = build_live_journal_inspection_report(
        _snapshot(orders=(first, second), claims=claims),
        "account-a",
        database_schema_version=2,
    )
    permuted = build_live_journal_inspection_report(
        _snapshot(
            orders=(second, first),
            claims=(_claim(second, "changed-2"), _claim(first, "changed-1")),
        ),
        "account-a",
        database_schema_version=2,
    )

    assert report == permuted


def test_other_account_valid_work_does_not_affect_selected_report() -> None:
    selected = _order("account-a", "order-a")
    foreign = _order("account-b", "order-b", "command-b")

    baseline = build_live_journal_inspection_report(
        _snapshot(orders=(selected,)), "account-a", database_schema_version=2
    )
    with_foreign_work = build_live_journal_inspection_report(
        _snapshot(orders=(selected, foreign), claims=(_claim(foreign),)),
        "account-a",
        database_schema_version=2,
    )

    assert baseline == with_foreign_work


@pytest.mark.parametrize(
    "scoped_issue",
    (
        LiveJournalInspectionIssueCode.UNRESOLVED_OBSERVATION,
        LiveJournalInspectionIssueCode.CONFLICT_OBSERVATION,
        LiveJournalInspectionIssueCode.AMBIGUOUS_OBSERVATION,
        LiveJournalInspectionIssueCode.DURABLE_RECONCILIATION_REQUIREMENT,
        LiveJournalInspectionIssueCode.GLOBAL_RECOVERY_BLOCKER,
    ),
)
def test_scoped_recovery_issue_overrides_missing_account(
    scoped_issue: LiveJournalInspectionIssueCode,
) -> None:
    snapshot = _snapshot(orders=(_order("account-b", "order-b"),))
    report = build_live_journal_inspection_report(
        snapshot,
        "account-a",
        database_schema_version=2,
        scoped_issue_codes=(scoped_issue,),
    )

    assert report.disposition is LiveJournalInspectionDisposition.RECOVERY_REQUIRED
    assert report.issue_codes == (scoped_issue,)
    assert report.targets == ()
    assert report.requires_reconciliation
    assert not report.commit_allowed
    assert not report.may_dispatch


def test_scoped_status_code_is_rejected() -> None:
    snapshot = _snapshot(orders=(_order("account-b", "order-b"),))

    with pytest.raises(ValueError, match="status issue codes"):
        build_live_journal_inspection_report(
            snapshot,
            "account-a",
            database_schema_version=2,
            scoped_issue_codes=(LiveJournalInspectionIssueCode.INTEGRITY_FAILURE,),
        )


def test_blocked_verification_has_only_generic_integrity_failure() -> None:
    order = _order("account-a", "order-a")
    object.__setattr__(order, "version", 0)
    report = build_live_journal_inspection_report(
        _snapshot(orders=(order,)), "account-a", database_schema_version=2
    )

    assert report.disposition is LiveJournalInspectionDisposition.BLOCKED_INTEGRITY_FAILURE
    assert report.issue_codes == (LiveJournalInspectionIssueCode.INTEGRITY_FAILURE,)
    assert report.targets == ()


def test_large_valid_aggregate_snapshot_builds_recovery_report() -> None:
    secret = b"large-aggregate-payload-secret" + b"x" * 20_000
    observations = tuple(
        RawBrokerObservation(
            f"observation-{index}",
            "reply",
            1,
            index + 1,
            NOW,
            secret,
        )
        for index in range(64)
    )
    snapshot = _snapshot(
        orders=(_order("account-a", "order-a"),),
        unresolved=observations,
    )
    with pytest.raises(LiveJournalCodecError):
        encode_journal_value(snapshot)

    report = build_live_journal_inspection_report(
        snapshot,
        "account-a",
        database_schema_version=2,
        scoped_issue_codes=(LiveJournalInspectionIssueCode.UNRESOLVED_OBSERVATION,),
    )

    assert report.disposition is LiveJournalInspectionDisposition.RECOVERY_REQUIRED
    assert report.issue_codes == (LiveJournalInspectionIssueCode.UNRESOLVED_OBSERVATION,)
    assert report.targets == ()
    assert secret.decode("ascii") not in repr(report)


def test_target_change_changes_opaque_target_and_digest() -> None:
    first = _order("account-a", "order-a", "command-a")
    second = _order("account-a", "order-a", "command-b")

    first_report = build_live_journal_inspection_report(
        _snapshot(orders=(first,)), "account-a", database_schema_version=2
    )
    second_report = build_live_journal_inspection_report(
        _snapshot(orders=(second,)), "account-a", database_schema_version=2
    )

    assert first_report.targets[0].target_id != second_report.targets[0].target_id
    assert first_report.inspection_digest != second_report.inspection_digest


def test_target_limit_collapses_to_only_limit_issue(monkeypatch: pytest.MonkeyPatch) -> None:
    pending = _order("account-a", "order-a", "command-a")
    monkeypatch.setattr(inspection, "MAX_INSPECTION_TARGETS", 1)

    report = build_live_journal_inspection_report(
        _snapshot(orders=(pending,), claims=(_claim(pending),)),
        "account-a",
        database_schema_version=2,
        scoped_issue_codes=(LiveJournalInspectionIssueCode.GLOBAL_RECOVERY_BLOCKER,),
    )

    assert report.disposition is LiveJournalInspectionDisposition.RECOVERY_REQUIRED
    assert report.issue_codes == (LiveJournalInspectionIssueCode.REPORT_LIMIT_EXCEEDED,)
    assert report.targets == ()


def test_v1_builder_returns_exact_upgrade_report() -> None:
    report = build_schema_upgrade_required_inspection_report(
        _identity(1),
        "account-a",
        database_schema_version=1,
        journal_sequence=0,
    )

    assert report.disposition is LiveJournalInspectionDisposition.SCHEMA_UPGRADE_REQUIRED
    assert report.issue_codes == (LiveJournalInspectionIssueCode.SCHEMA_UPGRADE_REQUIRED,)
    assert report.targets == ()


def test_v2_builder_accepts_migrated_journal_with_v1_creation_identity() -> None:
    snapshot = _snapshot(
        identity=_identity(1),
        orders=(_order("account-a", "order-a"),),
    )

    report = build_live_journal_inspection_report(
        snapshot,
        "account-a",
        database_schema_version=2,
    )

    assert report.disposition is LiveJournalInspectionDisposition.READY_NO_ACTION


def test_public_exports_are_exact() -> None:
    from tx_trade.orders import live_journal_inspection as module

    assert module.__all__ == [
        "build_live_journal_inspection_report",
        "build_schema_upgrade_required_inspection_report",
    ]
