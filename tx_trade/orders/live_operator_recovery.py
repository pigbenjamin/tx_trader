"""Pure planning and request construction for offline operator recovery."""

from __future__ import annotations

from hashlib import sha256
import json

from .live_contracts import NewOrderCommand, canonical_bytes
from .live_journal_contracts import LiveJournalRecoverySnapshot, OutstandingDispatchClaim
from .live_journal_recovery import RecoveryReadiness, verify_recovery_snapshot
from .live_operator_recovery_contracts import (
    ExplicitOperatorRecoverySelection,
    OfflineOperatorRecoveryPlan,
    OperatorRecoveryDisposition,
    OperatorRecoveryReason,
    OperatorRecoveryResolution,
    OperatorRecoveryTargetKind,
    RedactedOperatorRecoveryTarget,
)
from .live_reconciliation_commit_contracts import (
    ClaimResolution,
    ClaimResolutionDirective,
    DurableReconciliationCommitRequest,
)
from .live_reconciliation_contracts import ReconciliationAssessment
from .live_reconciliation_projection import project_authoritative_orders
from .live_reconciliation_projection_contracts import (
    AuthoritativeOrderProjectionPlan,
    OrderProjectionDisposition,
    OrderProjectionReason,
)

_INSPECTION_DOMAIN = b"tx_trade.live.operator-recovery.inspection.v1\x00"
_TARGET_DOMAIN = b"tx_trade.live.operator-recovery.target.v1\x00"
_SUPPORTED_ISSUES = frozenset({"outstanding_dispatch", "pending_broker_evidence"})


def _account_id(
    snapshot: LiveJournalRecoverySnapshot,
    assessment: ReconciliationAssessment | None,
) -> str:
    if assessment is not None:
        return assessment.local_snapshot.account_id
    accounts = {order.intent.account_id for order in snapshot.orders}
    if len(accounts) == 1:
        return next(iter(accounts))
    return "unattributed-account" if not accounts else "multiple-accounts"


def _target_id(account_id: str, journal_sequence: int, client_command_id: str) -> str:
    material = f"{account_id}\x00{journal_sequence}\x00{client_command_id}".encode("ascii")
    return f"claim-{sha256(_TARGET_DOMAIN + material).hexdigest()}"


def _projection_material(plan: AuthoritativeOrderProjectionPlan | None) -> str | None:
    if plan is None:
        return None
    # Bind the complete projection contract, including every nested LiveOrder field.
    # The canonical representation is used only as hash input and is never exposed.
    return canonical_bytes(plan).decode("utf-8")


def _inspection_digest(
    *,
    account_id: str,
    journal_sequence: int,
    disposition: OperatorRecoveryDisposition,
    issue_codes: tuple[str, ...],
    reasons: tuple[OperatorRecoveryReason, ...],
    targets: tuple[RedactedOperatorRecoveryTarget, ...],
    projection_plan: AuthoritativeOrderProjectionPlan | None,
) -> str:
    document = {
        "account_id": account_id,
        "journal_sequence": journal_sequence,
        "disposition": disposition.value,
        "issue_codes": list(issue_codes),
        "reasons": [item.value for item in reasons],
        "targets": [
            [
                item.kind.value,
                item.target_id,
                [value.value for value in item.allowed_resolutions],
            ]
            for item in targets
        ],
        "projection": _projection_material(projection_plan),
    }
    payload = json.dumps(
        document,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    return f"sha256:{sha256(_INSPECTION_DOMAIN + payload).hexdigest()}"


def _plan(
    *,
    account_id: str,
    journal_sequence: int,
    disposition: OperatorRecoveryDisposition,
    issue_codes: tuple[str, ...] = (),
    reasons: tuple[OperatorRecoveryReason, ...] = (),
    targets: tuple[RedactedOperatorRecoveryTarget, ...] = (),
    projection_plan: AuthoritativeOrderProjectionPlan | None = None,
) -> OfflineOperatorRecoveryPlan:
    digest = _inspection_digest(
        account_id=account_id,
        journal_sequence=journal_sequence,
        disposition=disposition,
        issue_codes=issue_codes,
        reasons=reasons,
        targets=targets,
        projection_plan=projection_plan,
    )
    return OfflineOperatorRecoveryPlan(
        account_id,
        journal_sequence,
        digest,
        disposition,
        issue_codes,
        reasons,
        targets,
        projection_plan,
    )


def _operator_reasons(
    projection: AuthoritativeOrderProjectionPlan,
) -> tuple[OperatorRecoveryReason, ...]:
    values = {OperatorRecoveryReason.BROKER_EVIDENCE_REQUIRED}
    if OrderProjectionReason.INCOMPLETE_EVIDENCE in projection.reasons:
        values.add(OperatorRecoveryReason.INCOMPLETE_EVIDENCE)
    if OrderProjectionReason.AMBIGUOUS_EVIDENCE in projection.reasons:
        values.add(OperatorRecoveryReason.AMBIGUOUS_EVIDENCE)
    return tuple(sorted(values, key=lambda item: item.value))


def _supported_claims(
    snapshot: LiveJournalRecoverySnapshot,
    account_id: str,
) -> tuple[OutstandingDispatchClaim, ...] | None:
    orders = {order.intent.client_order_id: order for order in snapshot.orders}
    claims: list[OutstandingDispatchClaim] = []
    for claim in snapshot.outstanding_claims:
        command = claim.command
        if type(command) is not NewOrderCommand:
            return None
        order = orders.get(command.intent.client_order_id)
        if (
            order is None
            or order.intent.account_id != account_id
            or order.pending_command is None
            or order.pending_command.command != command
            or claim.expected_order_version != order.version
        ):
            return None
        claims.append(claim)
    return tuple(sorted(claims, key=lambda item: item.command.client_command_id))


def _new_claim_order_id(claim: OutstandingDispatchClaim) -> str:
    command = claim.command
    if type(command) is not NewOrderCommand:
        raise ValueError("operator recovery claim is unsupported")
    return command.intent.client_order_id


def plan_operator_recovery(
    snapshot: LiveJournalRecoverySnapshot,
    assessment: ReconciliationAssessment | None,
) -> OfflineOperatorRecoveryPlan:
    """Classify an already-hydrated snapshot without performing external work."""

    if type(snapshot) is not LiveJournalRecoverySnapshot:
        raise TypeError("snapshot must be LiveJournalRecoverySnapshot")
    if assessment is not None and type(assessment) is not ReconciliationAssessment:
        raise TypeError("assessment must be ReconciliationAssessment or None")

    verification = verify_recovery_snapshot(snapshot)
    account_id = _account_id(snapshot, assessment)
    issue_codes = tuple(item.value for item in verification.issues)
    if verification.readiness is RecoveryReadiness.BLOCKED:
        return _plan(
            account_id=account_id,
            journal_sequence=snapshot.journal_sequence,
            disposition=OperatorRecoveryDisposition.BLOCKED_INTEGRITY_FAILURE,
            issue_codes=issue_codes,
            reasons=(OperatorRecoveryReason.INTEGRITY_FAILURE,),
        )
    if verification.readiness is RecoveryReadiness.READY:
        return _plan(
            account_id=account_id,
            journal_sequence=snapshot.journal_sequence,
            disposition=OperatorRecoveryDisposition.READY_NO_ACTION,
        )

    unsupported_work = bool(
        snapshot.unresolved_observations
        or snapshot.conflict_observations
        or snapshot.ambiguous_observations
        or snapshot.reconciliation_requirements
        or set(issue_codes) - _SUPPORTED_ISSUES
    )
    claims = _supported_claims(snapshot, account_id)
    if unsupported_work or claims is None or not claims:
        return _plan(
            account_id=account_id,
            journal_sequence=snapshot.journal_sequence,
            disposition=OperatorRecoveryDisposition.UNSUPPORTED_REQUIRES_ESCALATION,
            issue_codes=issue_codes,
            reasons=(OperatorRecoveryReason.UNSUPPORTED_ISSUE,),
        )
    if assessment is None:
        return _plan(
            account_id=account_id,
            journal_sequence=snapshot.journal_sequence,
            disposition=OperatorRecoveryDisposition.NEEDS_BROKER_EVIDENCE,
            issue_codes=issue_codes,
            reasons=(OperatorRecoveryReason.BROKER_EVIDENCE_REQUIRED,),
        )
    if (
        assessment.local_snapshot.account_id != account_id
        or assessment.local_snapshot.journal_sequence != snapshot.journal_sequence
    ):
        return _plan(
            account_id=account_id,
            journal_sequence=snapshot.journal_sequence,
            disposition=OperatorRecoveryDisposition.UNSUPPORTED_REQUIRES_ESCALATION,
            issue_codes=issue_codes,
            reasons=(OperatorRecoveryReason.UNSUPPORTED_ISSUE,),
        )

    projection = project_authoritative_orders(assessment)
    if projection.disposition is OrderProjectionDisposition.NOT_AUTHORITATIVE:
        return _plan(
            account_id=account_id,
            journal_sequence=snapshot.journal_sequence,
            disposition=OperatorRecoveryDisposition.NEEDS_BROKER_EVIDENCE,
            issue_codes=issue_codes,
            reasons=_operator_reasons(projection),
            projection_plan=projection,
        )
    claim_order_ids = {_new_claim_order_id(item) for item in claims}
    projection_order_ids = {item.intent.client_order_id for item in projection.projected_orders}
    if (
        projection.disposition is not OrderProjectionDisposition.READY
        or claim_order_ids != projection_order_ids
        or len(projection.expected_order_versions) != len(claims)
    ):
        return _plan(
            account_id=account_id,
            journal_sequence=snapshot.journal_sequence,
            disposition=OperatorRecoveryDisposition.UNSUPPORTED_REQUIRES_ESCALATION,
            issue_codes=issue_codes,
            reasons=(OperatorRecoveryReason.PROJECTION_NOT_READY,),
            projection_plan=projection,
        )

    targets = tuple(
        sorted(
            (
                RedactedOperatorRecoveryTarget(
                    OperatorRecoveryTargetKind.CLAIM,
                    _target_id(
                        account_id,
                        snapshot.journal_sequence,
                        claim.command.client_command_id,
                    ),
                    (OperatorRecoveryResolution.BROKER_ORDER_CONFIRMED,),
                )
                for claim in claims
            ),
            key=lambda item: (item.kind.value, item.target_id),
        )
    )
    return _plan(
        account_id=account_id,
        journal_sequence=snapshot.journal_sequence,
        disposition=OperatorRecoveryDisposition.READY_FOR_EXPLICIT_COMMIT,
        issue_codes=issue_codes,
        targets=targets,
        projection_plan=projection,
    )


def build_operator_reconciliation_request(
    plan: OfflineOperatorRecoveryPlan,
    selection: ExplicitOperatorRecoverySelection,
    snapshot: LiveJournalRecoverySnapshot,
    assessment: ReconciliationAssessment,
) -> DurableReconciliationCommitRequest:
    """Build, but never execute, one explicit durable reconciliation request."""

    if type(plan) is not OfflineOperatorRecoveryPlan:
        raise TypeError("plan must be OfflineOperatorRecoveryPlan")
    if type(selection) is not ExplicitOperatorRecoverySelection:
        raise TypeError("selection must be ExplicitOperatorRecoverySelection")
    if type(snapshot) is not LiveJournalRecoverySnapshot:
        raise TypeError("snapshot must be LiveJournalRecoverySnapshot")
    if type(assessment) is not ReconciliationAssessment:
        raise TypeError("assessment must be ReconciliationAssessment")
    if plan.disposition is not OperatorRecoveryDisposition.READY_FOR_EXPLICIT_COMMIT:
        raise ValueError("operator recovery plan is not ready for explicit commit")
    if (
        selection.account_id != plan.account_id
        or selection.journal_sequence != plan.journal_sequence
        or selection.inspection_digest != plan.inspection_digest
        or snapshot.journal_sequence != plan.journal_sequence
    ):
        raise ValueError("operator recovery selection does not match inspected journal cut")
    recomputed = plan_operator_recovery(snapshot, assessment)
    if recomputed != plan:
        raise ValueError("operator recovery plan no longer matches inspected state")

    expected_targets = tuple((item.kind, item.target_id) for item in plan.targets)
    selected_targets = tuple((item.kind, item.target_id) for item in selection.selected_targets)
    if expected_targets != selected_targets or any(
        item.resolution is not OperatorRecoveryResolution.BROKER_ORDER_CONFIRMED
        for item in selection.selected_targets
    ):
        raise ValueError("operator recovery selection must match the complete target set")

    claims_by_target = {
        _target_id(
            plan.account_id,
            snapshot.journal_sequence,
            claim.command.client_command_id,
        ): claim
        for claim in snapshot.outstanding_claims
    }
    try:
        selected_claims = tuple(claims_by_target[item.target_id] for item in plan.targets)
    except KeyError:
        raise ValueError("operator recovery plan no longer matches inspected state") from None
    projection = plan.projection_plan
    if projection is None or projection.disposition is not OrderProjectionDisposition.READY:
        raise ValueError("operator recovery projection is not ready")

    claim_directives = tuple(
        sorted(
            (
                ClaimResolutionDirective(
                    claim.command.client_command_id,
                    claim.claim_token,
                    ClaimResolution.BROKER_ORDER_CONFIRMED,
                )
                for claim in selected_claims
            ),
            key=lambda item: item.client_command_id,
        )
    )
    return DurableReconciliationCommitRequest(
        selection.commit_id,
        plan.account_id,
        assessment,
        plan.journal_sequence,
        projection.expected_order_versions,
        claim_directives,
        order_projections=projection.projected_orders,
    )


__all__ = [
    "build_operator_reconciliation_request",
    "plan_operator_recovery",
]
