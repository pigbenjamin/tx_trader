"""Pure planning for conservative, evidence-backed live-order projections."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime

from .live_contracts import (
    BrokerFillObservation,
    BrokerOpenOrderObservation,
    CorrelationStatus,
    LiveOrder,
    LiveOrderState,
    NewOrderCommand,
    ReconciliationKind,
)
from .live_ports import ReconciliationStatus
from .live_reconciliation import assess_reconciliation
from .live_reconciliation_commit_contracts import ExpectedOrderVersion
from .live_reconciliation_contracts import ReconciliationAssessment
from .live_reconciliation_projection_contracts import (
    AuthoritativeOrderProjectionPlan,
    OrderProjectionDisposition,
    OrderProjectionReason,
)

_PROJECTABLE_STATES = frozenset({LiveOrderState.SUBMISSION_UNKNOWN, LiveOrderState.RECONCILING})


def _reasons(
    values: set[OrderProjectionReason],
) -> tuple[OrderProjectionReason, ...]:
    return tuple(sorted(values, key=lambda item: item.value))


def _base(
    assessment: ReconciliationAssessment,
    disposition: OrderProjectionDisposition,
    *,
    reasons: set[OrderProjectionReason] | None = None,
    unsupported_discrepancy_ids: set[str] | None = None,
) -> AuthoritativeOrderProjectionPlan:
    return AuthoritativeOrderProjectionPlan(
        account_id=assessment.local_snapshot.account_id,
        snapshot_id=assessment.broker_snapshot.snapshot_id,
        expected_journal_sequence=assessment.local_snapshot.journal_sequence,
        disposition=disposition,
        unsupported_discrepancy_ids=tuple(sorted(unsupported_discrepancy_ids or ())),
        reasons=_reasons(reasons or set()),
    )


def _authority_reasons(assessment: ReconciliationAssessment) -> set[OrderProjectionReason]:
    reasons = {OrderProjectionReason.NOT_AUTHORITATIVE}
    if assessment.result.status is ReconciliationStatus.INCOMPLETE:
        reasons.add(OrderProjectionReason.INCOMPLETE_EVIDENCE)
    elif assessment.result.status is ReconciliationStatus.AMBIGUOUS:
        reasons.add(OrderProjectionReason.AMBIGUOUS_EVIDENCE)
    return reasons


def _local_reasons(order: LiveOrder) -> set[OrderProjectionReason]:
    reasons: set[OrderProjectionReason] = set()
    if (
        order.state not in _PROJECTABLE_STATES
        or order.accepted_at is not None
        or order.filled_quantity != 0
        or order.remaining_quantity != order.total_quantity
        or order.average_fill_price is not None
    ):
        reasons.add(OrderProjectionReason.UNSUPPORTED_LOCAL_STATE)
    binding = order.pending_command
    if binding is None or type(binding.command) is not NewOrderCommand:
        reasons.add(OrderProjectionReason.UNSUPPORTED_COMMAND)
    else:
        intent = binding.command.intent
        if (
            order.total_quantity != intent.quantity
            or order.remaining_quantity != intent.quantity
            or order.working_limit_price != intent.limit_price
        ):
            reasons.add(OrderProjectionReason.UNSUPPORTED_LOCAL_STATE)
    return reasons


def _broker_reasons(
    order: LiveOrder,
    matches: tuple[BrokerOpenOrderObservation, ...],
    correlated_fills: tuple[BrokerFillObservation, ...],
    local_snapshot_as_of: datetime,
) -> set[OrderProjectionReason]:
    if not matches:
        reasons = {OrderProjectionReason.MISSING_BROKER_MATCH}
    elif len(matches) > 1:
        reasons = {OrderProjectionReason.MULTIPLE_BROKER_MATCHES}
    else:
        observation = matches[0]
        correlation = observation.correlation
        reasons = set()
        if (
            correlation.status is not CorrelationStatus.CONFIRMED
            or correlation.client_order_id != order.intent.client_order_id
            or observation.account_id != order.intent.account_id
            or observation.instrument_id != order.intent.instrument_id
            or observation.side is not order.intent.side
        ):
            reasons.add(OrderProjectionReason.BROKER_IDENTITY_MISMATCH)
        if (
            observation.working_total_quantity != order.total_quantity
            or observation.working_remaining_quantity != order.remaining_quantity
            or observation.working_remaining_quantity != observation.working_total_quantity
        ):
            reasons.add(OrderProjectionReason.BROKER_QUANTITY_MISMATCH)
        if observation.working_limit_price != order.working_limit_price:
            reasons.add(OrderProjectionReason.BROKER_PRICE_MISMATCH)
        if (
            observation.observed_at < max(order.updated_at, local_snapshot_as_of)
            or correlation.correlated_at > observation.observed_at
        ):
            reasons.add(OrderProjectionReason.BROKER_TIME_MISMATCH)
    if correlated_fills:
        reasons.add(OrderProjectionReason.FILL_EVIDENCE_UNSUPPORTED)
    return reasons


def project_authoritative_orders(
    assessment: ReconciliationAssessment,
) -> AuthoritativeOrderProjectionPlan:
    """Return a deterministic plan without performing persistence or dispatch."""

    if type(assessment) is not ReconciliationAssessment:
        raise TypeError("assessment must be ReconciliationAssessment")

    recomputed = assess_reconciliation(
        assessment.local_snapshot,
        assessment.broker_snapshot,
        assessment.result.reconciled_at,
    )
    if recomputed != assessment:
        return _base(
            assessment,
            OrderProjectionDisposition.NOT_AUTHORITATIVE,
            reasons={OrderProjectionReason.ASSESSMENT_MISMATCH},
        )
    if not assessment.result.is_authoritative:
        return _base(
            assessment,
            OrderProjectionDisposition.NOT_AUTHORITATIVE,
            reasons=_authority_reasons(assessment),
        )

    discrepancies = assessment.result.discrepancies
    if not discrepancies:
        if assessment.local_snapshot.recovery_blockers:
            return _base(
                assessment,
                OrderProjectionDisposition.UNSUPPORTED,
                reasons={OrderProjectionReason.UNSUPPORTED_DISCREPANCY},
            )
        return _base(assessment, OrderProjectionDisposition.NO_CHANGE)

    local_by_id = {
        order.intent.client_order_id: order for order in assessment.local_snapshot.orders
    }
    open_by_client_id: dict[str, list[BrokerOpenOrderObservation]] = {}
    for open_observation in assessment.broker_snapshot.open_orders.orders:
        client_order_id = open_observation.correlation.client_order_id
        if client_order_id is not None:
            open_by_client_id.setdefault(client_order_id, []).append(open_observation)
    fills_by_client_id: dict[str, list[BrokerFillObservation]] = {}
    for fill_observation in assessment.broker_snapshot.fills.fills:
        client_order_id = fill_observation.correlation.client_order_id
        if client_order_id is not None:
            fills_by_client_id.setdefault(client_order_id, []).append(fill_observation)

    reasons: set[OrderProjectionReason] = set()
    unsupported_ids: set[str] = set()
    projectable: list[tuple[str, str, LiveOrder, BrokerOpenOrderObservation]] = []
    seen_order_ids: set[str] = set()
    for discrepancy in discrepancies:
        discrepancy_reasons: set[OrderProjectionReason] = set()
        client_order_id = discrepancy.client_order_id
        if client_order_id is None:
            discrepancy_reasons.add(OrderProjectionReason.UNSUPPORTED_DISCREPANCY)
            order = None
        else:
            order = local_by_id.get(client_order_id)
        if discrepancy.kind is not ReconciliationKind.CORRELATION_MISSING or order is None:
            discrepancy_reasons.add(OrderProjectionReason.UNSUPPORTED_DISCREPANCY)
        elif client_order_id is None or client_order_id in seen_order_ids:
            discrepancy_reasons.add(OrderProjectionReason.UNSUPPORTED_DISCREPANCY)
        else:
            seen_order_ids.add(client_order_id)
            discrepancy_reasons.update(_local_reasons(order))
            matches = tuple(open_by_client_id.get(client_order_id, ()))
            correlated_fills = tuple(fills_by_client_id.get(client_order_id, ()))
            discrepancy_reasons.update(
                _broker_reasons(
                    order,
                    matches,
                    correlated_fills,
                    assessment.local_snapshot.as_of,
                )
            )
            if not discrepancy_reasons:
                projectable.append((client_order_id, discrepancy.discrepancy_id, order, matches[0]))
        if discrepancy_reasons:
            unsupported_ids.add(discrepancy.discrepancy_id)
            reasons.update(discrepancy_reasons)

    if reasons:
        return _base(
            assessment,
            OrderProjectionDisposition.UNSUPPORTED,
            reasons=reasons,
            unsupported_discrepancy_ids=unsupported_ids,
        )

    projectable.sort(key=lambda item: item[0])
    expected_versions = tuple(
        ExpectedOrderVersion(client_order_id, order.version)
        for client_order_id, _, order, _ in projectable
    )
    projected_orders = tuple(
        replace(
            order,
            state=LiveOrderState.ACCEPTED,
            filled_quantity=order.filled_quantity,
            remaining_quantity=order.remaining_quantity,
            average_fill_price=order.average_fill_price,
            version=order.version + 1,
            updated_at=observation.observed_at,
            accepted_at=observation.observed_at,
            pending_command=None,
        )
        for _, _, order, observation in projectable
    )
    consumed_ids = tuple(sorted(item[1] for item in projectable))
    return AuthoritativeOrderProjectionPlan(
        account_id=assessment.local_snapshot.account_id,
        snapshot_id=assessment.broker_snapshot.snapshot_id,
        expected_journal_sequence=assessment.local_snapshot.journal_sequence,
        disposition=OrderProjectionDisposition.READY,
        expected_order_versions=expected_versions,
        projected_orders=projected_orders,
        consumed_discrepancy_ids=consumed_ids,
    )


__all__ = ["project_authoritative_orders"]
