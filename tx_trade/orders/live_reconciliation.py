"""Deterministic, fake-only reconciliation for live-order snapshots.

This module deliberately has no persistence, broker SDK, configuration, or
dispatch dependencies.  All decisions are derived from the supplied snapshots
and explicit reconciliation time.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
import json
from typing import Protocol

from .live_contracts import (
    BrokerFillObservation,
    BrokerOpenOrderObservation,
    BrokerPosition,
    CorrelationStatus,
    LiveFill,
    LiveOrderState,
    ReconciliationDiscrepancy,
    ReconciliationKind,
)
from .live_ports import (
    EvidenceCompleteness,
    ReconciliationResult,
    ReconciliationStatus,
)
from .live_reconciliation_contracts import (
    BrokerReconciliationSnapshot,
    BrokerReconciliationSnapshotSourcePort,
    LocalReconciliationSnapshot,
    LocalReconciliationSourcePort,
    ReconciliationAssessment,
    exact_decimal_sum,
)

_WORKING_STATES = frozenset(
    {
        LiveOrderState.ACCEPTED,
        LiveOrderState.PARTIALLY_FILLED,
        LiveOrderState.CANCEL_PENDING,
    }
)
_DISCREPANCY_DOMAIN = b"tx_trade.live_reconciliation.discrepancy.v1\x00"


class UtcClock(Protocol):
    """Minimal injected clock used by the fake-only orchestration service."""

    def now(self) -> datetime: ...


def _require_utc(value: object, name: str) -> datetime:
    if type(value) is not datetime:
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware UTC")
    if value.utcoffset() != timezone.utc.utcoffset(value):
        raise ValueError(f"{name} must use UTC")
    return value


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    decimal_tuple = value.as_tuple()
    if all(digit == 0 for digit in decimal_tuple.digits):
        return "0"
    digits = list(decimal_tuple.digits)
    exponent = decimal_tuple.exponent
    assert isinstance(exponent, int)
    while digits[-1] == 0:
        digits.pop()
        exponent += 1
    coefficient = "".join(str(digit) for digit in digits)
    sign = "-" if decimal_tuple.sign else ""
    return f"{sign}{coefficient}e{exponent}"


def _correlation_material(
    observation: BrokerOpenOrderObservation | BrokerFillObservation,
) -> tuple[object, ...]:
    correlation = observation.correlation
    return (
        correlation.status.value,
        correlation.client_order_id,
        correlation.broker_fill_id,
        correlation.execution_no,
        correlation.broker_order_sequence,
        correlation.broker_book_no,
        correlation.proxy_stamp_id,
        correlation.async_thread_id,
        correlation.submission_attempt_id,
    )


def _open_payload(item: BrokerOpenOrderObservation) -> tuple[object, ...]:
    return (
        item.account_id,
        item.instrument_id,
        item.side.value,
        _decimal_text(item.working_total_quantity),
        _decimal_text(item.working_remaining_quantity),
        _decimal_text(item.working_limit_price),
        _correlation_material(item),
    )


def _fill_payload(item: BrokerFillObservation) -> tuple[object, ...]:
    return (
        item.account_id,
        item.instrument_id,
        item.side.value,
        _decimal_text(item.quantity),
        _decimal_text(item.execution_price),
        item.occurred_at.isoformat() if item.occurred_at is not None else None,
        _correlation_material(item),
    )


def _position_payload(item: BrokerPosition) -> tuple[object, ...]:
    return (
        item.account_id,
        item.instrument_id,
        _decimal_text(item.net_quantity),
        _decimal_text(item.average_open_price),
    )


def _stable_key(value: tuple[object, ...]) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _dedupe_and_find_conflicts(
    values: tuple[object, ...],
    *,
    identities: Callable[[object], tuple[str, ...]],
    payload: Callable[[object], tuple[object, ...]],
) -> tuple[tuple[object, ...], frozenset[str]]:
    """Dedupe equal semantic observations and flag reused identities."""

    by_identity: dict[str, set[tuple[object, ...]]] = {}
    unique: dict[tuple[object, ...], object] = {}
    for value in values:
        material = payload(value)
        unique.setdefault(material, value)
        for identity in identities(value):
            by_identity.setdefault(identity, set()).add(material)
    conflicts = frozenset(
        identity for identity, payloads in by_identity.items() if len(payloads) > 1
    )
    ordered = tuple(unique[key] for key in sorted(unique, key=_stable_key))
    return ordered, conflicts


def _open_identities(value: object) -> tuple[str, ...]:
    assert isinstance(value, BrokerOpenOrderObservation)
    identities = [f"observation:{value.observation_id}"]
    correlation = value.correlation
    if correlation.status is CorrelationStatus.CONFIRMED:
        assert correlation.client_order_id is not None
        identities.append(f"client:{correlation.client_order_id}")
    return tuple(identities)


def _fill_ids(item: BrokerFillObservation) -> tuple[str, ...]:
    correlation = item.correlation
    return tuple(
        identity
        for identity in (correlation.broker_fill_id, correlation.execution_no)
        if identity is not None
    )


def _fill_identities(value: object) -> tuple[str, ...]:
    assert isinstance(value, BrokerFillObservation)
    correlation = value.correlation
    identities = [f"observation:{value.observation_id}"]
    if correlation.broker_fill_id is not None:
        identities.append(f"broker_fill:{correlation.broker_fill_id}")
    if correlation.execution_no is not None:
        identities.append(f"execution:{correlation.execution_no}")
    return tuple(identities)


def _position_identities(value: object) -> tuple[str, ...]:
    assert isinstance(value, BrokerPosition)
    return (f"instrument:{value.instrument_id}",)


def _discrepancy_id(
    kind: ReconciliationKind,
    account_id: str,
    instrument_id: str,
    client_order_id: str | None,
    expected: Decimal | None,
    actual: Decimal | None,
) -> str:
    material = (
        kind.value,
        account_id,
        instrument_id,
        client_order_id,
        _decimal_text(expected),
        _decimal_text(actual),
    )
    encoded = json.dumps(material, ensure_ascii=True, separators=(",", ":")).encode()
    return f"sha256:{sha256(_DISCREPANCY_DOMAIN + encoded).hexdigest()}"


def _make_discrepancy(
    *,
    kind: ReconciliationKind,
    account_id: str,
    instrument_id: str,
    observed_at: datetime,
    client_order_id: str | None = None,
    expected: Decimal | None = None,
    actual: Decimal | None = None,
) -> ReconciliationDiscrepancy:
    return ReconciliationDiscrepancy(
        _discrepancy_id(kind, account_id, instrument_id, client_order_id, expected, actual),
        kind,
        account_id,
        instrument_id,
        observed_at,
        client_order_id,
        expected,
        actual,
    )


def assess_reconciliation(
    local_snapshot: LocalReconciliationSnapshot,
    broker_snapshot: BrokerReconciliationSnapshot,
    reconciled_at: datetime,
) -> ReconciliationAssessment:
    """Purely assess two immutable snapshots at an explicit UTC time."""

    if type(local_snapshot) is not LocalReconciliationSnapshot:
        raise TypeError("local_snapshot must be LocalReconciliationSnapshot")
    if type(broker_snapshot) is not BrokerReconciliationSnapshot:
        raise TypeError("broker_snapshot must be BrokerReconciliationSnapshot")
    reconciled_at = _require_utc(reconciled_at, "reconciled_at")
    if local_snapshot.account_id != broker_snapshot.account_id:
        raise ValueError("local and broker snapshot accounts must match")
    if reconciled_at < local_snapshot.as_of or reconciled_at < broker_snapshot.captured_at:
        raise ValueError("reconciled_at must not predate either snapshot")

    account_id = local_snapshot.account_id
    discrepancies: dict[str, ReconciliationDiscrepancy] = {}

    def add(
        kind: ReconciliationKind,
        instrument_id: str,
        client_order_id: str | None = None,
        expected: Decimal | None = None,
        actual: Decimal | None = None,
    ) -> None:
        item = _make_discrepancy(
            kind=kind,
            account_id=account_id,
            instrument_id=instrument_id,
            observed_at=reconciled_at,
            client_order_id=client_order_id,
            expected=expected,
            actual=actual,
        )
        discrepancies[item.discrepancy_id] = item

    raw_open = broker_snapshot.open_orders.orders
    raw_fills = broker_snapshot.fills.fills
    raw_positions = broker_snapshot.positions.positions
    open_values_raw, open_conflicts = _dedupe_and_find_conflicts(
        raw_open,
        identities=_open_identities,
        payload=lambda value: _open_payload(value),  # type: ignore[arg-type]
    )
    fill_values_raw, fill_conflicts = _dedupe_and_find_conflicts(
        raw_fills,
        identities=_fill_identities,
        payload=lambda value: _fill_payload(value),  # type: ignore[arg-type]
    )
    position_values_raw, position_conflicts = _dedupe_and_find_conflicts(
        raw_positions,
        identities=_position_identities,
        payload=lambda value: _position_payload(value),  # type: ignore[arg-type]
    )
    open_values = tuple(
        item for item in open_values_raw if isinstance(item, BrokerOpenOrderObservation)
    )
    fill_values = tuple(item for item in fill_values_raw if isinstance(item, BrokerFillObservation))
    position_values = tuple(
        item for item in position_values_raw if isinstance(item, BrokerPosition)
    )

    open_ambiguous = bool(open_conflicts) or any(
        item.correlation.status is not CorrelationStatus.CONFIRMED for item in open_values
    )
    fills_ambiguous = bool(fill_conflicts) or any(
        item.correlation.status is not CorrelationStatus.CONFIRMED for item in fill_values
    )
    ambiguous = open_ambiguous or fills_ambiguous or bool(position_conflicts)
    local_orders = {item.intent.client_order_id: item for item in local_snapshot.orders}
    for pending_local_order in local_snapshot.orders:
        if pending_local_order.pending_command is not None:
            add(
                ReconciliationKind.CORRELATION_MISSING,
                pending_local_order.intent.instrument_id,
                pending_local_order.intent.client_order_id,
            )
    matched_open_ids: set[str] = set()

    for broker_order in open_values:
        correlation = broker_order.correlation
        client_id = correlation.client_order_id
        identities = _open_identities(broker_order)
        conflicted = any(identity in open_conflicts for identity in identities)
        if correlation.status is not CorrelationStatus.CONFIRMED or conflicted:
            ambiguous = True
            add(ReconciliationKind.CORRELATION_MISSING, broker_order.instrument_id)
            continue
        assert client_id is not None
        local_order = local_orders.get(client_id)
        if local_order is None:
            add(ReconciliationKind.MISSING_LOCAL_ORDER, broker_order.instrument_id, client_id)
            continue
        matched_open_ids.add(client_id)
        if (
            local_order.intent.instrument_id != broker_order.instrument_id
            or local_order.intent.side is not broker_order.side
            or (local_order.state not in _WORKING_STATES and local_order.pending_command is None)
            or local_order.working_limit_price != broker_order.working_limit_price
        ):
            add(ReconciliationKind.ORDER_STATE_MISMATCH, broker_order.instrument_id, client_id)
        if local_order.state in _WORKING_STATES and (
            local_order.total_quantity != broker_order.working_total_quantity
            or local_order.remaining_quantity != broker_order.working_remaining_quantity
        ):
            if local_order.total_quantity != broker_order.working_total_quantity:
                expected_quantity = local_order.total_quantity
                actual_quantity = broker_order.working_total_quantity
            else:
                expected_quantity = local_order.remaining_quantity
                actual_quantity = broker_order.working_remaining_quantity
            add(
                ReconciliationKind.QUANTITY_MISMATCH,
                broker_order.instrument_id,
                client_id,
                expected_quantity,
                actual_quantity,
            )

    open_complete = broker_snapshot.open_orders.evidence.status is EvidenceCompleteness.COMPLETE
    for client_id, local_order in sorted(local_orders.items()):
        if client_id in matched_open_ids:
            continue
        if local_order.pending_command is not None:
            continue
        if open_complete and not open_ambiguous and local_order.state in _WORKING_STATES:
            add(
                ReconciliationKind.MISSING_BROKER_ORDER, local_order.intent.instrument_id, client_id
            )

    local_fills = {item.fill_id: item for item in local_snapshot.fills}
    matched_fill_ids: set[str] = set()
    resolved_fills: list[tuple[BrokerFillObservation, LiveFill]] = []
    for broker_fill in fill_values:
        identities = _fill_identities(broker_fill)
        conflicted = any(identity in fill_conflicts for identity in identities)
        correlation = broker_fill.correlation
        if correlation.status is not CorrelationStatus.CONFIRMED or conflicted:
            ambiguous = True
            add(ReconciliationKind.CORRELATION_MISSING, broker_fill.instrument_id)
            continue
        broker_ids = _fill_ids(broker_fill)
        candidates = {local_fills[item] for item in broker_ids if item in local_fills}
        if len(candidates) != 1:
            if len(candidates) > 1:
                ambiguous = True
                fills_ambiguous = True
            add(
                ReconciliationKind.FILL_MISMATCH,
                broker_fill.instrument_id,
                correlation.client_order_id,
            )
            continue
        resolved_fills.append((broker_fill, candidates.pop()))

    broker_observations_by_local_fill: dict[str, list[BrokerFillObservation]] = {}
    for broker_fill, local_fill in resolved_fills:
        broker_observations_by_local_fill.setdefault(local_fill.fill_id, []).append(broker_fill)
    collided_local_fill_ids = {
        fill_id
        for fill_id, observations in broker_observations_by_local_fill.items()
        if len(observations) > 1
    }
    if collided_local_fill_ids:
        ambiguous = True
        fills_ambiguous = True

    for broker_fill, local_fill in resolved_fills:
        correlation = broker_fill.correlation
        if local_fill.fill_id in collided_local_fill_ids:
            add(
                ReconciliationKind.FILL_MISMATCH,
                broker_fill.instrument_id,
                correlation.client_order_id,
            )
            continue
        matched_fill_ids.add(local_fill.fill_id)
        if (
            local_fill.account_id != broker_fill.account_id
            or local_fill.instrument_id != broker_fill.instrument_id
            or local_fill.side is not broker_fill.side
            or local_fill.quantity != broker_fill.quantity
            or local_fill.execution_price != broker_fill.execution_price
            or local_fill.client_order_id != correlation.client_order_id
            or (
                broker_fill.occurred_at is not None
                and local_fill.occurred_at != broker_fill.occurred_at
            )
        ):
            add(
                ReconciliationKind.FILL_MISMATCH,
                broker_fill.instrument_id,
                local_fill.client_order_id,
            )

    if (
        broker_snapshot.fills.evidence.status is EvidenceCompleteness.COMPLETE
        and not fills_ambiguous
    ):
        for fill_id, local_fill in sorted(local_fills.items()):
            if fill_id not in matched_fill_ids:
                add(
                    ReconciliationKind.FILL_MISMATCH,
                    local_fill.instrument_id,
                    local_fill.client_order_id,
                )

    position_quantities: dict[str, list[Decimal]] = {}
    for attribution in local_snapshot.position_attributions:
        position_quantities.setdefault(attribution.instrument_id, []).append(
            attribution.attributed_quantity
        )
    expected_positions = {
        instrument_id: exact_decimal_sum(quantities)
        for instrument_id, quantities in position_quantities.items()
    }
    broker_positions: dict[str, Decimal] = {}
    for position in position_values:
        identity = f"instrument:{position.instrument_id}"
        if identity not in position_conflicts:
            broker_positions[position.instrument_id] = position.net_quantity
    positions_complete = broker_snapshot.positions.evidence.status is EvidenceCompleteness.COMPLETE
    for instrument_id in sorted(set(expected_positions) | set(broker_positions)):
        if f"instrument:{instrument_id}" in position_conflicts:
            continue
        expected = expected_positions.get(instrument_id, Decimal(0))
        if instrument_id not in broker_positions and not positions_complete:
            continue
        actual = broker_positions.get(instrument_id, Decimal(0))
        if expected != actual:
            add(
                ReconciliationKind.POSITION_MISMATCH,
                instrument_id,
                expected=expected,
                actual=actual,
            )
    evidence = (
        broker_snapshot.open_orders.evidence,
        broker_snapshot.fills.evidence,
        broker_snapshot.positions.evidence,
    )
    if ambiguous:
        status = ReconciliationStatus.AMBIGUOUS
    elif any(item.status is not EvidenceCompleteness.COMPLETE for item in evidence):
        status = ReconciliationStatus.INCOMPLETE
    else:
        status = ReconciliationStatus.COMPLETE
    ordered_discrepancies = tuple(
        sorted(
            discrepancies.values(),
            key=lambda item: (
                item.kind.value,
                item.instrument_id,
                item.client_order_id or "",
                _decimal_text(item.expected_quantity) or "",
                _decimal_text(item.actual_quantity) or "",
                item.discrepancy_id,
            ),
        )
    )
    result = ReconciliationResult(
        account_id,
        status,
        ordered_discrepancies,
        evidence,
        reconciled_at,
    )
    return ReconciliationAssessment(local_snapshot, broker_snapshot, result)


# A descriptive alias keeps the pure entry point easy to discover.
reconcile_snapshots = assess_reconciliation


class FakeOnlyReconciliationService:
    """Thin snapshot orchestration with injected, side-effect-free dependencies."""

    def __init__(
        self,
        local_source: LocalReconciliationSourcePort,
        broker_snapshot_source: BrokerReconciliationSnapshotSourcePort,
        clock: UtcClock,
    ) -> None:
        self._local_source = local_source
        self._broker_snapshot_source = broker_snapshot_source
        self._clock = clock

    def assess(self, account_id: str) -> ReconciliationAssessment:
        local = self._local_source.load_account_snapshot(account_id)
        broker = self._broker_snapshot_source.query_reconciliation_snapshot(account_id)
        reconciled_at = _require_utc(self._clock.now(), "clock.now()")
        return assess_reconciliation(local, broker, reconciled_at)

    def reconcile(self, account_id: str) -> ReconciliationResult:
        return self.assess(account_id).result


__all__ = [
    "FakeOnlyReconciliationService",
    "UtcClock",
    "assess_reconciliation",
    "reconcile_snapshots",
]
