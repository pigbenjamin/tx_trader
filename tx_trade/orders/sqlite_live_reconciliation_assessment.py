"""One-shot trusted-source assessment for a sealed SQLite live journal.

The trusted bootstrap or caller must choose the injected broker source.  Runtime
structural protocol checks validate only its shape; they do not authenticate or
otherwise establish the source's identity.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
import re
from typing import Protocol, runtime_checkable

from .live_journal_inspection_contracts import (
    LiveJournalInspectionDisposition,
    LiveJournalInspectionError,
    LiveJournalInspectionFailureCode,
    LiveJournalInspectionReport,
)
from .live_contracts import (
    BrokerFillObservation,
    BrokerOpenOrderObservation,
    BrokerPosition,
)
from .live_ports import (
    BrokerFillsSnapshot,
    BrokerPositionsSnapshot,
    CompletenessEvidence,
    OpenOrdersSnapshot,
)
from .live_reconciliation import UtcClock, assess_reconciliation
from .live_reconciliation_assessment_contracts import (
    MAX_TRUSTED_BROKER_OBSERVATIONS,
    InspectedReconciliationAssessment,
    TrustedAssessmentSourceError,
    TrustedAssessmentSourceFailureCode,
)
from .live_reconciliation_contracts import (
    BrokerReconciliationSnapshot,
    BrokerReconciliationSnapshotSourcePort,
    LocalReconciliationSnapshot,
    ReconciliationAssessment,
)
from .sqlite_live_journal_inspection import (
    _inspect_sqlite_live_order_journal_with_account_snapshot,
)


@runtime_checkable
class _RuntimeUtcClock(Protocol):
    def now(self) -> datetime: ...


_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")


_INSPECTION_FAILURE_CODES = {
    LiveJournalInspectionFailureCode.INVALID_REQUEST: (
        TrustedAssessmentSourceFailureCode.INVALID_REQUEST
    ),
    LiveJournalInspectionFailureCode.SOURCE_UNAVAILABLE: (
        TrustedAssessmentSourceFailureCode.SOURCE_UNAVAILABLE
    ),
    LiveJournalInspectionFailureCode.ACTIVE_OR_UNCLEAN_SOURCE: (
        TrustedAssessmentSourceFailureCode.ACTIVE_OR_UNCLEAN_SOURCE
    ),
    LiveJournalInspectionFailureCode.SOURCE_CHANGED: (
        TrustedAssessmentSourceFailureCode.SOURCE_CHANGED
    ),
    LiveJournalInspectionFailureCode.CAPACITY_EXCEEDED: (
        TrustedAssessmentSourceFailureCode.CAPACITY_EXCEEDED
    ),
    LiveJournalInspectionFailureCode.INTEGRITY_FAILURE: (
        TrustedAssessmentSourceFailureCode.INTEGRITY_FAILURE
    ),
}


def _failure(code: TrustedAssessmentSourceFailureCode) -> TrustedAssessmentSourceError:
    return TrustedAssessmentSourceError(code)


def _clock_now(clock: UtcClock) -> datetime:
    try:
        value = clock.now()
        if type(value) is not datetime:
            raise TypeError
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError
        if value.utcoffset() != timezone.utc.utcoffset(value):
            raise ValueError
        return value
    except MemoryError:
        raise _failure(TrustedAssessmentSourceFailureCode.CAPACITY_EXCEEDED) from None
    except (Exception, KeyboardInterrupt, SystemExit):
        raise _failure(TrustedAssessmentSourceFailureCode.INTERNAL_FAILURE) from None


def _canonical_broker_snapshot(
    snapshot: BrokerReconciliationSnapshot,
) -> BrokerReconciliationSnapshot:
    """Re-run every nested contract invariant, including on forged frozen values."""

    def canonical_evidence(value: object) -> CompletenessEvidence:
        if type(value) is not CompletenessEvidence:
            raise TypeError
        return replace(value)

    if type(snapshot.open_orders) is not OpenOrdersSnapshot:
        raise TypeError
    if type(snapshot.fills) is not BrokerFillsSnapshot:
        raise TypeError
    if type(snapshot.positions) is not BrokerPositionsSnapshot:
        raise TypeError
    if type(snapshot.open_orders.orders) is not tuple:
        raise TypeError
    if type(snapshot.fills.fills) is not tuple:
        raise TypeError
    if type(snapshot.positions.positions) is not tuple:
        raise TypeError

    open_orders: list[BrokerOpenOrderObservation] = []
    for open_order in snapshot.open_orders.orders:
        if type(open_order) is not BrokerOpenOrderObservation:
            raise TypeError
        open_orders.append(replace(open_order, correlation=replace(open_order.correlation)))
    fills: list[BrokerFillObservation] = []
    for fill in snapshot.fills.fills:
        if type(fill) is not BrokerFillObservation:
            raise TypeError
        fills.append(replace(fill, correlation=replace(fill.correlation)))
    positions: list[BrokerPosition] = []
    for position in snapshot.positions.positions:
        if type(position) is not BrokerPosition:
            raise TypeError
        positions.append(replace(position))

    return BrokerReconciliationSnapshot(
        snapshot.snapshot_id,
        snapshot.account_id,
        OpenOrdersSnapshot(tuple(open_orders), canonical_evidence(snapshot.open_orders.evidence)),
        BrokerFillsSnapshot(tuple(fills), canonical_evidence(snapshot.fills.evidence)),
        BrokerPositionsSnapshot(tuple(positions), canonical_evidence(snapshot.positions.evidence)),
        snapshot.captured_at,
    )


def _admit_broker_account_id(snapshot: BrokerReconciliationSnapshot) -> str:
    broker_account_id = snapshot.account_id
    if type(broker_account_id) is not str or _IDENTIFIER.fullmatch(broker_account_id) is None:
        raise TypeError
    return broker_account_id


def _admit_broker_observation_count(snapshot: BrokerReconciliationSnapshot) -> int:
    """Validate cheap outer shape and count before reconstructing observations."""

    if type(snapshot.open_orders) is not OpenOrdersSnapshot:
        raise TypeError
    if type(snapshot.fills) is not BrokerFillsSnapshot:
        raise TypeError
    if type(snapshot.positions) is not BrokerPositionsSnapshot:
        raise TypeError
    if type(snapshot.open_orders.orders) is not tuple:
        raise TypeError
    if type(snapshot.fills.fills) is not tuple:
        raise TypeError
    if type(snapshot.positions.positions) is not tuple:
        raise TypeError
    observation_count = (
        len(snapshot.open_orders.orders)
        + len(snapshot.fills.fills)
        + len(snapshot.positions.positions)
    )
    return observation_count


def assess_sqlite_live_order_journal(
    path: str | Path,
    *,
    account_id: str,
    broker_snapshot_source: BrokerReconciliationSnapshotSourcePort,
    clock: UtcClock,
) -> InspectedReconciliationAssessment:
    """Inspect once and assess once using dependencies chosen by a trusted caller."""

    try:
        valid_source = isinstance(broker_snapshot_source, BrokerReconciliationSnapshotSourcePort)
        valid_clock = isinstance(clock, _RuntimeUtcClock)
    except MemoryError:
        raise _failure(TrustedAssessmentSourceFailureCode.CAPACITY_EXCEEDED) from None
    except (Exception, KeyboardInterrupt, SystemExit):
        raise _failure(TrustedAssessmentSourceFailureCode.INVALID_REQUEST) from None
    if not valid_source or not valid_clock:
        raise _failure(TrustedAssessmentSourceFailureCode.INVALID_REQUEST)

    local_as_of = _clock_now(clock)
    try:
        inspected = _inspect_sqlite_live_order_journal_with_account_snapshot(
            path,
            account_id=account_id,
            as_of=local_as_of,
        )
    except LiveJournalInspectionError as exc:
        raise _failure(_INSPECTION_FAILURE_CODES[exc.code]) from None
    except MemoryError:
        raise _failure(TrustedAssessmentSourceFailureCode.CAPACITY_EXCEEDED) from None
    except (Exception, KeyboardInterrupt, SystemExit):
        raise _failure(TrustedAssessmentSourceFailureCode.INTERNAL_FAILURE) from None

    if type(inspected) is not tuple or len(inspected) != 2:
        raise _failure(TrustedAssessmentSourceFailureCode.INTEGRITY_FAILURE)
    inspection, local_snapshot = inspected
    if type(inspection) is not LiveJournalInspectionReport:
        raise _failure(TrustedAssessmentSourceFailureCode.INTEGRITY_FAILURE)
    if inspection.account_id != account_id:
        raise _failure(TrustedAssessmentSourceFailureCode.INTEGRITY_FAILURE)

    if inspection.disposition is LiveJournalInspectionDisposition.SCHEMA_UPGRADE_REQUIRED:
        raise _failure(TrustedAssessmentSourceFailureCode.SCHEMA_UPGRADE_REQUIRED)
    if inspection.disposition is LiveJournalInspectionDisposition.ACCOUNT_NOT_FOUND:
        raise _failure(TrustedAssessmentSourceFailureCode.ACCOUNT_NOT_FOUND)
    if inspection.disposition is LiveJournalInspectionDisposition.BLOCKED_INTEGRITY_FAILURE:
        raise _failure(TrustedAssessmentSourceFailureCode.INTEGRITY_FAILURE)
    if inspection.disposition not in {
        LiveJournalInspectionDisposition.READY_NO_ACTION,
        LiveJournalInspectionDisposition.RECOVERY_REQUIRED,
    }:
        raise _failure(TrustedAssessmentSourceFailureCode.INTEGRITY_FAILURE)
    if type(local_snapshot) is not LocalReconciliationSnapshot:
        raise _failure(TrustedAssessmentSourceFailureCode.INTEGRITY_FAILURE)
    if (
        local_snapshot.account_id != account_id
        or local_snapshot.as_of != local_as_of
        or local_snapshot.journal_sequence != inspection.journal_sequence
    ):
        raise _failure(TrustedAssessmentSourceFailureCode.INTEGRITY_FAILURE)

    try:
        broker_snapshot = broker_snapshot_source.query_reconciliation_snapshot(account_id)
    except MemoryError:
        raise _failure(TrustedAssessmentSourceFailureCode.CAPACITY_EXCEEDED) from None
    except (Exception, KeyboardInterrupt, SystemExit):
        raise _failure(TrustedAssessmentSourceFailureCode.BROKER_SOURCE_FAILURE) from None
    if type(broker_snapshot) is not BrokerReconciliationSnapshot:
        raise _failure(TrustedAssessmentSourceFailureCode.MALFORMED_EVIDENCE)

    try:
        broker_account_id = _admit_broker_account_id(broker_snapshot)
    except MemoryError:
        raise _failure(TrustedAssessmentSourceFailureCode.CAPACITY_EXCEEDED) from None
    except (Exception, KeyboardInterrupt, SystemExit):
        raise _failure(TrustedAssessmentSourceFailureCode.MALFORMED_EVIDENCE) from None
    if broker_account_id != account_id:
        raise _failure(TrustedAssessmentSourceFailureCode.ACCOUNT_SCOPE_MISMATCH)

    try:
        observation_count = _admit_broker_observation_count(broker_snapshot)
    except MemoryError:
        raise _failure(TrustedAssessmentSourceFailureCode.CAPACITY_EXCEEDED) from None
    except (Exception, KeyboardInterrupt, SystemExit):
        raise _failure(TrustedAssessmentSourceFailureCode.MALFORMED_EVIDENCE) from None
    if observation_count > MAX_TRUSTED_BROKER_OBSERVATIONS:
        raise _failure(TrustedAssessmentSourceFailureCode.CAPACITY_EXCEEDED)

    try:
        broker_snapshot = _canonical_broker_snapshot(broker_snapshot)
        assert observation_count == (
            len(broker_snapshot.open_orders.orders)
            + len(broker_snapshot.fills.fills)
            + len(broker_snapshot.positions.positions)
        )
    except MemoryError:
        raise _failure(TrustedAssessmentSourceFailureCode.CAPACITY_EXCEEDED) from None
    except (Exception, KeyboardInterrupt, SystemExit):
        raise _failure(TrustedAssessmentSourceFailureCode.MALFORMED_EVIDENCE) from None

    reconciled_at = _clock_now(clock)
    try:
        assessment = assess_reconciliation(local_snapshot, broker_snapshot, reconciled_at)
        if type(assessment) is not ReconciliationAssessment:
            raise TypeError
        return InspectedReconciliationAssessment(inspection, assessment)
    except MemoryError:
        raise _failure(TrustedAssessmentSourceFailureCode.CAPACITY_EXCEEDED) from None
    except (TypeError, ValueError):
        raise _failure(TrustedAssessmentSourceFailureCode.MALFORMED_EVIDENCE) from None
    except (Exception, KeyboardInterrupt, SystemExit):
        raise _failure(TrustedAssessmentSourceFailureCode.INTERNAL_FAILURE) from None


__all__ = ["assess_sqlite_live_order_journal"]
