"""Pure contracts for authoritative reconciliation order projection plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
import re
from typing import TypeVar

from .live_contracts import LiveOrder
from .live_reconciliation_commit_contracts import ExpectedOrderVersion

_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
_T = TypeVar("_T")


def _identifier(value: object, name: str) -> None:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"{name} must be a bounded ASCII identifier")


def _nonnegative_int(value: object, name: str) -> None:
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    if value < 0:
        raise ValueError(f"{name} must be nonnegative")


def _exact_tuple(values: object, expected: type[_T], name: str) -> tuple[_T, ...]:
    if type(values) is not tuple:
        raise TypeError(f"{name} must be a tuple")
    if any(type(item) is not expected for item in values):
        raise TypeError(f"{name} must contain exact {expected.__name__} values")
    return values


class OrderProjectionDisposition(StrEnum):
    NO_CHANGE = "no_change"
    READY = "ready"
    NOT_AUTHORITATIVE = "not_authoritative"
    UNSUPPORTED = "unsupported"


class OrderProjectionReason(StrEnum):
    ASSESSMENT_MISMATCH = "assessment_mismatch"
    NOT_AUTHORITATIVE = "not_authoritative"
    INCOMPLETE_EVIDENCE = "incomplete_evidence"
    AMBIGUOUS_EVIDENCE = "ambiguous_evidence"
    UNSUPPORTED_DISCREPANCY = "unsupported_discrepancy"
    UNSUPPORTED_LOCAL_STATE = "unsupported_local_state"
    UNSUPPORTED_COMMAND = "unsupported_command"
    MISSING_BROKER_MATCH = "missing_broker_match"
    MULTIPLE_BROKER_MATCHES = "multiple_broker_matches"
    BROKER_IDENTITY_MISMATCH = "broker_identity_mismatch"
    BROKER_QUANTITY_MISMATCH = "broker_quantity_mismatch"
    BROKER_PRICE_MISMATCH = "broker_price_mismatch"
    BROKER_TIME_MISMATCH = "broker_time_mismatch"
    FILL_EVIDENCE_UNSUPPORTED = "fill_evidence_unsupported"


@dataclass(frozen=True, slots=True)
class AuthoritativeOrderProjectionPlan:
    account_id: str = field(repr=False)
    snapshot_id: str
    expected_journal_sequence: int
    disposition: OrderProjectionDisposition
    expected_order_versions: tuple[ExpectedOrderVersion, ...] = ()
    projected_orders: tuple[LiveOrder, ...] = field(default=(), repr=False)
    consumed_discrepancy_ids: tuple[str, ...] = ()
    unsupported_discrepancy_ids: tuple[str, ...] = ()
    reasons: tuple[OrderProjectionReason, ...] = ()

    def __post_init__(self) -> None:
        _identifier(self.account_id, "account_id")
        _identifier(self.snapshot_id, "snapshot_id")
        _nonnegative_int(self.expected_journal_sequence, "expected_journal_sequence")
        if type(self.disposition) is not OrderProjectionDisposition:
            raise TypeError("disposition must be OrderProjectionDisposition")

        versions = _exact_tuple(
            self.expected_order_versions,
            ExpectedOrderVersion,
            "expected_order_versions",
        )
        projections = _exact_tuple(self.projected_orders, LiveOrder, "projected_orders")
        consumed = _exact_tuple(self.consumed_discrepancy_ids, str, "consumed_discrepancy_ids")
        unsupported = _exact_tuple(
            self.unsupported_discrepancy_ids,
            str,
            "unsupported_discrepancy_ids",
        )
        reasons = _exact_tuple(self.reasons, OrderProjectionReason, "reasons")
        for value in (*consumed, *unsupported):
            _identifier(value, "discrepancy_id")

        version_ids = tuple(item.client_order_id for item in versions)
        projection_ids = tuple(item.intent.client_order_id for item in projections)
        canonical_groups: tuple[tuple[str, ...], ...] = (
            version_ids,
            projection_ids,
            consumed,
            unsupported,
            tuple(item.value for item in reasons),
        )
        if any(len(set(values)) != len(values) for values in canonical_groups):
            raise ValueError("plan values must be unique")
        if any(tuple(sorted(values)) != values for values in canonical_groups):
            raise ValueError("plan values must use canonical sorted ordering")
        if set(consumed) & set(unsupported):
            raise ValueError("consumed and unsupported discrepancy IDs must be disjoint")
        if any(item.intent.account_id != self.account_id for item in projections):
            raise ValueError("projected orders must match plan account")
        expected_by_id = {item.client_order_id: item.version for item in versions}
        if set(expected_by_id) != set(projection_ids):
            raise ValueError("expected versions and projections must map one-to-one")
        if any(
            item.version != expected_by_id[item.intent.client_order_id] + 1 for item in projections
        ):
            raise ValueError("projected versions must advance expected versions exactly once")

        if self.disposition is OrderProjectionDisposition.READY:
            if not projections or not consumed or unsupported or reasons:
                raise ValueError("READY requires projections and consumed discrepancies only")
        elif self.disposition is OrderProjectionDisposition.NO_CHANGE:
            if versions or projections or consumed or unsupported or reasons:
                raise ValueError("NO_CHANGE must not contain projection decision data")
        else:
            if versions or projections or consumed or not reasons:
                raise ValueError("rejected plans require reasons and no projections")
            if self.disposition is OrderProjectionDisposition.NOT_AUTHORITATIVE and unsupported:
                raise ValueError("NOT_AUTHORITATIVE must not classify unsupported discrepancies")

    @property
    def may_commit(self) -> bool:
        return self.disposition is OrderProjectionDisposition.READY

    @property
    def may_dispatch(self) -> bool:
        return False


__all__ = [
    "AuthoritativeOrderProjectionPlan",
    "OrderProjectionDisposition",
    "OrderProjectionReason",
]
