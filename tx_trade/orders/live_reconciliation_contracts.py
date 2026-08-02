"""Immutable, side-effect-free contracts for fake-only live reconciliation."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
import re
from typing import Iterable, Protocol, TypeVar, runtime_checkable

from .live_contracts import LiveFill, LiveOrder, LiveSide, StrategyPositionAttribution
from .live_ports import (
    BrokerFillsSnapshot,
    BrokerPositionsSnapshot,
    OpenOrdersSnapshot,
    ReconciliationResult,
)

_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
_T = TypeVar("_T")
_MAX_DECIMAL_DIGITS = 34
_MIN_DECIMAL_EXPONENT = -6143
_MAX_DECIMAL_EXPONENT = 6144


def exact_decimal_sum(values: Iterable[Decimal]) -> Decimal:
    """Return an exact, deterministic sum without consulting decimal context."""

    total = 0
    common_exponent: int | None = None
    for value in values:
        if type(value) is not Decimal:
            raise TypeError("values must contain Decimal values")
        if not value.is_finite():
            raise ValueError("values must contain finite Decimal values")
        decimal_tuple = value.as_tuple()
        exponent = decimal_tuple.exponent
        if (
            len(decimal_tuple.digits) > _MAX_DECIMAL_DIGITS
            or not isinstance(exponent, int)
            or not _MIN_DECIMAL_EXPONENT <= exponent <= _MAX_DECIMAL_EXPONENT
        ):
            raise ValueError("value exceeds supported Decimal bounds")

        coefficient = 0
        for digit in decimal_tuple.digits:
            coefficient = coefficient * 10 + digit
        if decimal_tuple.sign:
            coefficient = -coefficient
        if common_exponent is None:
            common_exponent = exponent
        elif exponent < common_exponent:
            total *= 10 ** (common_exponent - exponent)
            common_exponent = exponent
        else:
            coefficient *= 10 ** (exponent - common_exponent)
        total += coefficient

    if total == 0:
        return Decimal(0)
    assert common_exponent is not None

    sign = int(total < 0)
    coefficient = abs(total)
    while coefficient % 10 == 0:
        coefficient //= 10
        common_exponent += 1
    if (
        coefficient >= 10**_MAX_DECIMAL_DIGITS
        or not _MIN_DECIMAL_EXPONENT <= common_exponent <= _MAX_DECIMAL_EXPONENT
    ):
        raise ValueError("exact sum exceeds supported Decimal bounds")

    reversed_digits: list[int] = []
    while coefficient:
        coefficient, digit = divmod(coefficient, 10)
        reversed_digits.append(digit)
    return Decimal((sign, tuple(reversed(reversed_digits)), common_exponent))


def _require_identifier(value: object, name: str) -> None:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"{name} must be a bounded ASCII identifier")


def _require_utc(value: object, name: str) -> None:
    if type(value) is not datetime:
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware UTC")
    if value.utcoffset() != timezone.utc.utcoffset(value):
        raise ValueError(f"{name} must use UTC")


def _require_exact_tuple(
    values: object,
    expected_type: type[_T],
    name: str,
) -> tuple[_T, ...]:
    if type(values) is not tuple:
        raise TypeError(f"{name} must be a tuple")
    if any(type(item) is not expected_type for item in values):
        raise TypeError(f"{name} must contain {expected_type.__name__} values")
    return values


@dataclass(frozen=True, slots=True)
class LocalReconciliationSnapshot:
    """One account's durable local state at a single upper time bound."""

    account_id: str = field(repr=False)
    orders: tuple[LiveOrder, ...]
    fills: tuple[LiveFill, ...]
    position_attributions: tuple[StrategyPositionAttribution, ...]
    as_of: datetime
    recovery_blockers: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_identifier(self.account_id, "account_id")
        orders = _require_exact_tuple(self.orders, LiveOrder, "orders")
        fills = _require_exact_tuple(self.fills, LiveFill, "fills")
        attributions = _require_exact_tuple(
            self.position_attributions,
            StrategyPositionAttribution,
            "position_attributions",
        )
        _require_utc(self.as_of, "as_of")
        blockers = _require_exact_tuple(self.recovery_blockers, str, "recovery_blockers")
        for blocker in blockers:
            _require_identifier(blocker, "recovery_blocker")
        if len(set(blockers)) != len(blockers):
            raise ValueError("recovery_blockers must be unique")

        if any(order.intent.account_id != self.account_id for order in orders):
            raise ValueError("orders must match snapshot account")
        if any(fill.account_id != self.account_id for fill in fills):
            raise ValueError("fills must match snapshot account")
        if any(item.account_id != self.account_id for item in attributions):
            raise ValueError("position attributions must match snapshot account")

        order_ids = tuple(order.intent.client_order_id for order in orders)
        if len(set(order_ids)) != len(order_ids):
            raise ValueError("order client_order_ids must be unique")
        fill_ids = tuple(fill.fill_id for fill in fills)
        if len(set(fill_ids)) != len(fill_ids):
            raise ValueError("fill_ids must be unique")
        attribution_keys = tuple((item.strategy_id, item.instrument_id) for item in attributions)
        if len(set(attribution_keys)) != len(attribution_keys):
            raise ValueError("position attribution keys must be unique")

        item_times = (
            *(order.updated_at for order in orders),
            *(fill.occurred_at for fill in fills),
            *(item.as_of for item in attributions),
        )
        if any(item_time > self.as_of for item_time in item_times):
            raise ValueError("local items must not postdate as_of")

        orders_by_id = {order.intent.client_order_id: order for order in orders}
        fills_by_order: dict[str, list[Decimal]] = {order_id: [] for order_id in order_ids}
        projected: dict[tuple[str, str], list[Decimal]] = {}
        for fill in fills:
            order = orders_by_id.get(fill.client_order_id)
            if order is None:
                raise ValueError("each fill client_order_id must reference an order")
            intent = order.intent
            if (
                fill.account_id != intent.account_id
                or fill.strategy_id != intent.strategy_id
                or fill.instrument_id != intent.instrument_id
                or fill.side is not intent.side
            ):
                raise ValueError("fill fields must match referenced order intent")
            fills_by_order[fill.client_order_id].append(fill.quantity)
            signed_quantity = (
                fill.quantity if fill.side is LiveSide.BUY else fill.quantity.copy_negate()
            )
            projected.setdefault((fill.strategy_id, fill.instrument_id), []).append(signed_quantity)

        for order_id, order in orders_by_id.items():
            if exact_decimal_sum(fills_by_order[order_id]) != order.filled_quantity:
                raise ValueError("fill quantity sum must equal order filled_quantity")

        exact_projection = {
            key: quantity
            for key, quantities in projected.items()
            if (quantity := exact_decimal_sum(quantities)) != 0
        }
        durable_attributions = {
            (item.strategy_id, item.instrument_id): item.attributed_quantity
            for item in attributions
        }
        if durable_attributions != exact_projection:
            raise ValueError("position attributions must equal durable fill projection")


@dataclass(frozen=True, slots=True)
class BrokerReconciliationSnapshot:
    """Coherent broker query bundle captured for one account."""

    snapshot_id: str
    account_id: str = field(repr=False)
    open_orders: OpenOrdersSnapshot
    fills: BrokerFillsSnapshot
    positions: BrokerPositionsSnapshot
    captured_at: datetime

    def __post_init__(self) -> None:
        _require_identifier(self.snapshot_id, "snapshot_id")
        _require_identifier(self.account_id, "account_id")
        if type(self.open_orders) is not OpenOrdersSnapshot:
            raise TypeError("open_orders must be OpenOrdersSnapshot")
        if type(self.fills) is not BrokerFillsSnapshot:
            raise TypeError("fills must be BrokerFillsSnapshot")
        if type(self.positions) is not BrokerPositionsSnapshot:
            raise TypeError("positions must be BrokerPositionsSnapshot")
        _require_utc(self.captured_at, "captured_at")

        evidence = (
            self.open_orders.evidence,
            self.fills.evidence,
            self.positions.evidence,
        )
        if any(item.account_id != self.account_id for item in evidence):
            raise ValueError("broker snapshots must match snapshot account")
        if any(item.observed_at > self.captured_at for item in evidence):
            raise ValueError("broker evidence must not postdate captured_at")
        if any(item.source_cursor != self.snapshot_id for item in evidence):
            raise ValueError("broker evidence source_cursor must equal snapshot_id")

        if any(item.account_id != self.account_id for item in self.open_orders.orders):
            raise ValueError("broker observations must match snapshot account")
        if any(item.account_id != self.account_id for item in self.fills.fills):
            raise ValueError("broker observations must match snapshot account")
        if any(item.account_id != self.account_id for item in self.positions.positions):
            raise ValueError("broker observations must match snapshot account")
        if any(item.observed_at > self.captured_at for item in self.open_orders.orders):
            raise ValueError("broker observations must not postdate captured_at")
        if any(item.observed_at > self.captured_at for item in self.fills.fills):
            raise ValueError("broker observations must not postdate captured_at")
        if any(item.observed_at > self.captured_at for item in self.positions.positions):
            raise ValueError("broker observations must not postdate captured_at")


@runtime_checkable
class BrokerReconciliationSnapshotSourcePort(Protocol):
    """Source-attested coherent cut returned atomically by one broker-source call.

    The source, rather than the reconciliation service, creates ``snapshot_id``
    and the matching evidence ``source_cursor`` values.
    """

    def query_reconciliation_snapshot(self, account_id: str) -> BrokerReconciliationSnapshot: ...


@dataclass(frozen=True, slots=True)
class ReconciliationAssessment:
    """Decision evidence; this contract never grants or performs dispatch."""

    local_snapshot: LocalReconciliationSnapshot
    broker_snapshot: BrokerReconciliationSnapshot
    result: ReconciliationResult

    def __post_init__(self) -> None:
        if type(self.local_snapshot) is not LocalReconciliationSnapshot:
            raise TypeError("local_snapshot must be LocalReconciliationSnapshot")
        if type(self.broker_snapshot) is not BrokerReconciliationSnapshot:
            raise TypeError("broker_snapshot must be BrokerReconciliationSnapshot")
        if type(self.result) is not ReconciliationResult:
            raise TypeError("result must be ReconciliationResult")

        account_id = self.local_snapshot.account_id
        if self.broker_snapshot.account_id != account_id or self.result.account_id != account_id:
            raise ValueError("local, broker, and result accounts must match")
        if self.result.reconciled_at < self.local_snapshot.as_of:
            raise ValueError("reconciliation must not predate local snapshot")
        if self.result.reconciled_at < self.broker_snapshot.captured_at:
            raise ValueError("reconciliation must not predate broker snapshot")

        broker_evidence = (
            self.broker_snapshot.open_orders.evidence,
            self.broker_snapshot.fills.evidence,
            self.broker_snapshot.positions.evidence,
        )
        if any(item not in self.result.evidence for item in broker_evidence):
            raise ValueError("result evidence must include broker snapshot evidence")
        if any(item.observed_at < self.local_snapshot.as_of for item in broker_evidence):
            raise ValueError("broker evidence must not predate local snapshot")

    @property
    def may_resume(self) -> bool:
        return (
            self.result.is_authoritative
            and not self.result.discrepancies
            and not self.local_snapshot.recovery_blockers
        )

    @property
    def may_dispatch(self) -> bool:
        """Reconciliation evidence alone never authorizes a broker side effect."""

        return False


@runtime_checkable
class LocalReconciliationSourcePort(Protocol):
    def load_account_snapshot(self, account_id: str) -> LocalReconciliationSnapshot: ...
