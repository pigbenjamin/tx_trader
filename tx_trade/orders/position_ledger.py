"""Pure signed-net position ledger transitions for paper fills."""

from __future__ import annotations

import json
from decimal import (
    Context,
    Decimal,
    DecimalException,
    Inexact,
    ROUND_HALF_EVEN,
    Rounded,
    localcontext,
)
from enum import StrEnum
from uuid import UUID, uuid5

from .contracts import ExecutionProvenance, OrderSide, PaperFill, PaperPosition

POSITION_LEDGER_SEMANTICS = "signed-net-v1-allow-short"
ALLOW_NET_SHORT = True


class PositionLedgerErrorCode(StrEnum):
    KEY_MISMATCH = "key_mismatch"
    CURRENCY_MISMATCH = "currency_mismatch"
    TEMPORAL_ORDER = "temporal_order"
    ARITHMETIC_FAILURE = "arithmetic_failure"


class PositionLedgerError(ValueError):
    """A fill cannot be applied to the supplied position."""

    def __init__(self, code: PositionLedgerErrorCode) -> None:
        self.code = code
        super().__init__(code.value)


def _context() -> Context:
    return Context(prec=34, rounding=ROUND_HALF_EVEN, Emin=-6143, Emax=6144, clamp=1)


def _exact_context() -> Context:
    context = _context()
    context.traps[Inexact] = True
    context.traps[Rounded] = True
    return context


def paper_position_id(
    paper_run_id: UUID,
    strategy_id: str,
    account_id: str,
    instrument_id: str,
) -> UUID:
    """Derive a collision-safe position identity from its typed canonical key."""

    if type(paper_run_id) is not UUID:
        raise TypeError("paper_run_id must be UUID")
    values = (strategy_id, account_id, instrument_id)
    if any(type(value) is not str or not value for value in values):
        raise ValueError("position key values must be non-empty strings")
    canonical_key = json.dumps(
        ["paper-position-v1", *values],
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return uuid5(paper_run_id, canonical_key)


def apply_fill_to_position(
    previous: PaperPosition | None,
    fill: PaperFill,
) -> PaperPosition:
    """Return the next immutable signed-net position state for one fill."""

    if previous is not None and type(previous) is not PaperPosition:
        raise TypeError("previous must be PaperPosition or None")
    if type(fill) is not PaperFill:
        raise TypeError("fill must be PaperFill")
    expected_id = paper_position_id(
        fill.paper_run_id,
        fill.strategy_id,
        fill.account_id,
        fill.instrument_id,
    )
    if previous is not None:
        if (
            previous.paper_run_id != fill.paper_run_id
            or previous.paper_position_id != expected_id
            or previous.strategy_id != fill.strategy_id
            or previous.account_id != fill.account_id
            or previous.instrument_id != fill.instrument_id
        ):
            raise PositionLedgerError(PositionLedgerErrorCode.KEY_MISMATCH)
        if previous.provenance is not ExecutionProvenance.PAPER:
            raise PositionLedgerError(PositionLedgerErrorCode.KEY_MISMATCH)
        if fill.occurred_at < previous.updated_at:
            raise PositionLedgerError(PositionLedgerErrorCode.TEMPORAL_ORDER)

    old_quantity = Decimal(0) if previous is None else previous.net_quantity
    old_average = None if previous is None else previous.average_open_price
    old_fees = Decimal(0) if previous is None else previous.cumulative_fees
    old_currency = None if previous is None else previous.fee_currency
    if fill.fee != 0 and old_currency is not None and old_currency != fill.fee_currency:
        raise PositionLedgerError(PositionLedgerErrorCode.CURRENCY_MISMATCH)

    try:
        delta = fill.quantity if fill.side is OrderSide.BUY else fill.quantity.copy_negate()
        with localcontext(_exact_context()):
            new_quantity = old_quantity + delta
            new_fees = old_fees if fill.fee == 0 else old_fees + fill.fee
        with localcontext(_context()):
            if old_quantity == 0:
                new_average = fill.execution_price
            elif (old_quantity > 0) == (delta > 0):
                assert old_average is not None
                new_average = (
                    abs(old_quantity) * old_average + abs(delta) * fill.execution_price
                ) / abs(new_quantity)
            elif new_quantity == 0:
                new_average = None
            elif (new_quantity > 0) == (old_quantity > 0):
                new_average = old_average
            else:
                new_average = fill.execution_price
    except DecimalException as exc:
        raise PositionLedgerError(PositionLedgerErrorCode.ARITHMETIC_FAILURE) from exc

    if not new_quantity.is_finite() or not new_fees.is_finite() or new_fees < 0:
        raise PositionLedgerError(PositionLedgerErrorCode.ARITHMETIC_FAILURE)
    if new_average is not None and (not new_average.is_finite() or new_average <= 0):
        raise PositionLedgerError(PositionLedgerErrorCode.ARITHMETIC_FAILURE)
    fee_currency = fill.fee_currency if fill.fee != 0 and old_currency is None else old_currency
    return PaperPosition(
        paper_run_id=fill.paper_run_id,
        paper_position_id=expected_id,
        strategy_id=fill.strategy_id,
        account_id=fill.account_id,
        instrument_id=fill.instrument_id,
        net_quantity=new_quantity,
        average_open_price=new_average,
        cumulative_fees=new_fees,
        fee_currency=fee_currency,
        version=1 if previous is None else previous.version + 1,
        updated_at=fill.occurred_at,
        provenance=ExecutionProvenance.PAPER,
    )
