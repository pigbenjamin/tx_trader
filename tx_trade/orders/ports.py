"""Command, query, and event ports for paper execution."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, TypeAlias, runtime_checkable
from uuid import UUID

from .contracts import (
    OrderIntent,
    PaperEvent,
    PaperOrder,
    PaperPosition,
    PaperRejection,
)

OrderCommandResult: TypeAlias = PaperOrder | PaperRejection


@runtime_checkable
class PaperBrokerPort(Protocol):
    def submit(self, intent: OrderIntent) -> OrderCommandResult: ...

    def cancel(self, paper_order_id: UUID, requested_at: datetime) -> OrderCommandResult: ...

    def get_order(self, paper_order_id: UUID) -> PaperOrder | None: ...

    def list_orders(self) -> tuple[PaperOrder, ...]: ...

    def list_positions(self) -> tuple[PaperPosition, ...]: ...


@runtime_checkable
class PaperEventSink(Protocol):
    def publish(self, event: PaperEvent) -> None: ...
