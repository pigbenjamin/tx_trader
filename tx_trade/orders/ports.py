"""Command, query, and event ports for paper execution."""

from __future__ import annotations

from typing import Protocol, TypeAlias, runtime_checkable
from uuid import UUID

from .contracts import (
    CancelIntent,
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

    def cancel(self, request: CancelIntent) -> OrderCommandResult: ...

    def get_order(self, paper_order_id: UUID) -> PaperOrder | None: ...

    def list_orders(self) -> tuple[PaperOrder, ...]: ...

    def list_positions(self) -> tuple[PaperPosition, ...]: ...


@runtime_checkable
class PaperEventSink(Protocol):
    def publish(self, event: PaperEvent) -> None: ...
