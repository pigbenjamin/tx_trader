"""Storage-independent command and query port for durable research state."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable
from uuid import UUID

from .contracts import (
    CompleteResearchRun,
    ResearchDurableBatch,
    ResearchDurableBatchResult,
    ResearchHydrationState,
    ResearchOutboxRecord,
    ResearchRunIdentity,
    ResearchRunState,
    VersionedCheckpoint,
)


@runtime_checkable
class ResearchStateRepositoryPort(Protocol):
    def create_run(
        self,
        identity: ResearchRunIdentity,
        broker_checkpoint: VersionedCheckpoint,
        coordinator_checkpoint: VersionedCheckpoint,
        created_at: datetime,
    ) -> ResearchHydrationState: ...

    def load_run(self, paper_run_id: UUID) -> ResearchHydrationState: ...

    def commit_batch(
        self,
        batch: ResearchDurableBatch,
        committed_at: datetime,
    ) -> ResearchDurableBatchResult: ...

    def complete_run(self, request: CompleteResearchRun) -> ResearchRunState: ...

    def read_outbox(self, paper_run_id: UUID) -> tuple[ResearchOutboxRecord, ...]: ...

    def close(self) -> None: ...
