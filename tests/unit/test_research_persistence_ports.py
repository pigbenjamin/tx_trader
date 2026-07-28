from __future__ import annotations

from datetime import datetime
from typing import get_type_hints
from uuid import UUID

from tx_trade.research import ResearchStateRepositoryPort


class StructurallyCompleteRepository:
    def create_run(self, identity, broker_checkpoint, coordinator_checkpoint, created_at):
        raise NotImplementedError

    def load_run(self, paper_run_id):
        raise NotImplementedError

    def commit_batch(self, batch, committed_at):
        raise NotImplementedError

    def complete_run(self, request):
        raise NotImplementedError

    def read_outbox(self, paper_run_id):
        raise NotImplementedError

    def close(self):
        return None


def test_repository_port_is_runtime_checkable_and_storage_agnostic() -> None:
    assert isinstance(StructurallyCompleteRepository(), ResearchStateRepositoryPort)
    annotations = get_type_hints(ResearchStateRepositoryPort.create_run)
    assert annotations["created_at"] is datetime
    assert get_type_hints(ResearchStateRepositoryPort.load_run)["paper_run_id"] is UUID
    assert "sqlite" not in repr(annotations).lower()


def test_repository_port_rejects_missing_atomic_commit_operation() -> None:
    class QueryOnlyRepository:
        def load_run(self, paper_run_id):
            raise NotImplementedError

        def read_outbox(self, paper_run_id):
            return ()

        def close(self):
            return None

    assert not isinstance(QueryOnlyRepository(), ResearchStateRepositoryPort)
