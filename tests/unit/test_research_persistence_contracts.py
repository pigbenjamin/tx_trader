from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import datetime
from hashlib import sha256
from uuid import UUID
from zoneinfo import ZoneInfo

import pytest

from tx_trade.research import (
    CheckpointKind,
    CompleteResearchRun,
    DurableBatchDisposition,
    ResearchDurableBatch,
    ResearchDurableBatchResult,
    ResearchHydrationState,
    ResearchOutboxRecord,
    ResearchOutboxRecordType,
    ResearchPersistenceError,
    ResearchPersistenceErrorCode,
    ResearchRunIdentity,
    ResearchRunState,
    ResearchRunStatus,
    StrategyFingerprint,
    VersionedCheckpoint,
)

RUN_ID = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
SESSION_ID = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
TAIPEI = ZoneInfo("Asia/Taipei")
NOW = datetime(2026, 7, 28, 9, tzinfo=TAIPEI)
FP = "sha256:" + "a" * 64


def identity(**changes: object) -> ResearchRunIdentity:
    values: dict[str, object] = {
        "paper_run_id": RUN_ID,
        "source_session_id": SESSION_ID,
        "source_schema_version": 1,
        "source_event_count": 3,
        "source_first_sequence": 7,
        "source_last_sequence": 10,
        "source_content_fingerprint": FP,
        "research_config_fingerprint": "sha256:" + "b" * 64,
        "execution_config_fingerprint": "sha256:" + "c" * 64,
        "strategy_fingerprints": (
            StrategyFingerprint("alpha", "sha256:" + "d" * 64),
            StrategyFingerprint("beta", "sha256:" + "e" * 64),
        ),
        "output_schema_version": 1,
        "broker_algorithm_version": "paper-execution-v1",
    }
    values.update(changes)
    return ResearchRunIdentity(**values)  # type: ignore[arg-type]


def checkpoint(kind: CheckpointKind, payload: bytes = b"{}") -> VersionedCheckpoint:
    return VersionedCheckpoint.create(kind=kind, schema_version=1, payload=payload)


def outbox(
    sequence: int,
    record_type: ResearchOutboxRecordType,
    *,
    source: int | None,
    paper: int | None,
) -> ResearchOutboxRecord:
    return ResearchOutboxRecord.create(
        paper_run_id=RUN_ID,
        output_sequence=sequence,
        record_type=record_type,
        source_ingest_sequence=source,
        paper_sequence=paper,
        payload=b'{"schema_version":1}\n',
    )


def state(**changes: object) -> ResearchRunState:
    values: dict[str, object] = {
        "identity": identity(),
        "status": ResearchRunStatus.ACTIVE,
        "state_version": 1,
        "committed_cursor": 7,
        "committed_batch_count": 1,
        "created_at": NOW,
        "updated_at": NOW,
        "completed_at": None,
    }
    values.update(changes)
    return ResearchRunState(**values)  # type: ignore[arg-type]


def batch(**changes: object) -> ResearchDurableBatch:
    values: dict[str, object] = {
        "paper_run_id": RUN_ID,
        "expected_state_version": 0,
        "expected_previous_cursor": None,
        "source_session_id": SESSION_ID,
        "source_ingest_sequence": 7,
        "envelope_fingerprint": FP,
        "decision_fingerprint": "sha256:" + "f" * 64,
        "broker_checkpoint": checkpoint(CheckpointKind.BROKER),
        "coordinator_checkpoint": checkpoint(CheckpointKind.COORDINATOR),
        "outbox_records": (
            outbox(0, ResearchOutboxRecordType.MARKET, source=7, paper=None),
            outbox(1, ResearchOutboxRecordType.PAPER, source=7, paper=1),
        ),
    }
    values.update(changes)
    return ResearchDurableBatch(**values)  # type: ignore[arg-type]


def test_identity_is_frozen_domain_separated_and_semantically_sensitive() -> None:
    first = identity()
    assert first.identity_fingerprint.startswith("sha256:")
    assert first.identity_fingerprint == identity().identity_fingerprint
    assert first.identity_fingerprint != identity(output_schema_version=2).identity_fingerprint
    raw = sha256(b"").hexdigest()
    assert first.identity_fingerprint != f"sha256:{raw}"
    with pytest.raises(FrozenInstanceError):
        first.source_event_count = 9  # type: ignore[misc]


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"source_event_count": 5}, "span"),
        (
            {
                "strategy_fingerprints": (
                    StrategyFingerprint("beta", FP),
                    StrategyFingerprint("alpha", FP),
                )
            },
            "sorted and unique",
        ),
        ({"source_content_fingerprint": "A" * 64}, "canonical sha256"),
        ({"source_schema_version": True}, "integer"),
    ],
)
def test_identity_rejects_invalid_or_noncanonical_values(
    changes: dict[str, object], message: str
) -> None:
    with pytest.raises((TypeError, ValueError), match=message):
        identity(**changes)


def test_checkpoint_computes_digest_and_rejects_tampering_or_wrong_kind() -> None:
    value = checkpoint(CheckpointKind.BROKER, b'{"state":1}')
    assert value.payload_sha256.startswith("sha256:")
    with pytest.raises(ValueError, match="does not match"):
        replace(value, payload=b'{"state":2}')
    with pytest.raises(TypeError, match="CheckpointKind"):
        VersionedCheckpoint.create(kind="broker", schema_version=1, payload=b"x")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="payload size"):
        VersionedCheckpoint.create(kind=CheckpointKind.BROKER, schema_version=1, payload=b"")


@pytest.mark.parametrize(
    ("record_type", "source", "paper"),
    [
        (ResearchOutboxRecordType.MARKET, 7, None),
        (ResearchOutboxRecordType.PAPER, 7, 1),
        (ResearchOutboxRecordType.SUMMARY, None, None),
    ],
)
def test_outbox_types_have_strict_correlation_and_unchanged_payload(
    record_type: ResearchOutboxRecordType, source: int | None, paper: int | None
) -> None:
    value = outbox(0, record_type, source=source, paper=paper)
    assert value.payload == b'{"schema_version":1}\n'
    assert value.payload_sha256.startswith("sha256:")


def test_outbox_rejects_bad_shape_multiline_and_tampering() -> None:
    with pytest.raises(ValueError, match="require only"):
        outbox(0, ResearchOutboxRecordType.MARKET, source=7, paper=1)
    with pytest.raises(ValueError, match="newline-terminated"):
        ResearchOutboxRecord.create(
            paper_run_id=RUN_ID,
            output_sequence=0,
            record_type=ResearchOutboxRecordType.MARKET,
            source_ingest_sequence=7,
            paper_sequence=None,
            payload=b"{}\n{}\n",
        )
    with pytest.raises(ValueError, match="does not match"):
        replace(outbox(0, ResearchOutboxRecordType.MARKET, source=7, paper=None), payload=b"x\n")


def test_run_state_requires_monotonic_correlated_terminal_state() -> None:
    completed = state(
        status=ResearchRunStatus.COMPLETE,
        state_version=4,
        committed_cursor=10,
        committed_batch_count=3,
        completed_at=NOW,
    )
    assert completed.status is ResearchRunStatus.COMPLETE
    with pytest.raises(ValueError, match="terminal"):
        state(status=ResearchRunStatus.COMPLETE, completed_at=NOW)
    with pytest.raises(ValueError, match="advance together"):
        state(committed_cursor=None)
    with pytest.raises(ValueError, match="Asia/Taipei"):
        state(updated_at=datetime.fromisoformat("2026-07-28T09:00:00+00:00"))


def test_hydration_requires_checkpoint_roles() -> None:
    hydration = ResearchHydrationState(
        state(),
        checkpoint(CheckpointKind.BROKER),
        checkpoint(CheckpointKind.COORDINATOR),
    )
    assert hydration.run_state.identity.paper_run_id == RUN_ID
    with pytest.raises(ValueError, match="broker checkpoint"):
        replace(hydration, broker_checkpoint=checkpoint(CheckpointKind.COORDINATOR))


def test_durable_batch_is_atomic_correlated_ordered_and_excludes_summary() -> None:
    value = batch()
    assert value.outbox_records[0].record_type is ResearchOutboxRecordType.MARKET
    assert value.batch_fingerprint == batch().batch_fingerprint
    assert value.batch_fingerprint != batch(decision_fingerprint=FP).batch_fingerprint
    with pytest.raises(ValueError, match="exactly one"):
        batch(outbox_records=(outbox(1, ResearchOutboxRecordType.PAPER, source=7, paper=1),))
    with pytest.raises(ValueError, match="summary"):
        batch(
            outbox_records=(outbox(0, ResearchOutboxRecordType.SUMMARY, source=None, paper=None),)
        )
    with pytest.raises(ValueError, match="increasing output"):
        batch(
            outbox_records=(
                outbox(2, ResearchOutboxRecordType.MARKET, source=7, paper=None),
                outbox(1, ResearchOutboxRecordType.PAPER, source=7, paper=1),
            )
        )


def test_batch_result_duplicate_keeps_repository_returned_state_explicit() -> None:
    duplicate = ResearchDurableBatchResult(DurableBatchDisposition.DUPLICATE, state())
    assert duplicate.disposition is DurableBatchDisposition.DUPLICATE
    assert duplicate.run_state.state_version == 1


def test_complete_request_requires_optimistic_fence_and_summary() -> None:
    request = CompleteResearchRun(
        RUN_ID,
        expected_state_version=3,
        expected_previous_cursor=10,
        summary_record=outbox(9, ResearchOutboxRecordType.SUMMARY, source=None, paper=None),
        completed_at=NOW,
    )
    assert request.expected_state_version == 3
    with pytest.raises(ValueError, match="summary"):
        replace(
            request,
            summary_record=outbox(9, ResearchOutboxRecordType.MARKET, source=7, paper=None),
        )


@pytest.mark.parametrize("code", list(ResearchPersistenceErrorCode))
def test_persistence_errors_are_fixed_and_sanitized(
    code: ResearchPersistenceErrorCode,
) -> None:
    error = ResearchPersistenceError(code)
    assert error.code is code
    assert "payload" not in str(error)
    assert "sqlite" not in str(error).lower()
