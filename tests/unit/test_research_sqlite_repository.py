from __future__ import annotations

import sqlite3
import os
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from uuid import UUID
from zoneinfo import ZoneInfo

import pytest

from tx_trade.research.contracts import (
    CheckpointKind,
    CompleteResearchRun,
    DurableBatchDisposition,
    ResearchDurableBatch,
    ResearchOutboxRecord,
    ResearchOutboxRecordType,
    ResearchPersistenceError,
    ResearchPersistenceErrorCode,
    ResearchRunIdentity,
    ResearchRunStatus,
    StrategyFingerprint,
    VersionedCheckpoint,
)
from tx_trade.research.sqlite_repository import SQLiteResearchStateRepository

TAIPEI = ZoneInfo("Asia/Taipei")
NOW = datetime(2026, 7, 28, 12, 0, tzinfo=TAIPEI)
RUN_ID = UUID("11111111-1111-4111-8111-111111111111")
SESSION_ID = UUID("22222222-2222-4222-8222-222222222222")
FP = "sha256:" + "a" * 64


def identity() -> ResearchRunIdentity:
    return ResearchRunIdentity(
        paper_run_id=RUN_ID,
        source_session_id=SESSION_ID,
        source_schema_version=1,
        source_event_count=1,
        source_first_sequence=7,
        source_last_sequence=7,
        source_content_fingerprint=FP,
        research_config_fingerprint="sha256:" + "b" * 64,
        execution_config_fingerprint="sha256:" + "c" * 64,
        strategy_fingerprints=(StrategyFingerprint("alpha", "sha256:" + "d" * 64),),
        output_schema_version=1,
        broker_algorithm_version="paper-v1",
    )


def checkpoint(kind: CheckpointKind, payload: bytes = b"{}") -> VersionedCheckpoint:
    return VersionedCheckpoint.create(kind=kind, schema_version=1, payload=payload)


def record(
    sequence: int,
    record_type: ResearchOutboxRecordType,
    *,
    source: int | None,
    paper: int | None = None,
) -> ResearchOutboxRecord:
    return ResearchOutboxRecord.create(
        paper_run_id=RUN_ID,
        output_sequence=sequence,
        record_type=record_type,
        source_ingest_sequence=source,
        paper_sequence=paper,
        payload=b'{"ok":true}\n',
    )


def repository(path: Path) -> SQLiteResearchStateRepository:
    return SQLiteResearchStateRepository(
        path,
        max_main_database_bytes=16 * 1024 * 1024,
        create_new=not path.exists(),
    )


def create(repository: SQLiteResearchStateRepository) -> None:
    repository.create_run(
        identity(),
        checkpoint(CheckpointKind.BROKER),
        checkpoint(CheckpointKind.COORDINATOR),
        NOW,
    )


def batch(*, fingerprint: str = FP) -> ResearchDurableBatch:
    return ResearchDurableBatch(
        paper_run_id=RUN_ID,
        expected_state_version=0,
        expected_previous_cursor=None,
        source_session_id=SESSION_ID,
        source_ingest_sequence=7,
        envelope_fingerprint=fingerprint,
        decision_fingerprint="sha256:" + "e" * 64,
        broker_checkpoint=checkpoint(CheckpointKind.BROKER, b'{"v":1}'),
        coordinator_checkpoint=checkpoint(CheckpointKind.COORDINATOR, b'{"v":1}'),
        outbox_records=(record(0, ResearchOutboxRecordType.MARKET, source=7),),
    )


def test_create_load_commit_complete_and_read_outbox(tmp_path: Path) -> None:
    with repository(tmp_path / "state.sqlite3") as repo:
        create(repo)
        result = repo.commit_batch(batch(), NOW)
        assert result.disposition is DurableBatchDisposition.APPLIED
        assert result.run_state.committed_cursor == 7
        state = repo.complete_run(
            CompleteResearchRun(
                paper_run_id=RUN_ID,
                expected_state_version=1,
                expected_previous_cursor=7,
                summary_record=record(1, ResearchOutboxRecordType.SUMMARY, source=None),
                completed_at=NOW,
            )
        )
        assert state.status is ResearchRunStatus.COMPLETE
        assert tuple(item.output_sequence for item in repo.read_outbox(RUN_ID)) == (0, 1)


def test_duplicate_batch_is_idempotent(tmp_path: Path) -> None:
    with repository(tmp_path / "state.sqlite3") as repo:
        create(repo)
        first = repo.commit_batch(batch(), NOW)
        second = repo.commit_batch(batch(), NOW)
        assert first.run_state == second.run_state
        assert second.disposition is DurableBatchDisposition.DUPLICATE


def test_main_database_page_cap_excludes_wal_sidecar(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    limit = 16 * 1024 * 1024
    with repository(path) as repo:
        create(repo)
        reader = sqlite3.connect(path)
        try:
            reader.execute("BEGIN")
            reader.execute("SELECT COUNT(*) FROM research_runs").fetchone()
            repo.commit_batch(batch(), NOW)
            assert repo.max_main_database_bytes <= limit
            wal = path.with_name(path.name + "-wal")
            assert wal.exists()
            assert wal.stat().st_size > 0
        finally:
            reader.rollback()
            reader.close()


def test_sqlite_full_maps_to_capacity_and_rolls_back_batch(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with repository(path) as repo:
        create(repo)
    with sqlite3.connect(path) as inspector:
        page_size = inspector.execute("PRAGMA page_size").fetchone()[0]
        page_count = inspector.execute("PRAGMA page_count").fetchone()[0]
    constrained = SQLiteResearchStateRepository(
        path,
        max_main_database_bytes=page_size * page_count,
    )
    try:
        large_market = ResearchOutboxRecord.create(
            paper_run_id=RUN_ID,
            output_sequence=0,
            record_type=ResearchOutboxRecordType.MARKET,
            source_ingest_sequence=7,
            paper_sequence=None,
            payload=b"x" * 100_000 + b"\n",
        )
        oversized = replace(batch(), outbox_records=(large_market,))
        before = constrained.load_run(RUN_ID)
        with pytest.raises(ResearchPersistenceError) as caught:
            constrained.commit_batch(oversized, NOW)
        assert caught.value.code is ResearchPersistenceErrorCode.CAPACITY
    finally:
        constrained.close()
    with SQLiteResearchStateRepository(path, max_main_database_bytes=16 * 1024 * 1024) as reopened:
        assert reopened.load_run(RUN_ID) == before


def test_same_sequence_different_batch_conflicts(tmp_path: Path) -> None:
    with repository(tmp_path / "state.sqlite3") as repo:
        create(repo)
        repo.commit_batch(batch(), NOW)
        with pytest.raises(ResearchPersistenceError) as caught:
            repo.commit_batch(batch(fingerprint="sha256:" + "f" * 64), NOW)
        assert caught.value.code is ResearchPersistenceErrorCode.CONFLICT


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("broker_checkpoint", b'{"tampered":true}'),
        ("broker_checkpoint_sha256", FP),
        ("coordinator_checkpoint", b'{"tampered":true}'),
        ("coordinator_checkpoint_sha256", FP),
        ("identity_fingerprint", FP),
    ],
)
def test_load_run_rejects_corrupt_identity_and_checkpoint_material(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    path = tmp_path / "state.sqlite3"
    with repository(path) as repo:
        create(repo)
    with sqlite3.connect(path) as connection:
        connection.execute(
            f"UPDATE research_runs SET {field}=? WHERE paper_run_id=?",
            (value, str(RUN_ID)),
        )

    with repository(path) as repo:
        with pytest.raises(ResearchPersistenceError) as caught:
            repo.load_run(RUN_ID)
        assert caught.value.code is ResearchPersistenceErrorCode.CORRUPT


def test_load_run_maps_deeply_nested_strategy_json_to_corrupt(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with repository(path) as repo:
        create(repo)
    nested = "[" * 2_000 + "0" + "]" * 2_000
    with sqlite3.connect(path) as connection:
        connection.execute(
            """UPDATE research_runs SET strategy_fingerprints_json=?
            WHERE paper_run_id=?""",
            (nested, str(RUN_ID)),
        )

    with repository(path) as repo:
        with pytest.raises(ResearchPersistenceError) as caught:
            repo.load_run(RUN_ID)
    assert caught.value.code is ResearchPersistenceErrorCode.CORRUPT
    assert caught.value.__cause__ is None


@pytest.mark.parametrize(
    ("pragma", "value"),
    [("application_id", 0), ("user_version", 2)],
)
def test_open_rejects_wrong_or_newer_schema_metadata(
    tmp_path: Path, pragma: str, value: int
) -> None:
    path = tmp_path / "state.sqlite3"
    with repository(path):
        pass
    with sqlite3.connect(path) as connection:
        connection.execute(f"PRAGMA {pragma}={value}")

    with pytest.raises(ResearchPersistenceError) as caught:
        repository(path)
    assert caught.value.code is ResearchPersistenceErrorCode.SCHEMA_MISMATCH


def test_open_rejects_schema_checksum_mismatch(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with repository(path):
        pass
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE paper_schema_migrations SET checksum=? WHERE version=1",
            (FP,),
        )

    with pytest.raises(ResearchPersistenceError) as caught:
        repository(path)
    assert caught.value.code is ResearchPersistenceErrorCode.SCHEMA_MISMATCH


def test_open_rejects_same_named_weakened_index(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with repository(path):
        pass
    with sqlite3.connect(path) as connection:
        connection.execute("DROP INDEX idx_research_outbox_single_summary")
        connection.execute(
            """CREATE INDEX idx_research_outbox_single_summary
            ON research_outbox(paper_run_id)"""
        )

    with pytest.raises(ResearchPersistenceError) as caught:
        repository(path)
    assert caught.value.code is ResearchPersistenceErrorCode.SCHEMA_MISMATCH


def test_stale_expected_version_rolls_back_without_mutation(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with repository(path) as repo:
        create(repo)
        stale = replace(batch(), expected_state_version=1)
        before = repo.load_run(RUN_ID)
        with pytest.raises(ResearchPersistenceError) as caught:
            repo.commit_batch(stale, NOW)
        assert caught.value.code is ResearchPersistenceErrorCode.CONFLICT
        assert repo.load_run(RUN_ID) == before


def test_load_run_rejects_batch_ledger_gap(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with repository(path) as repo:
        create(repo)
        repo.commit_batch(batch(), NOW)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "DELETE FROM research_batches WHERE paper_run_id=?",
            (str(RUN_ID),),
        )

    with repository(path) as repo:
        with pytest.raises(ResearchPersistenceError) as caught:
            repo.load_run(RUN_ID)
        assert caught.value.code is ResearchPersistenceErrorCode.CORRUPT


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("payload", b'{"no":true}\n'),
        ("payload_sha256", FP),
        ("output_sequence", 9),
    ],
)
def test_read_outbox_rejects_payload_digest_and_sequence_corruption(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    path = tmp_path / "state.sqlite3"
    with repository(path) as repo:
        create(repo)
        repo.commit_batch(batch(), NOW)
        repo.complete_run(
            CompleteResearchRun(
                paper_run_id=RUN_ID,
                expected_state_version=1,
                expected_previous_cursor=7,
                summary_record=record(1, ResearchOutboxRecordType.SUMMARY, source=None),
                completed_at=NOW,
            )
        )
    with sqlite3.connect(path) as connection:
        connection.execute(
            f"UPDATE research_outbox SET {field}=? WHERE paper_run_id=? AND record_type='market'",
            (value, str(RUN_ID)),
        )

    with repository(path) as repo:
        with pytest.raises(ResearchPersistenceError) as caught:
            repo.read_outbox(RUN_ID)
        assert caught.value.code is ResearchPersistenceErrorCode.CORRUPT


def test_existing_empty_database_is_not_repaired(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    path.touch()
    with pytest.raises(ResearchPersistenceError) as caught:
        repository(path)
    assert caught.value.code is ResearchPersistenceErrorCode.SCHEMA_MISMATCH


def test_create_new_rejects_existing_target_before_sqlite_connect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "state.sqlite3"
    path.touch()
    called = False

    def forbidden_connect(*args: object, **kwargs: object) -> object:
        nonlocal called
        called = True
        raise AssertionError

    monkeypatch.setattr("tx_trade.research.sqlite_repository.sqlite3.connect", forbidden_connect)
    with pytest.raises(ResearchPersistenceError) as caught:
        SQLiteResearchStateRepository(
            path, max_main_database_bytes=16 * 1024 * 1024, create_new=True
        )
    assert caught.value.code is ResearchPersistenceErrorCode.ALREADY_EXISTS
    assert not called


def test_create_new_identity_race_fails_before_writable_pragma(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "state.sqlite3"
    original = SQLiteResearchStateRepository._verify_reserved_identity
    calls = 0

    def fail_second_verification(self: SQLiteResearchStateRepository, descriptor: int) -> None:
        nonlocal calls
        calls += 1
        original(self, descriptor)
        if calls == 2:
            raise ResearchPersistenceError(ResearchPersistenceErrorCode.CONFLICT)

    monkeypatch.setattr(
        SQLiteResearchStateRepository,
        "_verify_reserved_identity",
        fail_second_verification,
    )
    with pytest.raises(ResearchPersistenceError) as caught:
        repository(path)
    assert caught.value.code is ResearchPersistenceErrorCode.CONFLICT
    assert not path.with_name(path.name + "-wal").exists()
    connection = sqlite3.connect(path)
    try:
        assert (
            connection.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").fetchone()[
                0
            ]
            == 0
        )
    finally:
        connection.close()


def test_constructor_maps_sqlite_full_to_capacity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FullConnection:
        row_factory: object = None

        def execute(self, statement: str) -> object:
            error = sqlite3.OperationalError("full")
            error.sqlite_errorcode = sqlite3.SQLITE_FULL
            raise error

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        "tx_trade.research.sqlite_repository.sqlite3.connect",
        lambda *args, **kwargs: FullConnection(),
    )
    with pytest.raises(ResearchPersistenceError) as caught:
        SQLiteResearchStateRepository(
            tmp_path / "state.sqlite3",
            max_main_database_bytes=16 * 1024 * 1024,
            create_new=True,
        )
    assert caught.value.code is ResearchPersistenceErrorCode.CAPACITY


def test_resume_rejects_symlink(tmp_path: Path) -> None:
    target = tmp_path / "target.sqlite3"
    target.touch()
    link = tmp_path / "link.sqlite3"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("symlinks are unavailable")
    with pytest.raises(ResearchPersistenceError) as caught:
        SQLiteResearchStateRepository(link, max_main_database_bytes=16 * 1024 * 1024)
    assert caught.value.code is ResearchPersistenceErrorCode.CONFLICT


def test_source_hardlink_identity_is_rejected_before_sqlite_connect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.sqlite3"
    original = b"immutable source bytes"
    source.write_bytes(original)
    state = tmp_path / "state.sqlite3"
    os.link(source, state)
    source_stat = os.stat(source)
    called = False

    def forbidden_connect(*args: object, **kwargs: object) -> object:
        nonlocal called
        called = True
        raise AssertionError

    monkeypatch.setattr("tx_trade.research.sqlite_repository.sqlite3.connect", forbidden_connect)
    with pytest.raises(ResearchPersistenceError) as caught:
        SQLiteResearchStateRepository(
            state,
            max_main_database_bytes=16 * 1024 * 1024,
            forbidden_file_identity=(source_stat.st_dev, source_stat.st_ino),
        )
    assert caught.value.code is ResearchPersistenceErrorCode.CONFLICT
    assert not called
    assert source.read_bytes() == original
    assert not source.with_name(source.name + "-wal").exists()
    assert not source.with_name(source.name + "-shm").exists()


def test_close_is_idempotent_and_operations_fail_closed(tmp_path: Path) -> None:
    repo = repository(tmp_path / "state.sqlite3")
    repo.close()
    repo.close()
    with pytest.raises(ResearchPersistenceError) as caught:
        repo.load_run(RUN_ID)
    assert caught.value.code is ResearchPersistenceErrorCode.CLOSED


def test_memory_database_is_rejected() -> None:
    with pytest.raises(ValueError, match="in-memory"):
        SQLiteResearchStateRepository(":memory:", max_main_database_bytes=4096)
