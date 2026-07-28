from __future__ import annotations

import hashlib
import io
from pathlib import Path

import pytest

import tx_trade.app.research_paper as research_paper
from tx_trade.app.research_output import encode_market_record
from tests.integration.test_research_paper_app import RUN_ID, _environment, _record
from tx_trade.app.research_paper import (
    ResearchPaperApplicationError,
    main,
    run_research_paper_app,
)
from tx_trade.research import ResearchRunStatus
from tx_trade.research.sqlite_repository import SQLiteResearchStateRepository
from tx_trade.storage import SQLiteMarketDataRepository


def _durable_environment(source: Path, state: Path, *, mode: str) -> dict[str, str]:
    values = _environment(source)
    values.update(
        {
            "TX_TRADE_RESEARCH_PAPER_RESTART_MODE": mode,
            "TX_TRADE_RESEARCH_PAPER_STATE_DB_PATH": str(state),
            "TX_TRADE_RESEARCH_PAPER_MAX_STATE_MAIN_DB_BYTES": str(16 * 1024 * 1024),
        }
    )
    return values


def test_create_and_completed_resume_match_disabled_bytes(tmp_path: Path) -> None:
    source = tmp_path / "source.sqlite3"
    state = tmp_path / "state.sqlite3"
    _record(source)
    source_before = hashlib.sha256(source.read_bytes()).digest()

    disabled = run_research_paper_app(_environment(source))
    created = run_research_paper_app(_durable_environment(source, state, mode="create"))
    resumed = run_research_paper_app(_durable_environment(source, state, mode="resume"))

    assert created.output == disabled.output == resumed.output
    assert created.broker_snapshot == resumed.broker_snapshot
    assert hashlib.sha256(source.read_bytes()).digest() == source_before
    assert not Path(f"{source}-wal").exists()
    assert not Path(f"{source}-shm").exists()


def test_commit_then_process_failure_resumes_from_durable_cursor(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.sqlite3"
    state = tmp_path / "interrupted.sqlite3"
    clean_state = tmp_path / "clean.sqlite3"
    _record(source)

    class CommitThenFail:
        def __init__(
            self,
            path: Path,
            *,
            max_main_database_bytes: int,
            create_new: bool,
            forbidden_file_identity: tuple[int, int],
        ) -> None:
            self._inner = SQLiteResearchStateRepository(
                path,
                max_main_database_bytes=max_main_database_bytes,
                create_new=create_new,
                forbidden_file_identity=forbidden_file_identity,
            )
            self._failed = False

        def __getattr__(self, name: str):
            return getattr(self._inner, name)

        def commit_batch(self, batch, committed_at):
            result = self._inner.commit_batch(batch, committed_at)
            if not self._failed:
                self._failed = True
                raise RuntimeError("simulated process termination")
            return result

    with pytest.raises(ResearchPaperApplicationError, match="failed safely"):
        run_research_paper_app(
            _durable_environment(source, state, mode="create"),
            state_repository_factory=CommitThenFail,
        )

    resumed = run_research_paper_app(_durable_environment(source, state, mode="resume"))
    clean = run_research_paper_app(_durable_environment(source, clean_state, mode="create"))
    assert resumed.output == clean.output


def test_output_cap_rejects_summary_before_completing_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.sqlite3"
    state = tmp_path / "bounded.sqlite3"
    _record(source)
    complete = run_research_paper_app(_environment(source))
    monkeypatch.setattr(research_paper, "_MAX_OUTPUT_BYTES", len(complete.output) - 1)

    with pytest.raises(ResearchPaperApplicationError, match="failed safely"):
        run_research_paper_app(_durable_environment(source, state, mode="create"))

    with SQLiteResearchStateRepository(
        state, max_main_database_bytes=16 * 1024 * 1024
    ) as repository:
        run_state = repository.load_run(RUN_ID).run_state
    assert run_state.status is ResearchRunStatus.ACTIVE
    assert run_state.committed_cursor == run_state.identity.source_last_sequence


def test_output_cap_rejects_paper_batch_without_advancing_that_cursor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.sqlite3"
    state = tmp_path / "paper-bounded.sqlite3"
    events = _record(source)
    market_bytes = sum(len(encode_market_record(event)) for event in events)
    monkeypatch.setattr(research_paper, "_MAX_OUTPUT_BYTES", market_bytes)

    with pytest.raises(ResearchPaperApplicationError, match="failed safely"):
        run_research_paper_app(_durable_environment(source, state, mode="create"))

    with SQLiteResearchStateRepository(
        state, max_main_database_bytes=16 * 1024 * 1024
    ) as repository:
        run_state = repository.load_run(RUN_ID).run_state
    assert run_state.status is ResearchRunStatus.ACTIVE
    assert run_state.committed_cursor is not None
    assert run_state.committed_cursor < run_state.identity.source_last_sequence


def test_source_guard_is_opened_before_repository_factory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.sqlite3"
    state = tmp_path / "guard-order.sqlite3"
    _record(source)
    guarded = False
    original_open = research_paper.os.open

    def tracked_open(path, flags, *args):
        nonlocal guarded
        descriptor = original_open(path, flags, *args)
        if Path(path) == source.resolve(strict=True):
            guarded = True
        return descriptor

    def source_factory(path, *, recover_incomplete_sessions, read_only):
        assert guarded
        return SQLiteMarketDataRepository(
            path,
            recover_incomplete_sessions=recover_incomplete_sessions,
            read_only=read_only,
        )

    monkeypatch.setattr(research_paper.os, "open", tracked_open)
    result = run_research_paper_app(
        _durable_environment(source, state, mode="create"),
        repository_factory=source_factory,
    )
    assert result.output


def test_flush_failure_leaves_completed_outbox_available_for_resume(tmp_path: Path, capsys) -> None:
    source = tmp_path / "source.sqlite3"
    state = tmp_path / "state.sqlite3"
    _record(source)

    class FlushFailure(io.BytesIO):
        def flush(self) -> None:
            raise OSError("simulated receiver failure")

    failed_output = FlushFailure()
    assert (
        main(
            [],
            environment=_durable_environment(source, state, mode="create"),
            output=failed_output,
        )
        == 2
    )
    assert failed_output.getvalue()
    assert capsys.readouterr().err == "Research paper replay failed safely.\n"

    resumed = io.BytesIO()
    assert (
        main(
            [],
            environment=_durable_environment(source, state, mode="resume"),
            output=resumed,
        )
        == 0
    )
    assert resumed.getvalue() == failed_output.getvalue()
