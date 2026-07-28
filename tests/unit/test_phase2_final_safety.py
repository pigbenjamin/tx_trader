from __future__ import annotations

import os
import subprocess
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from uuid import UUID

import pytest

import tx_trade.app.research_paper as research_paper
from tx_trade.app.research_paper import (
    ResearchPaperApplicationError,
    _open_repository,
    run_research_paper,
)
from tx_trade.app.research_paper_config import (
    ResearchRestartMode,
    parse_research_paper_settings,
)
from tx_trade.market_data.fixtures import (
    OFFLINE_FIXTURE_TIME,
    OFFLINE_FIXTURE_TRADING_DAY,
    make_offline_fixture_envelopes,
)
from tx_trade.market_data.models import SCHEMA_VERSION, SourceMode
from tx_trade.market_data.ports import RecordingSession
from tx_trade.storage import SQLiteMarketDataRepository

SESSION_ID = UUID("11111111-1111-1111-1111-111111111111")
RUN_ID = UUID("22222222-2222-2222-2222-222222222222")


def _settings(database_path: Path):
    values = {
        "TX_TRADE_RUNTIME_PRESET": "research_paper",
        "TX_TRADE_RESEARCH_PAPER_DB_PATH": str(database_path),
        "TX_TRADE_RESEARCH_PAPER_SESSION_ID": str(SESSION_ID),
        "TX_TRADE_RESEARCH_PAPER_REPLAY_MODE": "fastest",
        "TX_TRADE_RESEARCH_PAPER_REPLAY_SPEED": "1",
        "TX_TRADE_RESEARCH_PAPER_RUN_ID": str(RUN_ID),
        "TX_TRADE_RESEARCH_PAPER_MAX_ORDERS": "10",
        "TX_TRADE_RESEARCH_PAPER_MAX_OPEN_ORDERS": "10",
        "TX_TRADE_RESEARCH_PAPER_MAX_FILLS": "10",
        "TX_TRADE_RESEARCH_PAPER_MAX_EVENTS": "30",
        "TX_TRADE_RESEARCH_PAPER_MAX_MARKET_DATA_RECORDS": "20",
        "TX_TRADE_RESEARCH_PAPER_MAX_INSTRUMENT_VERSIONS": "10",
        "TX_TRADE_RESEARCH_PAPER_MAX_POSITIONS": "10",
        "TX_TRADE_RESEARCH_PAPER_MAX_DECISION_RECORDS": "20",
        "TX_TRADE_RESEARCH_PAPER_SLIPPAGE_MODE": "none",
        "TX_TRADE_RESEARCH_PAPER_SLIPPAGE_VALUE": "0",
        "TX_TRADE_RESEARCH_PAPER_FEE_POLICY": "zero",
        "TX_TRADE_RESEARCH_PAPER_STRATEGY_ID": "alpha",
        "TX_TRADE_RESEARCH_PAPER_CLIENT_ORDER_ID": "entry-1",
        "TX_TRADE_RESEARCH_PAPER_ACCOUNT_ID": "paper",
        "TX_TRADE_RESEARCH_PAPER_INSTRUMENT_ID": "TAIFEX:0:TX00",
        "TX_TRADE_RESEARCH_PAPER_ORDER_SIDE": "buy",
        "TX_TRADE_RESEARCH_PAPER_ORDER_QUANTITY": "2",
        "TX_TRADE_RESEARCH_PAPER_ORDER_TYPE": "market",
        "TX_TRADE_RESEARCH_PAPER_TIME_IN_FORCE": "day",
        "TX_TRADE_RESEARCH_PAPER_DAY_TRADE": "0",
    }
    return parse_research_paper_settings(values)


def _forbidden_repository_factory(*args, **kwargs):
    raise AssertionError("unsafe paths must fail before the repository factory")


def _make_symlink(link: Path, target: Path, *, directory: bool = False) -> None:
    try:
        link.symlink_to(target, target_is_directory=directory)
    except (NotImplementedError, OSError) as exc:
        pytest.skip(f"symlink fixture is unavailable: {type(exc).__name__}")


def _record_source(database_path: Path) -> None:
    repository = SQLiteMarketDataRepository(database_path)
    events = tuple(
        replace(envelope, session_id=SESSION_ID) for envelope in make_offline_fixture_envelopes()
    )
    repository.begin_session(
        RecordingSession(
            session_id=SESSION_ID,
            schema_version=SCHEMA_VERSION,
            source=events[0].source,
            source_mode=SourceMode.OFFLINE,
            started_at=OFFLINE_FIXTURE_TIME,
            trading_day=OFFLINE_FIXTURE_TRADING_DAY,
            config_fingerprint="phase2-final-safety",
        )
    )
    repository.append_batch(events)
    repository.end_session(
        SESSION_ID,
        OFFLINE_FIXTURE_TIME + timedelta(minutes=1),
        "complete",
    )
    repository.close()


def test_existing_regular_local_file_reaches_factory_with_canonical_path(tmp_path: Path) -> None:
    database_path = tmp_path / "recording.sqlite3"
    database_path.touch()
    alias = tmp_path / "child" / ".." / database_path.name
    calls = []
    sentinel = object()

    def factory(path, *, recover_incomplete_sessions, read_only):
        calls.append((path, recover_incomplete_sessions, read_only))
        return sentinel

    repository, canonical_path = _open_repository(_settings(alias), factory)

    assert repository is sentinel
    assert canonical_path == database_path.resolve(strict=True)
    assert calls == [(database_path.resolve(strict=True), False, True)]


def test_existing_source_leaf_symlink_fails_before_repository_factory(tmp_path: Path) -> None:
    target = tmp_path / "target.sqlite3"
    target.touch()
    link = tmp_path / "source-link.sqlite3"
    _make_symlink(link, target)

    with pytest.raises(ResearchPaperApplicationError, match="failed safely"):
        _open_repository(_settings(link), _forbidden_repository_factory)


def test_existing_source_parent_symlink_fails_before_repository_factory(tmp_path: Path) -> None:
    target = tmp_path / "real"
    target.mkdir()
    (target / "source.sqlite3").touch()
    link = tmp_path / "linked-parent"
    _make_symlink(link, target, directory=True)

    with pytest.raises(ResearchPaperApplicationError, match="failed safely"):
        _open_repository(_settings(link / "source.sqlite3"), _forbidden_repository_factory)


def test_existing_junction_reparse_component_fails_before_repository_factory(
    tmp_path: Path,
) -> None:
    if os.name != "nt":
        pytest.skip("Windows junction/reparse fixtures are unavailable on this platform")
    target = tmp_path / "junction-target"
    target.mkdir()
    (target / "source.sqlite3").touch()
    junction = tmp_path / "junction"
    created = subprocess.run(
        ["cmd.exe", "/d", "/c", "mklink", "/J", str(junction), str(target)],
        capture_output=True,
        check=False,
        text=True,
    )
    if created.returncode != 0:
        pytest.skip("Windows junction creation is unavailable in this environment")
    try:
        if not getattr(junction.lstat(), "st_file_attributes", 0) & 0x400:
            pytest.skip("created junction is not exposed as a reparse point")
        with pytest.raises(ResearchPaperApplicationError, match="failed safely"):
            _open_repository(
                _settings(junction / "source.sqlite3"),
                _forbidden_repository_factory,
            )
    finally:
        os.rmdir(junction)


def test_unc_path_fails_before_repository_factory_without_network_access() -> None:
    unc_path = Path(r"\\phase2-invalid\share\existing.sqlite3")

    with pytest.raises(ResearchPaperApplicationError, match="failed safely"):
        _open_repository(_settings(unc_path), _forbidden_repository_factory)


def test_existing_mapped_drive_path_fails_before_repository_factory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "mapped.sqlite3"
    database_path.touch()
    monkeypatch.setattr(research_paper, "_is_remote_drive", lambda path: True)

    with pytest.raises(ResearchPaperApplicationError, match="failed safely"):
        _open_repository(_settings(database_path), _forbidden_repository_factory)


def test_actual_mapped_drive_root_fails_before_repository_factory() -> None:
    if os.name != "nt":
        pytest.skip("Windows mapped-drive detection is unavailable on this platform")
    import ctypes

    drive_mask = ctypes.windll.kernel32.GetLogicalDrives()
    mapped_roots = [
        Path(f"{chr(ord('A') + index)}:\\")
        for index in range(26)
        if drive_mask & (1 << index)
        and ctypes.windll.kernel32.GetDriveTypeW(f"{chr(ord('A') + index)}:\\") == 4
    ]
    if not mapped_roots:
        pytest.skip("no mapped network drive is configured on this host")

    with pytest.raises(ResearchPaperApplicationError, match="failed safely"):
        _open_repository(_settings(mapped_roots[0]), _forbidden_repository_factory)


def test_source_hardlink_is_a_regular_local_file(tmp_path: Path) -> None:
    target = tmp_path / "target.sqlite3"
    target.touch()
    hardlink = tmp_path / "source-hardlink.sqlite3"
    try:
        os.link(target, hardlink)
    except OSError as exc:
        pytest.skip(f"hardlink fixture is unavailable: {type(exc).__name__}")
    calls = []

    def factory(path, *, recover_incomplete_sessions, read_only):
        calls.append(path)
        return object()

    _open_repository(_settings(hardlink), factory)

    assert calls == [hardlink.resolve(strict=True)]


def test_state_hardlink_to_source_fails_before_writable_repository_factory(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.sqlite3"
    _record_source(source)
    state = tmp_path / "state.sqlite3"
    try:
        os.link(source, state)
    except OSError as exc:
        pytest.skip(f"hardlink fixture is unavailable: {type(exc).__name__}")
    settings = replace(
        _settings(source),
        restart_mode=ResearchRestartMode.RESUME,
        state_database_path=state,
        max_state_main_database_bytes=16 * 1024 * 1024,
    )
    state_factory_called = False

    def forbidden_state_factory(*args, **kwargs):
        nonlocal state_factory_called
        state_factory_called = True
        raise AssertionError("hardlink must fail before writable SQLite construction")

    with pytest.raises(ResearchPaperApplicationError, match="failed safely"):
        run_research_paper(
            settings,
            state_repository_factory=forbidden_state_factory,
        )

    assert not state_factory_called
    assert source.read_bytes() == state.read_bytes()
    assert not Path(f"{source}-wal").exists()
    assert not Path(f"{source}-shm").exists()
