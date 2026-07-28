from __future__ import annotations

from collections.abc import Iterator, Mapping
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from tx_trade.app.phase2 import (
    Phase2ApplicationError,
    run_phase2,
    run_phase2_replay,
)
from tx_trade.app.phase2_config import (
    Phase2ReplaySettings,
    parse_phase2_replay_settings,
)
from tx_trade.replay import ReplayMode, ReplayOptions, ReplaySnapshot, ReplayState

_SECRET = "phase2-composition-secret-canary"


class _Sink:
    def publish(self, envelope) -> None:
        pass


class _Repository:
    def __init__(self, *, close_error: bool = False) -> None:
        self.closed = False
        self.close_error = close_error

    def close(self) -> None:
        self.closed = True
        if self.close_error:
            raise RuntimeError(_SECRET)


def _settings(database_path: Path, session_id: UUID | None = None) -> Phase2ReplaySettings:
    return Phase2ReplaySettings(
        runtime_preset="phase2_replay",
        execution_mode="disabled",
        database_path=database_path,
        session_id=session_id or uuid4(),
        options=ReplayOptions(mode=ReplayMode.FASTEST),
    )


def _snapshot(settings: Phase2ReplaySettings, state=ReplayState.COMPLETED) -> ReplaySnapshot:
    return ReplaySnapshot(
        state=state,
        session_id=settings.session_id,
        cursor=5,
        emitted_count=6,
        failure_code=None,
    )


def test_run_composes_runtime_and_closes_repository(tmp_path, monkeypatch) -> None:
    database_path = tmp_path / "recording.db"
    database_path.touch()
    settings = _settings(database_path)
    repository = _Repository()
    calls: list[object] = []

    def repository_factory(path, *, recover_incomplete_sessions, read_only):
        calls.append(("repository", path, recover_incomplete_sessions, read_only))
        return repository

    def prepare(actual_repository, session_id):
        calls.append(("prepare", actual_repository, session_id))
        return "source", "descriptor"

    class Runtime:
        def __init__(self, **kwargs):
            calls.append(("runtime", kwargs))

        def run(self):
            calls.append("run")
            return _snapshot(settings)

    monkeypatch.setattr("tx_trade.app.phase2.prepare_sqlite_replay_source", prepare)
    monkeypatch.setattr("tx_trade.app.phase2.ReplayRuntime", Runtime)
    timer = object()

    result = run_phase2_replay(
        settings,
        _Sink(),
        timer=timer,  # type: ignore[arg-type]
        repository_factory=repository_factory,
    )

    assert result == _snapshot(settings)
    assert calls[0] == ("repository", database_path, False, True)
    assert calls[1] == ("prepare", repository, settings.session_id)
    runtime_arguments = calls[2][1]
    assert runtime_arguments["source"] == "source"
    assert runtime_arguments["descriptor"] == "descriptor"
    assert runtime_arguments["options"] is settings.options
    assert runtime_arguments["timer"] is timer
    assert calls[3:] == ["run"]
    assert repository.closed


def test_missing_database_fails_before_factory_and_is_not_created(tmp_path) -> None:
    database_path = tmp_path / _SECRET / "missing.db"
    called = False

    def repository_factory(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("factory must not be called")

    with pytest.raises(Phase2ApplicationError) as caught:
        run_phase2_replay(
            _settings(database_path),
            _Sink(),
            repository_factory=repository_factory,
        )

    assert not called
    assert not database_path.exists()
    assert str(caught.value) == "Phase 2 replay failed safely."
    assert _SECRET not in repr(caught.value)


@pytest.mark.parametrize("stage", ["prepare", "run", "terminal", "close"])
def test_every_failure_stage_is_sanitized_and_closes_repository(
    tmp_path,
    monkeypatch,
    stage,
) -> None:
    database_path = tmp_path / "recording.db"
    database_path.touch()
    settings = _settings(database_path)
    repository = _Repository(close_error=stage == "close")
    stopped: list[bool] = []

    def prepare(actual_repository, session_id):
        if stage == "prepare":
            raise RuntimeError(_SECRET)
        return "source", "descriptor"

    class Runtime:
        def __init__(self, **kwargs):
            pass

        def run(self):
            if stage == "run":
                raise RuntimeError(_SECRET)
            if stage == "terminal":
                return _snapshot(settings, ReplayState.STOPPED)
            return _snapshot(settings)

        def stop(self, timeout_seconds):
            stopped.append(timeout_seconds)

    monkeypatch.setattr("tx_trade.app.phase2.prepare_sqlite_replay_source", prepare)
    monkeypatch.setattr("tx_trade.app.phase2.ReplayRuntime", Runtime)

    with pytest.raises(Phase2ApplicationError) as caught:
        run_phase2_replay(
            settings,
            _Sink(),
            repository_factory=lambda *args, **kwargs: repository,
        )

    assert repository.closed
    assert str(caught.value) == "Phase 2 replay failed safely."
    assert _SECRET not in repr(caught.value)
    if stage in {"run"}:
        assert stopped == [1.0]


def test_keyboard_interrupt_stops_runtime_and_closes_repository(
    tmp_path,
    monkeypatch,
) -> None:
    database_path = tmp_path / "recording.db"
    database_path.touch()
    settings = _settings(database_path)
    repository = _Repository()
    calls: list[str] = []

    monkeypatch.setattr(
        "tx_trade.app.phase2.prepare_sqlite_replay_source",
        lambda *args: ("source", "descriptor"),
    )

    class Runtime:
        def __init__(self, **kwargs):
            pass

        def run(self):
            raise KeyboardInterrupt

        def stop(self, timeout_seconds):
            calls.append(f"stop:{timeout_seconds}")

    monkeypatch.setattr("tx_trade.app.phase2.ReplayRuntime", Runtime)

    with pytest.raises(Phase2ApplicationError, match="failed safely"):
        run_phase2_replay(
            settings,
            _Sink(),
            repository_factory=lambda *args, **kwargs: repository,
        )

    assert calls == ["stop:1.0"]
    assert repository.closed


class _AccessTrackingMapping(Mapping[str, str]):
    def __init__(self, values: dict[str, str]) -> None:
        self._values = values
        self.accessed: list[str] = []

    def __getitem__(self, key: str) -> str:
        self.accessed.append(key)
        return self._values[key]

    def __iter__(self) -> Iterator[str]:
        raise AssertionError("replay composition must not enumerate configuration")

    def __len__(self) -> int:
        return len(self._values)

    def get(self, key: str, default: str | None = None) -> str | None:
        self.accessed.append(key)
        return self._values.get(key, default)


def test_run_reads_only_replay_configuration_keys(tmp_path, monkeypatch) -> None:
    database_path = tmp_path / "recording.db"
    values = _AccessTrackingMapping(
        {
            "TX_TRADE_REPLAY_DB_PATH": str(database_path),
            "TX_TRADE_REPLAY_SESSION_ID": str(uuid4()),
            "TX_TRADE_ACCOUNT": _SECRET,
            "TX_TRADE_PASSWORD": _SECRET,
            "TX_TRADE_SKCOM_DLL_PATH": _SECRET,
        }
    )
    expected = parse_phase2_replay_settings(values)
    values.accessed.clear()
    monkeypatch.setattr(
        "tx_trade.app.phase2.run_phase2_replay",
        lambda settings, sink, **kwargs: _snapshot(expected),
    )

    result = run_phase2(values, sink=_Sink())

    assert result == _snapshot(expected)
    assert set(values.accessed) == {
        "TX_TRADE_RUNTIME_PRESET",
        "TX_TRADE_REPLAY_DB_PATH",
        "TX_TRADE_REPLAY_SESSION_ID",
        "TX_TRADE_REPLAY_MODE",
        "TX_TRADE_REPLAY_SPEED",
        "TX_TRADE_REPLAY_AFTER_INGEST_SEQUENCE",
    }
    assert not {
        "TX_TRADE_ACCOUNT",
        "TX_TRADE_PASSWORD",
        "TX_TRADE_SKCOM_DLL_PATH",
    }.intersection(values.accessed)


def test_settings_type_is_checked_before_filesystem_access(tmp_path) -> None:
    with pytest.raises(TypeError, match="Phase2ReplaySettings"):
        run_phase2_replay(None, _Sink())  # type: ignore[arg-type]
