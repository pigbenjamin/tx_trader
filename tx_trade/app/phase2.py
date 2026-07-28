"""Standalone, replay-only Phase 2A composition root and command-line entry."""

from __future__ import annotations

import os
import sys
from collections.abc import Callable, Mapping, Sequence
from typing import Any, TextIO

from tx_trade.app.phase2_config import (
    Phase2ReplaySettings,
    parse_phase2_replay_settings,
)
from tx_trade.market_data.models import MarketDataEnvelope, serialize_envelope
from tx_trade.market_data.ports import MarketDataSink
from tx_trade.replay import (
    ReplayRuntime,
    ReplaySnapshot,
    ReplayState,
    ReplayTimer,
    prepare_sqlite_replay_source,
)
from tx_trade.storage import SQLiteMarketDataRepository

_FAILURE_MESSAGE = "Phase 2 replay failed safely."
_SUCCESS_MESSAGE = "Phase 2 replay completed."
_SHUTDOWN_TIMEOUT_SECONDS = 1.0


class Phase2ApplicationError(RuntimeError):
    """A stable application failure that contains no underlying details."""


class JsonLinesSink:
    """Write one canonical market-data envelope per output line."""

    def __init__(self, stream: TextIO) -> None:
        self._stream = stream

    def publish(self, envelope: MarketDataEnvelope) -> None:
        self._stream.write(serialize_envelope(envelope))
        self._stream.write("\n")
        self._stream.flush()


def _open_repository(
    settings: Phase2ReplaySettings,
    repository_factory: Callable[..., Any],
) -> Any:
    try:
        is_file = settings.database_path.is_file()
    except OSError:
        is_file = False
    if not is_file:
        raise Phase2ApplicationError(_FAILURE_MESSAGE)
    if any(
        settings.database_path.with_name(f"{settings.database_path.name}{suffix}").exists()
        for suffix in ("-wal", "-shm")
    ):
        raise Phase2ApplicationError(_FAILURE_MESSAGE)
    return repository_factory(
        settings.database_path,
        recover_incomplete_sessions=False,
        read_only=True,
    )


def run_phase2_replay(
    settings: Phase2ReplaySettings,
    sink: MarketDataSink,
    *,
    timer: ReplayTimer | None = None,
    repository_factory: Callable[..., Any] = SQLiteMarketDataRepository,
) -> ReplaySnapshot:
    """Run one validated SQLite session to completion and always close storage."""

    if type(settings) is not Phase2ReplaySettings:
        raise TypeError("settings must be Phase2ReplaySettings")

    repository: Any | None = None
    runtime: ReplayRuntime | None = None
    snapshot: ReplaySnapshot | None = None
    failed = False
    try:
        repository = _open_repository(settings, repository_factory)
        source, descriptor = prepare_sqlite_replay_source(
            repository,
            settings.session_id,
        )
        runtime = ReplayRuntime(
            source=source,
            descriptor=descriptor,
            sink=sink,
            options=settings.options,
            timer=timer,
        )
        snapshot = runtime.run()
        if snapshot.state is not ReplayState.COMPLETED:
            failed = True
    except (Exception, KeyboardInterrupt):
        failed = True
        if runtime is not None:
            try:
                runtime.stop(_SHUTDOWN_TIMEOUT_SECONDS)
            except (Exception, KeyboardInterrupt):
                pass
    finally:
        if repository is not None:
            try:
                repository.close()
            except (Exception, KeyboardInterrupt):
                failed = True

    if failed or snapshot is None:
        raise Phase2ApplicationError(_FAILURE_MESSAGE) from None
    return snapshot


def run_phase2(
    environment: Mapping[str, str] | None = None,
    *,
    sink: MarketDataSink,
    timer: ReplayTimer | None = None,
    repository_factory: Callable[..., Any] = SQLiteMarketDataRepository,
) -> ReplaySnapshot:
    """Parse only the replay settings and run the standalone composition."""

    supplied = os.environ if environment is None else environment
    settings = parse_phase2_replay_settings(supplied)
    return run_phase2_replay(
        settings,
        sink,
        timer=timer,
        repository_factory=repository_factory,
    )


def main(argv: Sequence[str] | None = None) -> int:
    arguments = tuple(sys.argv[1:] if argv is None else argv)
    if arguments:
        print(_FAILURE_MESSAGE, file=sys.stderr)
        return 2
    try:
        run_phase2(sink=JsonLinesSink(sys.stdout))
    except (Exception, KeyboardInterrupt):
        print(_FAILURE_MESSAGE, file=sys.stderr)
        return 2
    print(_SUCCESS_MESSAGE, file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
