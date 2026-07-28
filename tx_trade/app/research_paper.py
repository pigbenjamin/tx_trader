"""Standalone deterministic research-paper replay composition and CLI."""

from __future__ import annotations

import os
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO

from tx_trade.app.research_output import (
    ResearchOutputCorrelation,
    ResearchOutputLimits,
    materialize_research_jsonl,
)
from tx_trade.app.research_paper_config import (
    ResearchPaperSettings,
    parse_research_paper_settings,
)
from tx_trade.market_data.models import MarketDataEnvelope, serialize_envelope
from tx_trade.orders import PaperBrokerSnapshot
from tx_trade.orders.paper_broker import PaperBroker
from tx_trade.replay import (
    ReplayRuntime,
    ReplaySnapshot,
    ReplayState,
    ReplayTimer,
    prepare_sqlite_replay_source,
)
from tx_trade.storage import SQLiteMarketDataRepository
from tx_trade.strategy import (
    InstrumentTriggeredOrderStrategy,
    PaperReplayCoordinator,
    StrategyDecisionRecord,
    StrategyExecutionMode,
    StrategyRegistration,
)

_FAILURE_MESSAGE = "Research paper replay failed safely."
_SUCCESS_MESSAGE = "Research paper replay completed."
_SHUTDOWN_TIMEOUT_SECONDS = 1.0
_MAX_OUTPUT_BYTES = 64 * 1024 * 1024


class ResearchPaperApplicationError(RuntimeError):
    """A fixed, non-sensitive application boundary failure."""


@dataclass(frozen=True, slots=True)
class ResearchPaperResult:
    """Complete terminal state and buffered output for one successful run."""

    replay_snapshot: ReplaySnapshot
    broker_snapshot: PaperBrokerSnapshot
    decision_records: tuple[StrategyDecisionRecord, ...]
    market_envelopes: tuple[MarketDataEnvelope, ...]
    correlation: ResearchOutputCorrelation
    output: bytes

    def __post_init__(self) -> None:
        if type(self.replay_snapshot) is not ReplaySnapshot:
            raise TypeError("replay_snapshot must be ReplaySnapshot")
        if self.replay_snapshot.state is not ReplayState.COMPLETED:
            raise ValueError("replay_snapshot must be completed")
        if type(self.broker_snapshot) is not PaperBrokerSnapshot:
            raise TypeError("broker_snapshot must be PaperBrokerSnapshot")
        if type(self.decision_records) is not tuple or any(
            type(item) is not StrategyDecisionRecord for item in self.decision_records
        ):
            raise TypeError("decision_records must contain StrategyDecisionRecord")
        if type(self.market_envelopes) is not tuple or any(
            type(item) is not MarketDataEnvelope for item in self.market_envelopes
        ):
            raise TypeError("market_envelopes must contain MarketDataEnvelope")
        if type(self.correlation) is not ResearchOutputCorrelation:
            raise TypeError("correlation must be ResearchOutputCorrelation")
        if type(self.output) is not bytes or not self.output:
            raise ValueError("output must be non-empty bytes")
        if (
            self.replay_snapshot.session_id != self.correlation.replay_session_id
            or self.replay_snapshot.cursor != self.correlation.terminal_cursor
            or self.broker_snapshot.paper_run_id != self.correlation.paper_run_id
            or self.broker_snapshot.bound_source_session_id != self.correlation.replay_session_id
            or self.broker_snapshot.last_committed_ingest_sequence
            != self.correlation.terminal_cursor
            or self.broker_snapshot.execution_config_fingerprint
            != self.correlation.execution_config_fingerprint
        ):
            raise ValueError("terminal result correlation mismatch")


class _BufferedCoordinatorSink:
    def __init__(
        self,
        coordinator: PaperReplayCoordinator,
        *,
        max_market_records: int,
        max_output_bytes: int,
    ) -> None:
        self._coordinator = coordinator
        self._max_market_records = max_market_records
        self._max_output_bytes = max_output_bytes
        self._serialized_market_bytes = 0
        self._envelopes: list[MarketDataEnvelope] = []

    def publish(self, envelope: MarketDataEnvelope) -> None:
        if len(self._envelopes) >= self._max_market_records:
            raise ResearchPaperApplicationError(_FAILURE_MESSAGE)
        try:
            serialized_size = len(serialize_envelope(envelope).encode("utf-8")) + 1
        except Exception:
            raise ResearchPaperApplicationError(_FAILURE_MESSAGE) from None
        if self._serialized_market_bytes + serialized_size > self._max_output_bytes:
            raise ResearchPaperApplicationError(_FAILURE_MESSAGE)
        self._coordinator.publish(envelope)
        self._envelopes.append(envelope)
        self._serialized_market_bytes += serialized_size

    def snapshot(self) -> tuple[MarketDataEnvelope, ...]:
        return tuple(self._envelopes)


def _open_repository(
    settings: ResearchPaperSettings,
    repository_factory: Callable[..., Any],
) -> tuple[Any, Path]:
    try:
        database_path = settings.database_path.resolve(strict=True)
        is_file = database_path.is_file()
    except (OSError, RuntimeError):
        is_file = False
        database_path = None
    if not is_file:
        raise ResearchPaperApplicationError(_FAILURE_MESSAGE)
    assert database_path is not None
    if _sidecars_exist(database_path):
        raise ResearchPaperApplicationError(_FAILURE_MESSAGE)
    repository = repository_factory(
        database_path,
        recover_incomplete_sessions=False,
        read_only=True,
    )
    if _sidecars_exist(database_path):
        try:
            repository.close()
        except (Exception, KeyboardInterrupt):
            pass
        raise ResearchPaperApplicationError(_FAILURE_MESSAGE)
    return repository, database_path


def _sidecars_exist(database_path: Path) -> bool:
    try:
        return any(
            database_path.with_name(f"{database_path.name}{suffix}").exists()
            for suffix in ("-wal", "-shm")
        )
    except OSError:
        return True


def run_research_paper(
    settings: ResearchPaperSettings,
    *,
    timer: ReplayTimer | None = None,
    repository_factory: Callable[..., Any] = SQLiteMarketDataRepository,
) -> ResearchPaperResult:
    """Run one complete, synchronous paper replay and return buffered output."""

    if type(settings) is not ResearchPaperSettings:
        raise TypeError("settings must be ResearchPaperSettings")

    repository: Any | None = None
    database_path: Path | None = None
    runtime: ReplayRuntime | None = None
    result: ResearchPaperResult | None = None
    failed = False
    try:
        repository, database_path = _open_repository(settings, repository_factory)
        source, descriptor = prepare_sqlite_replay_source(repository, settings.session_id)
        if (
            descriptor.event_count > settings.limits.max_market_data_records
            or descriptor.event_count > settings.max_decision_records
        ):
            raise ResearchPaperApplicationError(_FAILURE_MESSAGE)
        broker = PaperBroker(
            paper_run_id=settings.paper_run_id,
            limits=settings.limits,
            execution_config=settings.execution_config,
            expected_source_session_id=descriptor.session_id,
        )
        strategy = InstrumentTriggeredOrderStrategy(settings.order_template)
        coordinator = PaperReplayCoordinator(
            broker=broker,
            registrations=(
                StrategyRegistration(
                    strategy_id=settings.order_template.strategy_id,
                    strategy=strategy,
                ),
            ),
            mode=StrategyExecutionMode.PAPER,
            max_decision_records=settings.max_decision_records,
        )
        sink = _BufferedCoordinatorSink(
            coordinator,
            max_market_records=settings.limits.max_market_data_records,
            max_output_bytes=_MAX_OUTPUT_BYTES,
        )
        runtime = ReplayRuntime(
            source=source,
            descriptor=descriptor,
            sink=sink,
            options=settings.options,
            timer=timer,
        )
        replay_snapshot = runtime.run()
        broker_snapshot = broker.snapshot()
        envelopes = sink.snapshot()
        decisions = coordinator.decision_records()
        if (
            replay_snapshot.state is not ReplayState.COMPLETED
            or replay_snapshot.session_id != descriptor.session_id
            or replay_snapshot.cursor != descriptor.last_ingest_sequence
            or replay_snapshot.emitted_count != descriptor.event_count
            or len(envelopes) != descriptor.event_count
            or broker_snapshot.bound_source_session_id != descriptor.session_id
            or broker_snapshot.last_committed_ingest_sequence != descriptor.last_ingest_sequence
            or len(decisions) != descriptor.event_count
            or database_path is None
            or _sidecars_exist(database_path)
        ):
            raise ResearchPaperApplicationError(_FAILURE_MESSAGE)
        correlation = ResearchOutputCorrelation(
            replay_session_id=descriptor.session_id,
            paper_run_id=settings.paper_run_id,
            execution_config_fingerprint=broker_snapshot.execution_config_fingerprint,
            terminal_cursor=descriptor.last_ingest_sequence,
        )
        output = materialize_research_jsonl(
            market_envelopes=envelopes,
            decision_records=decisions,
            broker_snapshot=broker_snapshot,
            correlation=correlation,
            limits=ResearchOutputLimits(
                max_market_records=settings.limits.max_market_data_records,
                max_paper_events=settings.limits.max_events,
                max_decision_records=settings.max_decision_records,
                max_output_bytes=_MAX_OUTPUT_BYTES,
            ),
        )
        result = ResearchPaperResult(
            replay_snapshot=replay_snapshot,
            broker_snapshot=broker_snapshot,
            decision_records=decisions,
            market_envelopes=envelopes,
            correlation=correlation,
            output=output,
        )
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

    if failed or result is None:
        raise ResearchPaperApplicationError(_FAILURE_MESSAGE) from None
    return result


def run_research_paper_app(
    environment: Mapping[str, str] | None = None,
    *,
    timer: ReplayTimer | None = None,
    repository_factory: Callable[..., Any] = SQLiteMarketDataRepository,
) -> ResearchPaperResult:
    """Parse the research allowlist and run the isolated composition."""

    supplied = os.environ if environment is None else environment
    settings = parse_research_paper_settings(supplied)
    return run_research_paper(
        settings,
        timer=timer,
        repository_factory=repository_factory,
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    environment: Mapping[str, str] | None = None,
    output: BinaryIO | None = None,
) -> int:
    arguments = tuple(sys.argv[1:] if argv is None else argv)
    if arguments:
        print(_FAILURE_MESSAGE, file=sys.stderr)
        return 2
    try:
        result = run_research_paper_app(environment)
        stream = sys.stdout.buffer if output is None else output
        stream.write(result.output)
        stream.flush()
    except (Exception, KeyboardInterrupt):
        print(_FAILURE_MESSAGE, file=sys.stderr)
        return 2
    print(_SUCCESS_MESSAGE, file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
