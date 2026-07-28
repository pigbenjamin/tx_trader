"""Standalone deterministic research-paper replay composition and CLI."""

from __future__ import annotations

import os
import stat
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any, BinaryIO

from tx_trade.app.research_output import (
    ResearchOutputCorrelation,
    ResearchOutputLimits,
    encode_market_record,
    encode_paper_record,
    encode_summary_record,
    materialize_research_jsonl,
)
from tx_trade.app.research_paper_config import (
    ResearchPaperSettings,
    ResearchRestartMode,
    parse_research_paper_settings,
)
from tx_trade.market_data.models import MarketDataEnvelope, serialize_envelope
from tx_trade.orders import PaperBrokerSnapshot
from tx_trade.orders.paper_broker import PaperBroker
from tx_trade.research import (
    CompleteResearchRun,
    ResearchDurableBatch,
    ResearchOutboxRecord,
    ResearchOutboxRecordType,
    ResearchRunIdentity,
    ResearchRunStatus,
    StrategyFingerprint,
)
from tx_trade.research.sqlite_repository import SQLiteResearchStateRepository
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
_SOURCE_FINGERPRINT_DOMAIN = b"tx_trade.research.source.v1\0"
_BROKER_ALGORITHM_VERSION = "paper-v1"


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


class _DurableCoordinatorSink:
    def __init__(
        self,
        *,
        coordinator: PaperReplayCoordinator,
        broker: PaperBroker,
        repository: Any,
        settings: ResearchPaperSettings,
        envelopes: tuple[MarketDataEnvelope, ...],
        state_version: int,
        committed_cursor: int | None,
        durable_output_bytes: int,
    ) -> None:
        self._coordinator = coordinator
        self._broker = broker
        self._repository = repository
        self._settings = settings
        self._ordinal_by_sequence = {
            envelope.ingest_sequence: ordinal for ordinal, envelope in enumerate(envelopes)
        }
        self._timestamp_base = envelopes[0].received_at
        self._state_version = state_version
        self._committed_cursor = committed_cursor
        self._durable_output_bytes = durable_output_bytes

    def publish(self, envelope: MarketDataEnvelope) -> None:
        ordinal = self._ordinal_by_sequence.get(envelope.ingest_sequence)
        if ordinal is None:
            raise ResearchPaperApplicationError(_FAILURE_MESSAGE)
        before_event_count = len(self._broker.snapshot().events)
        self._coordinator.publish(envelope)
        snapshot = self._broker.snapshot()
        records = self._coordinator.decision_records()
        decision = next(
            (
                record
                for record in records
                if record.source_session_id == envelope.session_id
                and record.source_ingest_sequence == envelope.ingest_sequence
            ),
            None,
        )
        if decision is None or decision.batch_result is None:
            raise ResearchPaperApplicationError(_FAILURE_MESSAGE)
        new_events = snapshot.events[before_event_count:]
        outbox = [
            ResearchOutboxRecord.create(
                paper_run_id=self._settings.paper_run_id,
                output_sequence=ordinal,
                record_type=ResearchOutboxRecordType.MARKET,
                source_ingest_sequence=envelope.ingest_sequence,
                paper_sequence=None,
                payload=encode_market_record(envelope),
            )
        ]
        outbox.extend(
            ResearchOutboxRecord.create(
                paper_run_id=self._settings.paper_run_id,
                output_sequence=self._settings.limits.max_market_data_records
                + event.paper_sequence
                - 1,
                record_type=ResearchOutboxRecordType.PAPER,
                source_ingest_sequence=envelope.ingest_sequence,
                paper_sequence=event.paper_sequence,
                payload=encode_paper_record(event, decision),
            )
            for event in new_events
        )
        # The durable sequence offset is the validated source count, not capacity.
        event_count = len(self._ordinal_by_sequence)
        outbox = [
            replace(
                record,
                output_sequence=(
                    event_count + record.paper_sequence - 1
                    if record.record_type is ResearchOutboxRecordType.PAPER
                    and record.paper_sequence is not None
                    else record.output_sequence
                ),
            )
            for record in outbox
        ]
        added_bytes = sum(len(record.payload) for record in outbox)
        if self._durable_output_bytes + added_bytes > _MAX_OUTPUT_BYTES:
            raise ResearchPaperApplicationError(_FAILURE_MESSAGE)
        result = self._repository.commit_batch(
            ResearchDurableBatch(
                paper_run_id=self._settings.paper_run_id,
                expected_state_version=self._state_version,
                expected_previous_cursor=self._committed_cursor,
                source_session_id=envelope.session_id,
                source_ingest_sequence=envelope.ingest_sequence,
                envelope_fingerprint=f"sha256:{decision.envelope_digest}",
                decision_fingerprint=decision.decision.decision_fingerprint,
                broker_checkpoint=self._broker.export_checkpoint(),
                coordinator_checkpoint=self._coordinator.export_checkpoint(),
                outbox_records=tuple(sorted(outbox, key=lambda item: item.output_sequence)),
            ),
            self._timestamp_base + timedelta(microseconds=ordinal + 1),
        )
        self._state_version = result.run_state.state_version
        self._committed_cursor = result.run_state.committed_cursor
        self._durable_output_bytes += added_bytes

    @property
    def state_version(self) -> int:
        return self._state_version

    @property
    def durable_output_bytes(self) -> int:
        return self._durable_output_bytes


def _open_repository(
    settings: ResearchPaperSettings,
    repository_factory: Callable[..., Any],
) -> tuple[Any, Path]:
    try:
        _validate_local_configured_path(settings.database_path)
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


def _open_source_guard(
    settings: ResearchPaperSettings,
) -> tuple[int, Path, tuple[int, int]]:
    """Pin the local source file identity before any repository factory runs."""

    try:
        _validate_local_configured_path(settings.database_path)
        source_path = settings.database_path.resolve(strict=True)
        if not source_path.is_file() or _sidecars_exist(source_path):
            raise OSError
        flags = (
            os.O_RDONLY
            | getattr(os, "O_BINARY", 0)
            | getattr(os, "O_NOINHERIT", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        descriptor = os.open(source_path, flags)
        try:
            guarded = os.fstat(descriptor)
            if not stat.S_ISREG(guarded.st_mode):
                raise OSError
            identity = (guarded.st_dev, guarded.st_ino)
            pathname = source_path.stat()
            if (pathname.st_dev, pathname.st_ino) != identity or _sidecars_exist(source_path):
                raise OSError
        except BaseException:
            os.close(descriptor)
            raise
        return descriptor, source_path, identity
    except (OSError, RuntimeError):
        raise ResearchPaperApplicationError(_FAILURE_MESSAGE) from None


def _sidecars_exist(database_path: Path) -> bool:
    try:
        return any(
            database_path.with_name(f"{database_path.name}{suffix}").exists()
            for suffix in ("-wal", "-shm")
        )
    except OSError:
        return True


def _scan_source(source: Any, descriptor: Any) -> tuple[tuple[MarketDataEnvelope, ...], str]:
    envelopes = tuple(source.iter_events(after_ingest_sequence=None))
    sequences = tuple(item.ingest_sequence for item in envelopes)
    if (
        len(envelopes) != descriptor.event_count
        or not envelopes
        or any(item.session_id != descriptor.session_id for item in envelopes)
        or sequences[0] != descriptor.first_ingest_sequence
        or sequences[-1] != descriptor.last_ingest_sequence
        or any(left >= right for left, right in zip(sequences, sequences[1:], strict=False))
    ):
        raise ResearchPaperApplicationError(_FAILURE_MESSAGE)
    digest = sha256(_SOURCE_FINGERPRINT_DOMAIN)
    for envelope in envelopes:
        encoded = serialize_envelope(envelope).encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    if sum(len(encode_market_record(envelope)) for envelope in envelopes) > _MAX_OUTPUT_BYTES:
        raise ResearchPaperApplicationError(_FAILURE_MESSAGE)
    return envelopes, f"sha256:{digest.hexdigest()}"


def _registration(settings: ResearchPaperSettings) -> tuple[StrategyRegistration, ...]:
    return (
        StrategyRegistration(
            strategy_id=settings.order_template.strategy_id,
            strategy=InstrumentTriggeredOrderStrategy(settings.order_template),
            fingerprint=settings.strategy_fingerprint,
        ),
    )


def _identity(
    settings: ResearchPaperSettings,
    descriptor: Any,
    source_fingerprint: str,
) -> ResearchRunIdentity:
    return ResearchRunIdentity(
        paper_run_id=settings.paper_run_id,
        source_session_id=descriptor.session_id,
        source_schema_version=descriptor.schema_version,
        source_event_count=descriptor.event_count,
        source_first_sequence=descriptor.first_ingest_sequence,
        source_last_sequence=descriptor.last_ingest_sequence,
        source_content_fingerprint=source_fingerprint,
        research_config_fingerprint=settings.research_config_fingerprint,
        execution_config_fingerprint=settings.execution_config.fingerprint,
        strategy_fingerprints=(
            StrategyFingerprint(settings.order_template.strategy_id, settings.strategy_fingerprint),
        ),
        output_schema_version=1,
        broker_algorithm_version=_BROKER_ALGORITHM_VERSION,
    )


def _validate_state_path(source_path: Path, settings: ResearchPaperSettings) -> Path:
    configured = settings.state_database_path
    if configured is None:
        raise ResearchPaperApplicationError(_FAILURE_MESSAGE)
    try:
        _validate_local_configured_path(configured, include_leaf=False)
        parent = configured.parent.resolve(strict=True)
        if not parent.is_dir():
            raise OSError
        target = parent / configured.name
        if target == source_path:
            raise OSError
        if settings.restart_mode is ResearchRestartMode.CREATE:
            if target.exists():
                raise OSError
        else:
            if not target.is_file() or configured.is_symlink():
                raise OSError
            if os.path.samefile(target, source_path):
                raise OSError
            target_stat = target.stat()
            source_stat = source_path.stat()
            if (
                target_stat.st_ino
                and source_stat.st_ino
                and target_stat.st_dev == source_stat.st_dev
                and target_stat.st_ino == source_stat.st_ino
            ):
                raise OSError
        return target
    except (OSError, RuntimeError):
        raise ResearchPaperApplicationError(_FAILURE_MESSAGE) from None


def _reject_reparse_components(path: Path) -> None:
    """Reject symlink/reparse-point components without resolving them away."""

    absolute = path.absolute()
    current = Path(absolute.anchor)
    parts = absolute.parts[1:]
    index = 0
    while index < len(parts):
        part = parts[index]
        if part == "..":
            current = current.parent
            index += 1
            continue
        current = current / part
        try:
            metadata = current.lstat()
        except FileNotFoundError:
            if index + 1 < len(parts) and parts[index + 1] == "..":
                current = current.parent
                index += 2
                continue
            raise
        file_attributes = getattr(metadata, "st_file_attributes", 0)
        if current.is_symlink() or file_attributes & 0x400:
            raise OSError
        index += 1


def _validate_local_configured_path(path: Path, *, include_leaf: bool = True) -> None:
    raw = str(path)
    if raw.startswith(("\\\\", "//")) or _is_remote_drive(path):
        raise OSError
    _reject_reparse_components(path if include_leaf else path.parent)


def _is_remote_drive(path: Path) -> bool:
    """Return whether Windows reports the path root as a mapped/network drive."""

    if os.name != "nt":
        return False
    try:
        import ctypes

        anchor = path.absolute().anchor
        if not anchor:
            return True
        return ctypes.windll.kernel32.GetDriveTypeW(anchor) == 4
    except (AttributeError, OSError, RuntimeError, ValueError):
        return True


def _terminal_result(
    *,
    settings: ResearchPaperSettings,
    descriptor: Any,
    envelopes: tuple[MarketDataEnvelope, ...],
    broker: PaperBroker,
    coordinator: PaperReplayCoordinator,
    output: bytes,
) -> ResearchPaperResult:
    snapshot = broker.snapshot()
    decisions = coordinator.decision_records()
    correlation = ResearchOutputCorrelation(
        replay_session_id=descriptor.session_id,
        paper_run_id=settings.paper_run_id,
        execution_config_fingerprint=snapshot.execution_config_fingerprint,
        terminal_cursor=descriptor.last_ingest_sequence,
    )
    return ResearchPaperResult(
        replay_snapshot=ReplaySnapshot(
            state=ReplayState.COMPLETED,
            session_id=descriptor.session_id,
            cursor=descriptor.last_ingest_sequence,
            emitted_count=descriptor.event_count,
            failure_code=None,
        ),
        broker_snapshot=snapshot,
        decision_records=decisions,
        market_envelopes=envelopes,
        correlation=correlation,
        output=output,
    )


def _durable_prefix_bytes(
    *,
    envelopes: tuple[MarketDataEnvelope, ...],
    committed_cursor: int | None,
    broker: PaperBroker,
    coordinator: PaperReplayCoordinator,
) -> int:
    if committed_cursor is None:
        if broker.snapshot().events or coordinator.decision_count:
            raise ResearchPaperApplicationError(_FAILURE_MESSAGE)
        return 0
    decisions = {
        (record.source_session_id, record.source_ingest_sequence): record
        for record in coordinator.decision_records()
    }
    market_bytes = sum(
        len(encode_market_record(envelope))
        for envelope in envelopes
        if envelope.ingest_sequence <= committed_cursor
    )
    paper_bytes = 0
    for event in broker.snapshot().events:
        if event.source_session_id is None or event.source_ingest_sequence is None:
            raise ResearchPaperApplicationError(_FAILURE_MESSAGE)
        record = decisions.get((event.source_session_id, event.source_ingest_sequence))
        if record is None:
            raise ResearchPaperApplicationError(_FAILURE_MESSAGE)
        paper_bytes += len(encode_paper_record(event, record))
    total = market_bytes + paper_bytes
    if total > _MAX_OUTPUT_BYTES:
        raise ResearchPaperApplicationError(_FAILURE_MESSAGE)
    return total


def _validate_committed_prefix(
    *,
    envelopes: tuple[MarketDataEnvelope, ...],
    committed_cursor: int | None,
    paper_run_id: Any,
    broker: PaperBroker,
    coordinator: PaperReplayCoordinator,
) -> None:
    expected = tuple(
        envelope
        for envelope in envelopes
        if committed_cursor is not None and envelope.ingest_sequence <= committed_cursor
    )
    expected_by_source = {
        (envelope.session_id, envelope.ingest_sequence): envelope for envelope in expected
    }
    records = coordinator.decision_records()
    record_by_source = {
        (record.source_session_id, record.source_ingest_sequence): record for record in records
    }
    if len(record_by_source) != len(records) or set(record_by_source) != set(expected_by_source):
        raise ResearchPaperApplicationError(_FAILURE_MESSAGE)
    for source, envelope in expected_by_source.items():
        record = record_by_source[source]
        digest = sha256(serialize_envelope(envelope).encode("utf-8")).hexdigest()
        result = record.batch_result
        if (
            record.envelope_digest != digest
            or record.decision.source_session_id != source[0]
            or record.decision.source_ingest_sequence != source[1]
            or result is None
            or result.paper_run_id != paper_run_id
            or result.source_session_id != source[0]
            or result.source_ingest_sequence != source[1]
            or result.decision_fingerprint != record.decision.decision_fingerprint
        ):
            raise ResearchPaperApplicationError(_FAILURE_MESSAGE)
    for event in broker.snapshot().events:
        if event.source_session_id is None or event.source_ingest_sequence is None:
            raise ResearchPaperApplicationError(_FAILURE_MESSAGE)
        event_source = (event.source_session_id, event.source_ingest_sequence)
        if event_source not in record_by_source:
            raise ResearchPaperApplicationError(_FAILURE_MESSAGE)


def run_research_paper(
    settings: ResearchPaperSettings,
    *,
    timer: ReplayTimer | None = None,
    repository_factory: Callable[..., Any] = SQLiteMarketDataRepository,
    state_repository_factory: Callable[..., Any] = SQLiteResearchStateRepository,
) -> ResearchPaperResult:
    """Run one complete, synchronous paper replay and return buffered output."""

    if type(settings) is not ResearchPaperSettings:
        raise TypeError("settings must be ResearchPaperSettings")
    if settings.restart_mode is not ResearchRestartMode.DISABLED:
        return _run_durable_research_paper(
            settings,
            timer=timer,
            repository_factory=repository_factory,
            state_repository_factory=state_repository_factory,
        )

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


def _run_durable_research_paper(
    settings: ResearchPaperSettings,
    *,
    timer: ReplayTimer | None,
    repository_factory: Callable[..., Any],
    state_repository_factory: Callable[..., Any],
) -> ResearchPaperResult:
    source_repository: Any | None = None
    state_repository: Any | None = None
    runtime: ReplayRuntime | None = None
    source_path: Path | None = None
    source_guard_fd: int | None = None
    failed = False
    result: ResearchPaperResult | None = None
    try:
        source_guard_fd, guarded_source_path, source_file_identity = _open_source_guard(settings)
        source_repository, source_path = _open_repository(settings, repository_factory)
        path_stat = source_path.stat()
        if (
            source_path != guarded_source_path
            or (path_stat.st_dev, path_stat.st_ino) != source_file_identity
            or _sidecars_exist(source_path)
        ):
            raise ResearchPaperApplicationError(_FAILURE_MESSAGE)
        source, descriptor = prepare_sqlite_replay_source(source_repository, settings.session_id)
        if (
            descriptor.event_count > settings.limits.max_market_data_records
            or descriptor.event_count > settings.max_decision_records
        ):
            raise ResearchPaperApplicationError(_FAILURE_MESSAGE)
        envelopes, source_fingerprint = _scan_source(source, descriptor)
        identity = _identity(settings, descriptor, source_fingerprint)
        state_path = _validate_state_path(source_path, settings)
        assert settings.max_state_main_database_bytes is not None
        state_repository = state_repository_factory(
            state_path,
            max_main_database_bytes=settings.max_state_main_database_bytes,
            create_new=settings.restart_mode is ResearchRestartMode.CREATE,
            forbidden_file_identity=source_file_identity,
        )
        path_stat = source_path.stat()
        if (path_stat.st_dev, path_stat.st_ino) != source_file_identity or _sidecars_exist(
            source_path
        ):
            raise ResearchPaperApplicationError(_FAILURE_MESSAGE)
        registrations = _registration(settings)
        if settings.restart_mode is ResearchRestartMode.CREATE:
            broker = PaperBroker(
                paper_run_id=settings.paper_run_id,
                limits=settings.limits,
                execution_config=settings.execution_config,
                expected_source_session_id=descriptor.session_id,
            )
            coordinator = PaperReplayCoordinator(
                broker=broker,
                registrations=registrations,
                mode=StrategyExecutionMode.PAPER,
                max_decision_records=settings.max_decision_records,
            )
            hydration = state_repository.create_run(
                identity,
                broker.export_checkpoint(),
                coordinator.export_checkpoint(),
                envelopes[0].received_at,
            )
        else:
            hydration = state_repository.load_run(settings.paper_run_id)
            if hydration.run_state.identity != identity:
                raise ResearchPaperApplicationError(_FAILURE_MESSAGE)
            broker = PaperBroker.restore_checkpoint(hydration.broker_checkpoint)
            coordinator = PaperReplayCoordinator.restore_checkpoint(
                hydration.coordinator_checkpoint,
                broker=broker,
                registrations=registrations,
            )
        state = hydration.run_state
        broker_snapshot = broker.snapshot()
        if (
            broker_snapshot.paper_run_id != settings.paper_run_id
            or broker_snapshot.execution_config_fingerprint != settings.execution_config.fingerprint
            or broker_snapshot.last_committed_ingest_sequence != state.committed_cursor
            or coordinator.decision_count != state.committed_batch_count
            or (
                state.committed_cursor is not None
                and broker_snapshot.bound_source_session_id != descriptor.session_id
            )
            or getattr(broker, "_limits", None) != settings.limits
            or getattr(broker, "_execution_config", None) != settings.execution_config
        ):
            raise ResearchPaperApplicationError(_FAILURE_MESSAGE)
        _validate_committed_prefix(
            envelopes=envelopes,
            committed_cursor=state.committed_cursor,
            paper_run_id=settings.paper_run_id,
            broker=broker,
            coordinator=coordinator,
        )
        durable_output_bytes = _durable_prefix_bytes(
            envelopes=envelopes,
            committed_cursor=state.committed_cursor,
            broker=broker,
            coordinator=coordinator,
        )
        if state.status is ResearchRunStatus.COMPLETE:
            outbox = state_repository.read_outbox(settings.paper_run_id)
            if sum(len(record.payload) for record in outbox) > _MAX_OUTPUT_BYTES:
                raise ResearchPaperApplicationError(_FAILURE_MESSAGE)
            output = b"".join(record.payload for record in outbox)
            expected = materialize_research_jsonl(
                market_envelopes=envelopes,
                decision_records=coordinator.decision_records(),
                broker_snapshot=broker_snapshot,
                correlation=ResearchOutputCorrelation(
                    descriptor.session_id,
                    settings.paper_run_id,
                    broker_snapshot.execution_config_fingerprint,
                    descriptor.last_ingest_sequence,
                ),
                limits=_output_limits(settings),
            )
            if output != expected or source_path is None or _sidecars_exist(source_path):
                raise ResearchPaperApplicationError(_FAILURE_MESSAGE)
            result = _terminal_result(
                settings=settings,
                descriptor=descriptor,
                envelopes=envelopes,
                broker=broker,
                coordinator=coordinator,
                output=output,
            )
        else:
            sink = _DurableCoordinatorSink(
                coordinator=coordinator,
                broker=broker,
                repository=state_repository,
                settings=settings,
                envelopes=envelopes,
                state_version=state.state_version,
                committed_cursor=state.committed_cursor,
                durable_output_bytes=durable_output_bytes,
            )
            options = replace(settings.options, after_ingest_sequence=state.committed_cursor)
            runtime = ReplayRuntime(
                source=source,
                descriptor=descriptor,
                sink=sink,
                options=options,
                timer=timer,
            )
            suffix_snapshot = runtime.run()
            snapshot = broker.snapshot()
            decisions = coordinator.decision_records()
            if (
                suffix_snapshot.state is not ReplayState.COMPLETED
                or snapshot.last_committed_ingest_sequence != descriptor.last_ingest_sequence
                or len(decisions) != descriptor.event_count
            ):
                raise ResearchPaperApplicationError(_FAILURE_MESSAGE)
            _validate_committed_prefix(
                envelopes=envelopes,
                committed_cursor=descriptor.last_ingest_sequence,
                paper_run_id=settings.paper_run_id,
                broker=broker,
                coordinator=coordinator,
            )
            correlation = ResearchOutputCorrelation(
                descriptor.session_id,
                settings.paper_run_id,
                snapshot.execution_config_fingerprint,
                descriptor.last_ingest_sequence,
            )
            summary = encode_summary_record(
                market_record_count=descriptor.event_count,
                decision_record_count=len(decisions),
                broker_snapshot=snapshot,
                correlation=correlation,
            )
            if sink.durable_output_bytes + len(summary) > _MAX_OUTPUT_BYTES:
                raise ResearchPaperApplicationError(_FAILURE_MESSAGE)
            state_repository.complete_run(
                CompleteResearchRun(
                    paper_run_id=settings.paper_run_id,
                    expected_state_version=sink.state_version,
                    expected_previous_cursor=descriptor.last_ingest_sequence,
                    summary_record=ResearchOutboxRecord.create(
                        paper_run_id=settings.paper_run_id,
                        output_sequence=descriptor.event_count + len(snapshot.events),
                        record_type=ResearchOutboxRecordType.SUMMARY,
                        source_ingest_sequence=None,
                        paper_sequence=None,
                        payload=summary,
                    ),
                    completed_at=envelopes[0].received_at
                    + timedelta(microseconds=descriptor.event_count + 1),
                )
            )
            output = b"".join(
                record.payload for record in state_repository.read_outbox(settings.paper_run_id)
            )
            expected = materialize_research_jsonl(
                market_envelopes=envelopes,
                decision_records=decisions,
                broker_snapshot=snapshot,
                correlation=correlation,
                limits=_output_limits(settings),
            )
            if output != expected or source_path is None or _sidecars_exist(source_path):
                raise ResearchPaperApplicationError(_FAILURE_MESSAGE)
            result = _terminal_result(
                settings=settings,
                descriptor=descriptor,
                envelopes=envelopes,
                broker=broker,
                coordinator=coordinator,
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
        for repository in (state_repository, source_repository):
            if repository is not None:
                try:
                    repository.close()
                except (Exception, KeyboardInterrupt):
                    failed = True
        if source_guard_fd is not None:
            try:
                os.close(source_guard_fd)
            except OSError:
                failed = True
    if failed or result is None:
        raise ResearchPaperApplicationError(_FAILURE_MESSAGE) from None
    return result


def _output_limits(settings: ResearchPaperSettings) -> ResearchOutputLimits:
    return ResearchOutputLimits(
        max_market_records=settings.limits.max_market_data_records,
        max_paper_events=settings.limits.max_events,
        max_decision_records=settings.max_decision_records,
        max_output_bytes=_MAX_OUTPUT_BYTES,
    )


def run_research_paper_app(
    environment: Mapping[str, str] | None = None,
    *,
    timer: ReplayTimer | None = None,
    repository_factory: Callable[..., Any] = SQLiteMarketDataRepository,
    state_repository_factory: Callable[..., Any] = SQLiteResearchStateRepository,
) -> ResearchPaperResult:
    """Parse the research allowlist and run the isolated composition."""

    supplied = os.environ if environment is None else environment
    settings = parse_research_paper_settings(supplied)
    return run_research_paper(
        settings,
        timer=timer,
        repository_factory=repository_factory,
        state_repository_factory=state_repository_factory,
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
        written = stream.write(result.output)
        if written != len(result.output):
            raise OSError
        stream.flush()
    except (Exception, KeyboardInterrupt):
        print(_FAILURE_MESSAGE, file=sys.stderr)
        return 2
    print(_SUCCESS_MESSAGE, file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
