"""Safe Phase 1 composition root.

The default path is a deterministic offline recording. Live quote composition
is lazy and remains unreachable until the Phase 1 preset and explicit opt-in
have both validated.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from tx_trade.app.config import (
    Phase1Settings,
    QuoteSource,
    parse_phase1_settings,
)
from tx_trade.market_data.event_mapper import Phase1CapturedEventMapper
from tx_trade.market_data.fixtures import (
    OFFLINE_FIXTURE_TIME,
    OFFLINE_FIXTURE_TRADING_DAY,
    make_offline_fixture_envelopes,
)
from tx_trade.market_data.ingress import (
    BoundedIngress,
    BoundedIngressProcessor,
    BoundedStaQuoteQueue,
    PipelineStorageFailureNotifier,
)
from tx_trade.market_data.legacy_quote_snapshot import LegacyQuoteSnapshotProjector
from tx_trade.market_data.models import (
    SCHEMA_VERSION,
    MarketDataEnvelope,
    SourceMode,
    TAIPEI,
    to_primitive,
)
from tx_trade.market_data.pipeline import CapturedEventPipeline
from tx_trade.market_data.ports import RecordingSession
from tx_trade.monitoring.health import (
    ControlledShutdown,
    PipelineHealth,
    SessionImpactTracker,
)
from tx_trade.monitoring.metrics import IngressMetrics
from tx_trade.storage import (
    SQLiteMarketDataRepository,
    SQLiteMarketDataWriter,
    SQLiteReplaySource,
)

_PHASE1_CONFIG_KEYS = (
    "TX_TRADE_RUNTIME_PRESET",
    "TX_TRADE_QUOTE_SOURCE",
    "TX_TRADE_EXECUTION_MODE",
    "TX_TRADE_ENABLE_LIVE_QUOTE",
    "TX_TRADE_INGRESS_QUEUE_CAPACITY",
    "TX_TRADE_INGRESS_CONNECTION_CAPACITY",
    "TX_TRADE_INGRESS_DIAGNOSTIC_RESERVED_CAPACITY",
    "TX_TRADE_INGRESS_QUOTE_CAPACITY",
    "TX_TRADE_INGRESS_TICK_CAPACITY",
    "TX_TRADE_INGRESS_DEDUPE_CAPACITY",
    "TX_TRADE_STA_QUOTE_ENRICHMENT_CAPACITY",
    "TX_TRADE_STORAGE_WRITER_QUEUE_CAPACITY",
    "TX_TRADE_STORAGE_BATCH_SIZE",
    "TX_TRADE_STORAGE_FLUSH_INTERVAL_MS",
)
_DEFAULT_DB_PATH = "phase1_offline.sqlite3"


class Phase1RuntimeError(RuntimeError):
    """A non-sensitive application lifecycle failure."""


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(TAIPEI)

    def monotonic(self) -> float:
        return time.monotonic()


@dataclass(frozen=True, slots=True)
class Phase1Result:
    mode: str
    session_id: UUID
    status: str
    event_count: int
    integrity_valid: bool
    db_path: str


@dataclass(frozen=True, slots=True)
class Phase1Dependencies:
    repository_factory: Callable[[str | Path], Any] = SQLiteMarketDataRepository
    writer_factory: Callable[[Any, Phase1Settings, Any], Any] | None = None
    backend_factory: Callable[[], Any] | None = None
    adapter_factory: Callable[..., Any] | None = None
    clock: Any | None = None
    session_id_factory: Callable[[], UUID] = uuid4
    idle: Callable[[float], None] = time.sleep


@dataclass(frozen=True, slots=True)
class _LiveRuntime:
    account: str
    password: str
    dll_path: str
    symbols: tuple[str, ...]
    db_path: str
    ready_timeout: float
    stop_timeout: float


def _environment_settings(environment: Mapping[str, str]) -> dict[str, str]:
    values: dict[str, str] = {}
    for key in _PHASE1_CONFIG_KEYS:
        value = environment.get(key)
        if value is not None:
            values[key] = value
    return values


def _positive_float(environment: Mapping[str, str], key: str, default: float) -> float:
    raw = environment.get(key)
    if raw is None:
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise Phase1RuntimeError("invalid live runtime configuration") from exc
    if value <= 0 or value == float("inf") or value != value:
        raise Phase1RuntimeError("invalid live runtime configuration")
    return value


def _required(environment: Mapping[str, str], key: str) -> str:
    value = environment.get(key)
    if type(value) is not str or not value.strip():
        raise Phase1RuntimeError("live runtime configuration is incomplete")
    return value


def _live_runtime(environment: Mapping[str, str], db_override: str | None) -> _LiveRuntime:
    account = _required(environment, "TX_TRADE_ACCOUNT")
    password = _required(environment, "TX_TRADE_PASSWORD")
    dll_path = _required(environment, "TX_TRADE_SKCOM_DLL_PATH")
    raw_symbols = _required(environment, "TX_TRADE_SYMBOLS")
    symbols = tuple(item.strip() for item in raw_symbols.split(",") if item.strip())
    if not symbols:
        raise Phase1RuntimeError("live runtime configuration is incomplete")
    db_path = db_override or environment.get("TX_TRADE_RECORDING_DB_PATH", "phase1_live.sqlite3")
    if type(db_path) is not str or not db_path.strip():
        raise Phase1RuntimeError("live runtime configuration is incomplete")
    return _LiveRuntime(
        account=account,
        password=password,
        dll_path=dll_path,
        symbols=symbols,
        db_path=db_path,
        ready_timeout=_positive_float(environment, "TX_TRADE_LIVE_READY_TIMEOUT_SECONDS", 20.0),
        stop_timeout=_positive_float(environment, "TX_TRADE_LIVE_STOP_TIMEOUT_SECONDS", 10.0),
    )


def _fingerprint(values: Mapping[str, Any]) -> str:
    canonical = json.dumps(values, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _writer(
    dependencies: Phase1Dependencies,
    repository: Any,
    settings: Phase1Settings,
    notifier: Any,
) -> Any:
    if dependencies.writer_factory is not None:
        return dependencies.writer_factory(repository, settings, notifier)
    return SQLiteMarketDataWriter(
        repository,
        capacity=settings.storage_writer_queue_capacity,
        batch_size=settings.storage_batch_size,
        flush_interval_seconds=settings.storage_flush_interval_ms / 1000,
        notifier=notifier,
    )


def _offline_envelopes(session_id: UUID) -> tuple[MarketDataEnvelope, ...]:
    """Rebase canonical fixture content onto a fresh recording identity."""

    rebased: list[MarketDataEnvelope] = []
    for envelope in make_offline_fixture_envelopes():
        dedupe_key = _fingerprint(
            {
                "source": envelope.source,
                "session_id": str(session_id),
                "connection_generation": envelope.connection_generation,
                "event_type": envelope.event_type.value,
                "sequence": envelope.sequence,
                "broker_sequence": envelope.broker_sequence,
                "payload": to_primitive(envelope.payload),
                "raw_payload": to_primitive(envelope.raw_payload),
            }
        )
        rebased.append(
            replace(
                envelope,
                session_id=session_id,
                dedupe_key=f"{envelope.event_type.value}:{dedupe_key}",
            )
        )
    return tuple(rebased)


def run_offline(
    settings: Phase1Settings,
    *,
    db_path: str | Path = _DEFAULT_DB_PATH,
    dependencies: Phase1Dependencies = Phase1Dependencies(),
) -> Phase1Result:
    if settings.quote_source is not QuoteSource.OFFLINE:
        raise Phase1RuntimeError("offline runner requires the offline preset")
    session_id = dependencies.session_id_factory()
    if type(session_id) is not UUID:
        raise TypeError("session_id_factory must return UUID")
    envelopes = _offline_envelopes(session_id)
    repository: Any | None = None
    writer: Any | None = None
    session_started = False
    result: Phase1Result | None = None
    failure: BaseException | None = None
    try:
        repository = dependencies.repository_factory(db_path)
        repository.begin_session(
            RecordingSession(
                session_id=session_id,
                schema_version=SCHEMA_VERSION,
                source=envelopes[0].source,
                source_mode=SourceMode.OFFLINE,
                started_at=OFFLINE_FIXTURE_TIME,
                trading_day=OFFLINE_FIXTURE_TRADING_DAY,
                config_fingerprint=_fingerprint(
                    {"preset": settings.preset.value, "fixture": "canonical-v1"}
                ),
            )
        )
        session_started = True
        writer = _writer(dependencies, repository, settings, None)
        writer.start()
        for envelope in envelopes:
            writer.publish(envelope)
        writer.flush(timeout=5)
        writer.stop(timeout=5)
        replay = SQLiteReplaySource(repository)
        replay.open(session_id)
        report = replay.verify_integrity()
        if not report.is_valid or tuple(replay.iter_events()) != envelopes:
            raise Phase1RuntimeError("offline readback integrity failed")
        repository.end_session(session_id, OFFLINE_FIXTURE_TIME, "complete")
        session_started = False
        result = Phase1Result(
            mode="offline",
            session_id=session_id,
            status="complete",
            event_count=report.event_count,
            integrity_valid=True,
            db_path=str(db_path),
        )
    except BaseException as exc:
        failure = exc
        if writer is not None:
            try:
                writer.stop(timeout=5)
            except BaseException:
                pass
        if session_started and repository is not None:
            try:
                repository.end_session(session_id, OFFLINE_FIXTURE_TIME, "incomplete")
                session_started = False
            except BaseException:
                pass
    finally:
        if repository is not None:
            try:
                repository.close()
            except BaseException:
                failure = Phase1RuntimeError("offline repository close failed")
    if failure is not None:
        if isinstance(failure, Phase1RuntimeError):
            raise failure from None
        raise Phase1RuntimeError("offline recording failed") from None
    assert result is not None
    return result


def _default_backend_factory() -> Any:
    from tx_trade.broker.capital.com_backend import ComtypesQuoteBackend

    return ComtypesQuoteBackend()


def _default_adapter_factory(**kwargs: Any) -> Any:
    from tx_trade.broker.capital.quote_adapter import CapitalQuoteStaAdapter

    return CapitalQuoteStaAdapter(**kwargs)


def run_live(
    settings: Phase1Settings,
    runtime: _LiveRuntime,
    *,
    dependencies: Phase1Dependencies = Phase1Dependencies(),
    max_live_iterations: int | None = None,
) -> Phase1Result:
    if settings.quote_source is not QuoteSource.LIVE:
        raise Phase1RuntimeError("live runner requires the live quote preset")
    if max_live_iterations is not None and (
        type(max_live_iterations) is not int or max_live_iterations < 1
    ):
        raise ValueError("max_live_iterations must be a positive integer or None")

    clock = dependencies.clock or SystemClock()
    session_id = dependencies.session_id_factory()
    if type(session_id) is not UUID:
        raise TypeError("session_id_factory must return UUID")
    health = PipelineHealth(clock)
    metrics = IngressMetrics()
    impact = SessionImpactTracker(2)
    shutdown = ControlledShutdown()
    ingress = BoundedIngress(
        control_capacity=settings.ingress_control_capacity,
        diagnostic_capacity=settings.ingress_diagnostic_capacity,
        quote_capacity=settings.ingress_quote_capacity,
        tick_capacity=settings.ingress_tick_capacity,
        dedupe_capacity=settings.ingress_dedupe_capacity,
        health=health,
        metrics=metrics,
        session_impact=impact,
        shutdown=shutdown,
    )
    sta_queue = BoundedStaQuoteQueue(
        settings.sta_quote_enrichment_capacity,
        health=health,
        metrics=metrics,
        session_impact=impact,
        shutdown=shutdown,
        session_id=session_id,
    )

    repository: Any | None = None
    writer: Any | None = None
    adapter: Any | None = None
    session_started = False
    lifecycle_damaged = False
    event_count = 0
    failure: BaseException | None = None
    try:
        repository = dependencies.repository_factory(runtime.db_path)
        repository.begin_session(
            RecordingSession(
                session_id=session_id,
                schema_version=SCHEMA_VERSION,
                source="capital_skcom",
                source_mode=SourceMode.LIVE,
                started_at=clock.now(),
                trading_day=None,
                config_fingerprint=_fingerprint(
                    {
                        "preset": settings.preset.value,
                        "symbols": runtime.symbols,
                        "ingress_capacity": settings.ingress_queue_capacity,
                    }
                ),
            )
        )
        session_started = True
        notifier = PipelineStorageFailureNotifier(session_id, health, metrics, impact, shutdown)
        writer = _writer(dependencies, repository, settings, notifier)
        writer.start()
        projector = LegacyQuoteSnapshotProjector()
        processor = BoundedIngressProcessor(
            ingress,
            CapturedEventPipeline(Phase1CapturedEventMapper(), writer),
            health,
            metrics,
            impact,
            shutdown,
            accepted_event_observer=projector.project,
        )
        backend_factory = dependencies.backend_factory or _default_backend_factory
        adapter_factory = dependencies.adapter_factory or _default_adapter_factory
        backend = backend_factory()
        adapter = adapter_factory(
            backend=backend,
            dll_path=runtime.dll_path,
            ingress=ingress,
            sta_queue=sta_queue,
            clock=clock,
            health=health,
            session_impact=impact,
            shutdown=shutdown,
            command_capacity=64,
            command_timeout=5.0,
            pump_interval=0.01,
            session_id=session_id,
            source="capital_skcom",
        )
        adapter.start()
        adapter.login(runtime.account, runtime.password)
        adapter.enter_monitor()
        adapter.wait_until_ready(runtime.ready_timeout)
        adapter.subscribe_quotes(runtime.symbols)
        adapter.subscribe_ticks(runtime.symbols)

        iterations = 0
        while not shutdown.snapshot().is_requested:
            processed = processor.process_one()
            if processed:
                event_count += 1
            else:
                dependencies.idle(0.01)
            iterations += 1
            if max_live_iterations is not None and iterations >= max_live_iterations:
                break
    except KeyboardInterrupt as exc:
        lifecycle_damaged = True
        health.fail("live_interrupted")
        try:
            impact.mark_incomplete(session_id, "live_interrupted")
        except RuntimeError:
            health.fail("session_impact_capacity_exhausted")
        shutdown.request_shutdown("live_interrupted")
        failure = exc
    except Exception as exc:
        lifecycle_damaged = True
        health.fail("live_lifecycle_failure")
        try:
            impact.mark_incomplete(session_id, "live_lifecycle_failure")
        except RuntimeError:
            health.fail("session_impact_capacity_exhausted")
        shutdown.request_shutdown("live_lifecycle_failure")
        failure = exc
    finally:
        if adapter is not None:
            try:
                adapter.stop(runtime.stop_timeout)
            except (Exception, KeyboardInterrupt) as exc:
                lifecycle_damaged = True
                if isinstance(exc, KeyboardInterrupt):
                    failure = exc
                try:
                    impact.mark_incomplete(session_id, "adapter_stop_failure")
                except RuntimeError:
                    health.fail("session_impact_capacity_exhausted")
        processor_obj = locals().get("processor")
        if processor_obj is not None and not processor_obj.snapshot().is_halted:
            try:
                while processor_obj.process_one():
                    event_count += 1
            except (Exception, KeyboardInterrupt) as exc:
                lifecycle_damaged = True
                if isinstance(exc, KeyboardInterrupt):
                    failure = exc
        if writer is not None:
            try:
                writer.flush(timeout=runtime.stop_timeout)
            except (Exception, KeyboardInterrupt) as exc:
                lifecycle_damaged = True
                if isinstance(exc, KeyboardInterrupt):
                    failure = exc
                try:
                    impact.mark_incomplete(session_id, "writer_finalization_failure")
                except RuntimeError:
                    health.fail("session_impact_capacity_exhausted")
            try:
                writer.stop(timeout=runtime.stop_timeout)
            except (Exception, KeyboardInterrupt) as exc:
                lifecycle_damaged = True
                if isinstance(exc, KeyboardInterrupt):
                    failure = exc
                try:
                    impact.mark_incomplete(session_id, "writer_finalization_failure")
                except RuntimeError:
                    health.fail("session_impact_capacity_exhausted")
        integrity_valid = False
        if repository is not None and session_started:
            try:
                replay = SQLiteReplaySource(repository)
                replay.open(session_id)
                report = replay.verify_integrity()
                integrity_valid = report.is_valid
                event_count = report.event_count
                if not integrity_valid:
                    lifecycle_damaged = True
                    impact.mark_incomplete(session_id, "readback_integrity_failure")
            except (Exception, KeyboardInterrupt) as exc:
                lifecycle_damaged = True
                if isinstance(exc, KeyboardInterrupt):
                    failure = exc
                try:
                    impact.mark_incomplete(session_id, "readback_integrity_failure")
                except RuntimeError:
                    health.fail("session_impact_capacity_exhausted")
            shutdown_snapshot = shutdown.snapshot()
            if shutdown_snapshot.is_requested:
                lifecycle_damaged = True
                try:
                    impact.mark_incomplete(session_id, "controlled_shutdown_requested")
                except RuntimeError:
                    health.fail("session_impact_capacity_exhausted")
            if event_count == 0:
                lifecycle_damaged = True
                try:
                    impact.mark_incomplete(session_id, "empty_live_recording")
                except RuntimeError:
                    health.fail("session_impact_capacity_exhausted")
            requested = "incomplete" if lifecycle_damaged else "complete"
            status = impact.effective_terminal_status(session_id, requested)
            try:
                repository.end_session(session_id, clock.now(), status)
                session_started = False
            except (Exception, KeyboardInterrupt) as exc:
                status = "incomplete"
                lifecycle_damaged = True
                if isinstance(exc, KeyboardInterrupt):
                    failure = exc
                try:
                    repository.end_session(session_id, clock.now(), "incomplete")
                    session_started = False
                except BaseException:
                    pass
        else:
            status = "incomplete"
        if repository is not None:
            try:
                repository.close()
            except (Exception, KeyboardInterrupt) as exc:
                lifecycle_damaged = True
                if isinstance(exc, KeyboardInterrupt):
                    failure = exc

    if failure is not None or lifecycle_damaged or status != "complete" or not integrity_valid:
        raise Phase1RuntimeError("live quote recording failed") from None
    return Phase1Result(
        mode="live",
        session_id=session_id,
        status=status,
        event_count=event_count,
        integrity_valid=integrity_valid,
        db_path=runtime.db_path,
    )


def run_phase1(
    environment: Mapping[str, str] | None = None,
    *,
    db_path: str | None = None,
    dependencies: Phase1Dependencies = Phase1Dependencies(),
    max_live_iterations: int | None = None,
) -> Phase1Result:
    supplied = os.environ if environment is None else environment
    settings = parse_phase1_settings(_environment_settings(supplied))
    if settings.quote_source is QuoteSource.OFFLINE:
        return run_offline(
            settings,
            db_path=db_path or supplied.get("TX_TRADE_RECORDING_DB_PATH", _DEFAULT_DB_PATH),
            dependencies=dependencies,
        )
    runtime = _live_runtime(supplied, db_path)
    return run_live(
        settings,
        runtime,
        dependencies=dependencies,
        max_live_iterations=max_live_iterations,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Safe Phase 1 market-data recorder")
    parser.add_argument("--db", dest="db_path")
    try:
        args = parser.parse_args(argv)
        result = run_phase1(db_path=args.db_path)
    except (Exception, KeyboardInterrupt):
        print("Phase 1 recorder failed safely.", file=sys.stderr)
        return 2
    print(
        f"Phase 1 {result.mode} recording {result.status}: "
        f"{result.event_count} verified events in {result.db_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
