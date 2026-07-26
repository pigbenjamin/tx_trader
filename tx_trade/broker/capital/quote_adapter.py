"""Dedicated-STA Capital quote adapter."""

from __future__ import annotations

from collections.abc import Sequence
from concurrent.futures import Future, TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from datetime import datetime
import math
from queue import Empty, Full, Queue
from threading import Condition, Event, Lock, Thread, get_ident
from typing import Any
from uuid import UUID, uuid4

from tx_trade.market_data.ingress import (
    BoundedIngress,
    BoundedStaQuoteQueue,
    StaIngressDecision,
)
from tx_trade.market_data.models import (
    CapturedAdapterDiagnostic,
    CapturedConnectionNotification,
    CapturedKind,
    CapturedMarketDataEvent,
    CapturedQuoteSnapshot,
    CapturedServerTimeNotification,
    CapturedStockListNotification,
    CapturedTickNotification,
    ConnectionState,
    ConnectionStatus,
    SourceMode,
    StaLocalQuoteNotification,
    TAIPEI,
    build_adapter_diagnostic_dedupe_key,
)
from tx_trade.market_data.ports import Clock
from tx_trade.monitoring.health import (
    ControlledShutdown,
    PipelineHealth,
    SessionImpactTracker,
)

from .contracts import (
    AdapterSnapshot,
    AdapterStoppedError,
    AuthenticationError,
    CapitalAdapterError,
    CommandQueueFullError,
    LiveQuoteInitializationError,
    MonitorError,
    QuoteComBackend,
    ReadyTimeoutError,
    ReconnectPolicy,
    SubscriptionError,
)

_CONNECTED = 3001
_DISCONNECTED = 3002
_STOCKS_READY = 3003
_READY_STATES = {ConnectionState.STOCKS_READY, ConnectionState.SUBSCRIBED}


@dataclass(frozen=True, slots=True)
class _Command:
    operation: str
    values: tuple[str, ...]
    future: Future[None]

    def __repr__(self) -> str:
        return f"_Command(operation={self.operation!r}, values=<redacted>)"


class _GenerationEventSink:
    """Callback boundary permanently bound to one connection generation."""

    __slots__ = ("_adapter", "_generation", "_lock", "_sequence")

    def __init__(
        self, adapter: "CapitalQuoteStaAdapter", generation: int
    ) -> None:
        self._adapter = adapter
        self._generation = generation
        self._sequence = 0
        self._lock = Lock()

    @property
    def generation(self) -> int:
        return self._generation

    def _invoke(self, callback_name: str, *values: object) -> None:
        with self._lock:
            sequence = self._sequence
            self._sequence += 1
        try:
            handler = getattr(self._adapter, f"_handle_{callback_name}")
            handler(self._generation, sequence, *values)
        except Exception:
            try:
                self._adapter._callback_failure(
                    self._generation, callback_name
                )
            except Exception:
                pass
        return None

    def OnConnection(self, nKind: int, nCode: int) -> None:
        return self._invoke("connection", nKind, nCode)

    def OnNotifyServerTime(
        self, sHour: int, sMinute: int, sSecond: int, nTotal: int
    ) -> None:
        return self._invoke(
            "server_time", sHour, sMinute, sSecond, nTotal
        )

    def OnNotifyStockList(
        self, sMarketNo: int, bstrStockData: str | bytes
    ) -> None:
        return self._invoke("stock_list", sMarketNo, bstrStockData)

    def OnNotifyQuote(self, sMarketNo: int, sStockIdx: int) -> None:
        return self._invoke("quote", sMarketNo, sStockIdx, False)

    def OnNotifyQuoteLONG(self, sMarketNo: int, nStockIdx: int) -> None:
        return self._invoke("quote", sMarketNo, nStockIdx, True)

    def OnNotifyTicks(
        self,
        sMarketNo: int,
        sStockIdx: int,
        nPtr: int,
        nDate: int,
        nTimehms: int,
        nTimemillismicros: int,
        nBid: int,
        nAsk: int,
        nClose: int,
        nQty: int,
        nSimulate: int,
    ) -> None:
        return self._invoke(
            "tick",
            sMarketNo,
            sStockIdx,
            nPtr,
            nDate,
            nTimehms,
            nTimemillismicros,
            nBid,
            nAsk,
            nClose,
            nQty,
            nSimulate,
            False,
        )

    def OnNotifyTicksLONG(
        self,
        sMarketNo: int,
        nStockIdx: int,
        nPtr: int,
        nDate: int,
        nTimehms: int,
        nTimemillismicros: int,
        nBid: int,
        nAsk: int,
        nClose: int,
        nQty: int,
        nSimulate: int,
    ) -> None:
        return self._invoke(
            "tick",
            sMarketNo,
            nStockIdx,
            nPtr,
            nDate,
            nTimehms,
            nTimemillismicros,
            nBid,
            nAsk,
            nClose,
            nQty,
            nSimulate,
            True,
        )


_TRANSITIONS: dict[ConnectionState, frozenset[ConnectionState]] = {
    ConnectionState.NEW: frozenset(
        (
            ConnectionState.STARTING,
            ConnectionState.ERROR,
            ConnectionState.STOPPING,
        )
    ),
    ConnectionState.STARTING: frozenset(
        (
            ConnectionState.COM_READY,
            ConnectionState.ERROR,
            ConnectionState.STOPPING,
        )
    ),
    ConnectionState.COM_READY: frozenset(
        (
            ConnectionState.LOGGING_IN,
            ConnectionState.ERROR,
            ConnectionState.STOPPING,
        )
    ),
    ConnectionState.LOGGING_IN: frozenset(
        (
            ConnectionState.LOGGED_IN,
            ConnectionState.ERROR,
            ConnectionState.STOPPING,
        )
    ),
    ConnectionState.LOGGED_IN: frozenset(
        (
            ConnectionState.ENTERING_MONITOR,
            ConnectionState.ERROR,
            ConnectionState.STOPPING,
        )
    ),
    ConnectionState.ENTERING_MONITOR: frozenset(
        (
            ConnectionState.CONNECTED,
            ConnectionState.ERROR,
            ConnectionState.STOPPING,
        )
    ),
    ConnectionState.CONNECTED: frozenset(
        (
            ConnectionState.STOCKS_READY,
            ConnectionState.DISCONNECTED,
            ConnectionState.ERROR,
            ConnectionState.STOPPING,
        )
    ),
    ConnectionState.STOCKS_READY: frozenset(
        (
            ConnectionState.SUBSCRIBED,
            ConnectionState.DISCONNECTED,
            ConnectionState.ERROR,
            ConnectionState.STOPPING,
        )
    ),
    ConnectionState.SUBSCRIBED: frozenset(
        (
            ConnectionState.STOCKS_READY,
            ConnectionState.DISCONNECTED,
            ConnectionState.ERROR,
            ConnectionState.STOPPING,
        )
    ),
    ConnectionState.DISCONNECTED: frozenset(
        (ConnectionState.RECONNECTING, ConnectionState.STOPPING)
    ),
    ConnectionState.RECONNECTING: frozenset(
        (
            ConnectionState.CONNECTED,
            ConnectionState.ERROR,
            ConnectionState.STOPPING,
        )
    ),
    ConnectionState.ERROR: frozenset((ConnectionState.STOPPING,)),
    ConnectionState.STOPPING: frozenset((ConnectionState.STOPPED,)),
    ConnectionState.STOPPED: frozenset(),
}


class CapitalQuoteStaAdapter:
    """Runs all backend activity and event callbacks on one daemon STA thread."""

    def __init__(
        self,
        *,
        backend: QuoteComBackend,
        dll_path: str,
        ingress: BoundedIngress,
        sta_queue: BoundedStaQuoteQueue,
        clock: Clock,
        health: PipelineHealth,
        session_impact: SessionImpactTracker,
        shutdown: ControlledShutdown,
        command_capacity: int,
        command_timeout: float,
        pump_interval: float,
        startup_timeout: float | None = None,
        command_batch_size: int = 8,
        reconnect_connected_timeout: float | None = None,
        reconnect_stocks_ready_timeout: float | None = None,
        reconnect_policy: ReconnectPolicy = ReconnectPolicy(),
        quote_lookup_attempts: int = 3,
        session_id: UUID | None = None,
        source: str = "capital_skcom",
    ) -> None:
        if not isinstance(backend, QuoteComBackend):
            raise TypeError("backend does not implement QuoteComBackend")
        if type(dll_path) is not str or not dll_path.strip():
            raise ValueError("dll_path must not be empty")
        for name, value in (
            ("command_capacity", command_capacity),
            ("command_batch_size", command_batch_size),
            ("quote_lookup_attempts", quote_lookup_attempts),
        ):
            if type(value) is not int or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        for name, value in (
            ("command_timeout", command_timeout),
            ("pump_interval", pump_interval),
        ):
            if (
                type(value) not in (int, float)
                or not math.isfinite(value)
                or value <= 0
            ):
                raise ValueError(f"{name} must be positive")
        if startup_timeout is None:
            startup_timeout = float(command_timeout)
        if (
            type(startup_timeout) not in (int, float)
            or not math.isfinite(startup_timeout)
            or startup_timeout <= 0
        ):
            raise ValueError("startup_timeout must be positive and finite")
        if reconnect_connected_timeout is None:
            reconnect_connected_timeout = float(command_timeout)
        if reconnect_stocks_ready_timeout is None:
            reconnect_stocks_ready_timeout = float(command_timeout)
        for name, value in (
            ("reconnect_connected_timeout", reconnect_connected_timeout),
            (
                "reconnect_stocks_ready_timeout",
                reconnect_stocks_ready_timeout,
            ),
        ):
            if (
                type(value) not in (int, float)
                or not math.isfinite(value)
                or value <= 0
            ):
                raise ValueError(f"{name} must be positive and finite")
        if session_id is None:
            session_id = uuid4()
        if type(session_id) is not UUID:
            raise TypeError("session_id must be UUID")
        if type(source) is not str or not source.strip():
            raise ValueError("source must not be empty")
        self._backend = backend
        self._dll_path = dll_path
        self._ingress = ingress
        self._sta_queue = sta_queue
        self._clock = clock
        self._health = health
        self._impact = session_impact
        self._shutdown = shutdown
        self._command_timeout = float(command_timeout)
        self._startup_timeout = float(startup_timeout)
        self._command_batch_size = command_batch_size
        self._reconnect_connected_timeout = float(
            reconnect_connected_timeout
        )
        self._reconnect_stocks_ready_timeout = float(
            reconnect_stocks_ready_timeout
        )
        self._pump_interval = float(pump_interval)
        self._reconnect_policy = reconnect_policy
        self._quote_lookup_attempts = quote_lookup_attempts
        self._session_id = session_id
        self._source = source
        self._commands: Queue[_Command] = Queue(maxsize=command_capacity)
        self._stop_event = Event()
        self._started_event = Event()
        self._state_condition = Condition(Lock())
        self._thread: Thread | None = None
        self._thread_id: int | None = None
        self._state = ConnectionState.NEW
        self._generation = 0
        self._callback_sequence = 0
        self._last_kind: int | None = None
        self._last_code: int | None = None
        self._desired_quotes: set[str] = set()
        self._actual_quotes: set[str] = set()
        self._desired_ticks: set[str] = set()
        self._actual_ticks: set[str] = set()
        self._accepting_commands = False
        self._initialization_error: LiveQuoteInitializationError | None = None
        self._reconnect_attempts = 0
        self._reconnect_due: float | None = None
        self._connected_deadline: float | None = None
        self._stocks_ready_deadline: float | None = None
        self._resubscribe_generation: int | None = None
        self._resubscribe_attempts = 0
        self._resubscribe_due: float | None = None
        self._co_initialized = False
        self._objects_initialized = False
        self._events_registered = False
        self._monitor_active = False
        self._current_sink = _GenerationEventSink(self, 0)
        self._quote_generations: dict[int, int] = {}
        sta_snapshot = sta_queue.snapshot()
        self._quote_generation_capacity = (
            sta_snapshot.main_capacity + sta_snapshot.overflow_capacity
        )

    def start(self) -> None:
        with self._state_condition:
            if self._state is not ConnectionState.NEW:
                raise CapitalAdapterError("adapter can only be started once")
            self._transition_locked(ConnectionState.STARTING)
            self._accepting_commands = True
            self._thread = Thread(
                target=self._run, name="capital-quote-sta", daemon=True
            )
            self._thread.start()
        if not self._started_event.wait(self._startup_timeout):
            self._fatal("quote_startup_timeout")
            raise LiveQuoteInitializationError("quote adapter startup timed out")
        if self._initialization_error is not None:
            raise self._initialization_error

    def login(self, account: str, password: str) -> None:
        if type(account) is not str or not account.strip():
            raise ValueError("account must not be empty")
        if type(password) is not str or not password:
            raise ValueError("password must not be empty")
        self._submit("login", (account, password))

    def enter_monitor(self) -> None:
        self._submit("enter_monitor", ())

    def wait_until_ready(self, timeout_seconds: float) -> ConnectionStatus:
        if type(timeout_seconds) not in (int, float) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        deadline = self._clock.monotonic() + float(timeout_seconds)
        timed_out = False
        with self._state_condition:
            while self._state not in _READY_STATES:
                if self._state in (
                    ConnectionState.ERROR,
                    ConnectionState.STOPPING,
                    ConnectionState.STOPPED,
                ):
                    raise AdapterStoppedError("adapter cannot become ready")
                remaining = deadline - self._clock.monotonic()
                if remaining <= 0:
                    timed_out = True
                    break
                self._state_condition.wait(min(remaining, self._pump_interval))
            if not timed_out:
                return self._status_locked()
        self._fatal("quote_ready_timeout")
        raise ReadyTimeoutError("quote service readiness timed out")

    def subscribe_quotes(self, symbols: Sequence[str]) -> None:
        self._submit("subscribe_quotes", self._symbols(symbols))

    def subscribe_ticks(self, symbols: Sequence[str]) -> None:
        self._submit("subscribe_ticks", self._symbols(symbols))

    def unsubscribe_quotes(self, symbols: Sequence[str]) -> None:
        self._submit("unsubscribe_quotes", self._symbols(symbols))

    def unsubscribe_ticks(self, symbols: Sequence[str]) -> None:
        self._submit("unsubscribe_ticks", self._symbols(symbols))

    def stop(self, timeout_seconds: float) -> None:
        if type(timeout_seconds) not in (int, float) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        with self._state_condition:
            if self._state is ConnectionState.STOPPED:
                return
            self._accepting_commands = False
            self._connected_deadline = None
            self._stocks_ready_deadline = None
            self._stop_event.set()
            thread = self._thread
            if thread is None:
                if self._state is ConnectionState.NEW:
                    self._transition_locked(ConnectionState.STOPPING)
                    self._transition_locked(ConnectionState.STOPPED)
                return
            self._state_condition.notify_all()
        thread.join(float(timeout_seconds))
        if thread.is_alive():
            raise TimeoutError("adapter stop timed out")

    def snapshot(self) -> AdapterSnapshot:
        with self._state_condition:
            return AdapterSnapshot(
                self._state,
                self._generation,
                self._callback_sequence,
                self._last_kind,
                self._last_code,
                frozenset(self._desired_quotes),
                frozenset(self._actual_quotes),
                frozenset(self._desired_ticks),
                frozenset(self._actual_ticks),
                self._thread_id,
                self._reconnect_attempts,
                self._accepting_commands,
            )

    @staticmethod
    def _symbols(symbols: Sequence[str]) -> tuple[str, ...]:
        if isinstance(symbols, (str, bytes)) or not isinstance(symbols, Sequence):
            raise TypeError("symbols must be a sequence of strings")
        result: set[str] = set()
        for symbol in symbols:
            if type(symbol) is not str:
                raise TypeError("each symbol must be a string")
            if not symbol.strip() or "," in symbol:
                raise ValueError("symbols must be non-empty and must not contain commas")
            result.add(symbol)
        if not result:
            raise ValueError("symbols must not be empty")
        return tuple(sorted(result))

    def _submit(self, operation: str, values: tuple[str, ...]) -> None:
        future: Future[None] = Future()
        command = _Command(operation, values, future)
        with self._state_condition:
            if not self._accepting_commands:
                raise AdapterStoppedError("adapter is stopped")
            try:
                self._commands.put_nowait(command)
            except Full as exc:
                raise CommandQueueFullError("adapter command queue is full") from exc
        try:
            future.result(timeout=self._command_timeout)
        except FutureTimeoutError as exc:
            future.cancel()
            raise TimeoutError("adapter command timed out") from exc

    def _run(self) -> None:
        self._thread_id = get_ident()
        try:
            self._backend.co_initialize()
            self._co_initialized = True
            if self._stop_event.is_set():
                raise LiveQuoteInitializationError("quote adapter startup cancelled")
            self._backend.initialize(self._dll_path)
            self._objects_initialized = True
            if self._stop_event.is_set():
                raise LiveQuoteInitializationError("quote adapter startup cancelled")
            self._backend.register_events(self._current_sink)
            self._events_registered = True
            if self._stop_event.is_set():
                raise LiveQuoteInitializationError("quote adapter startup cancelled")
            with self._state_condition:
                self._transition_locked(ConnectionState.COM_READY)
        except Exception as exc:
            error = (
                exc
                if isinstance(exc, LiveQuoteInitializationError)
                else LiveQuoteInitializationError("quote adapter initialization failed")
            )
            if error is not exc:
                error.__cause__ = exc
            self._initialization_error = error
            self._fatal("quote_initialization_failure")
        finally:
            self._started_event.set()
        try:
            while (
                self._initialization_error is None
                and not self._stop_event.is_set()
            ):
                self._process_commands()
                if self._stop_event.is_set():
                    break
                self._backend.pump_waiting_messages()
                if self._stop_event.is_set():
                    break
                self._drain_sta_quotes()
                self._run_resubscribe()
                self._run_reconnect_watchdog()
                self._run_reconnect()
                self._stop_event.wait(self._pump_interval)
        except Exception:
            self._fatal("quote_sta_runtime_failure")
        finally:
            self._cleanup()

    def _process_commands(self) -> None:
        processed = 0
        while (
            not self._stop_event.is_set()
            and processed < self._command_batch_size
        ):
            try:
                command = self._commands.get_nowait()
            except Empty:
                return
            if not command.future.set_running_or_notify_cancel():
                continue
            processed += 1
            try:
                self._execute(command)
            except Exception as exc:
                command.future.set_exception(exc)
            else:
                command.future.set_result(None)

    def _execute(self, command: _Command) -> None:
        operation = command.operation
        if operation == "login":
            self._login(command.values[0], command.values[1])
        elif operation == "enter_monitor":
            self._enter_monitor(False)
        elif operation == "subscribe_quotes":
            self._subscribe("quotes", command.values)
        elif operation == "subscribe_ticks":
            self._subscribe("ticks", command.values)
        elif operation == "unsubscribe_quotes":
            self._unsubscribe("quotes", command.values)
        elif operation == "unsubscribe_ticks":
            self._unsubscribe("ticks", command.values)
        else:
            raise CapitalAdapterError("unsupported adapter command")

    def _login(self, account: str, password: str) -> None:
        with self._state_condition:
            if self._state is not ConnectionState.COM_READY:
                raise CapitalAdapterError("adapter is not ready for login")
            self._transition_locked(ConnectionState.LOGGING_IN)
        try:
            code = self._code(self._backend.login(account, password), "login")
        except Exception:
            self._fatal("authentication_failure")
            raise AuthenticationError("authentication failed") from None
        if code != 0:
            self._fatal("authentication_failure")
            raise AuthenticationError(f"authentication failed with code {code}")
        with self._state_condition:
            self._last_code = code
            self._transition_locked(ConnectionState.LOGGED_IN)

    def _enter_monitor(self, reconnect: bool) -> None:
        with self._state_condition:
            expected = (
                ConnectionState.RECONNECTING
                if reconnect
                else ConnectionState.LOGGED_IN
            )
            if self._state is not expected:
                raise CapitalAdapterError("adapter is not ready to enter monitor")
            if not reconnect:
                self._transition_locked(ConnectionState.ENTERING_MONITOR)
            self._generation += 1
            self._callback_sequence = 0
            self._actual_quotes.clear()
            self._actual_ticks.clear()
            self._resubscribe_generation = None
            self._monitor_active = True
            new_sink = _GenerationEventSink(self, self._generation)
        if self._events_registered:
            try:
                self._backend.release_events()
            except Exception:
                pass
            self._events_registered = False
        try:
            self._backend.register_events(new_sink)
        except Exception:
            if reconnect:
                raise MonitorError("quote event registration failed") from None
            self._fatal("monitor_entry_failure")
            raise MonitorError("quote event registration failed") from None
        self._events_registered = True
        self._current_sink = new_sink
        try:
            code = self._code(self._backend.enter_monitor(), "enter monitor")
        except Exception:
            if reconnect:
                raise MonitorError("monitor entry failed") from None
            self._fatal("monitor_entry_failure")
            raise MonitorError("monitor entry failed") from None
        if code != 0:
            if reconnect:
                raise MonitorError(f"monitor entry failed with code {code}")
            self._fatal("monitor_entry_failure")
            raise MonitorError(f"monitor entry failed with code {code}")
        if self._stop_event.is_set():
            raise AdapterStoppedError("callback boundary stopped adapter")
        with self._state_condition:
            self._last_code = code

    def _subscribe(self, kind: str, symbols: tuple[str, ...]) -> None:
        with self._state_condition:
            desired = (
                self._desired_quotes if kind == "quotes" else self._desired_ticks
            )
            actual = self._actual_quotes if kind == "quotes" else self._actual_ticks
            desired.update(symbols)
            ready = self._state in _READY_STATES
            pending = tuple(sorted(desired - actual))
            if ready and pending:
                if self._state is ConnectionState.SUBSCRIBED:
                    self._transition_locked(ConnectionState.STOCKS_READY)
                self._resubscribe_generation = self._generation
                self._resubscribe_attempts = 0
                self._resubscribe_due = self._clock.monotonic()
        if ready and pending:
            try:
                self._request_batch(kind, pending)
            except SubscriptionError:
                self._schedule_resubscribe_failure()
                raise

    def _request_batch(self, kind: str, symbols: tuple[str, ...]) -> None:
        method = (
            self._backend.request_quotes
            if kind == "quotes"
            else self._backend.request_ticks
        )
        try:
            code = self._code(method(",".join(symbols)), "subscription")
        except Exception as exc:
            self._health.degrade("subscription_failure")
            raise SubscriptionError("subscription failed") from exc
        if code != 0:
            self._health.degrade("subscription_failure")
            raise SubscriptionError(f"subscription failed with code {code}")
        with self._state_condition:
            actual = self._actual_quotes if kind == "quotes" else self._actual_ticks
            actual.update(symbols)
            self._refresh_subscription_state_locked()

    def _unsubscribe(self, kind: str, symbols: tuple[str, ...]) -> None:
        with self._state_condition:
            desired = (
                self._desired_quotes if kind == "quotes" else self._desired_ticks
            )
            actual = self._actual_quotes if kind == "quotes" else self._actual_ticks
            requested = tuple(sorted(set(symbols) & actual))
            desired_only = set(symbols) - actual
            if not requested:
                desired.difference_update(desired_only)
                return
        method = (
            self._backend.cancel_quotes
            if kind == "quotes"
            else self._backend.cancel_ticks
        )
        try:
            code = self._code(method(",".join(requested)), "cancellation")
        except Exception as exc:
            self._health.degrade("subscription_cancellation_failure")
            raise SubscriptionError("subscription cancellation failed") from exc
        if code != 0:
            self._health.degrade("subscription_cancellation_failure")
            raise SubscriptionError(
                f"subscription cancellation failed with code {code}"
            )
        with self._state_condition:
            desired.difference_update(symbols)
            actual.difference_update(symbols)
            if not self._desired_quotes and not self._desired_ticks:
                self._resubscribe_generation = None
                self._resubscribe_due = None
            if self._state is ConnectionState.SUBSCRIBED and not (
                self._desired_quotes or self._desired_ticks
            ):
                self._transition_locked(ConnectionState.STOCKS_READY)
            else:
                self._refresh_subscription_state_locked()

    # Compatibility seam for deterministic tests; production registration uses
    # the generation-bound sink object above.
    def OnConnection(self, nKind: int, nCode: int) -> None:
        return self._current_sink.OnConnection(nKind, nCode)

    def OnNotifyServerTime(
        self, sHour: int, sMinute: int, sSecond: int, nTotal: int
    ) -> None:
        return self._current_sink.OnNotifyServerTime(
            sHour, sMinute, sSecond, nTotal
        )

    def OnNotifyStockList(
        self, sMarketNo: int, bstrStockData: str | bytes
    ) -> None:
        return self._current_sink.OnNotifyStockList(
            sMarketNo, bstrStockData
        )

    def OnNotifyQuote(self, sMarketNo: int, sStockIdx: int) -> None:
        return self._current_sink.OnNotifyQuote(sMarketNo, sStockIdx)

    def OnNotifyQuoteLONG(self, sMarketNo: int, nStockIdx: int) -> None:
        return self._current_sink.OnNotifyQuoteLONG(sMarketNo, nStockIdx)

    def OnNotifyTicks(self, *values: int) -> None:
        return self._current_sink.OnNotifyTicks(*values)

    def OnNotifyTicksLONG(self, *values: int) -> None:
        return self._current_sink.OnNotifyTicksLONG(*values)

    def _handle_connection(
        self, generation: int, sequence: int, nKind: int, nCode: int
    ) -> None:
        self._strict_callback_ints((nKind, nCode))
        received_at = self._handler_time(generation, sequence)
        with self._state_condition:
            is_current = generation == self._generation
            if is_current:
                self._last_kind = nKind
                self._last_code = nCode
            current = self._state
            target: ConnectionState | None = None
            if not is_current:
                pass
            elif nKind == _CONNECTED:
                if current in (
                    ConnectionState.ENTERING_MONITOR,
                    ConnectionState.RECONNECTING,
                ):
                    target = ConnectionState.CONNECTED
                    self._connected_deadline = None
                    if current is ConnectionState.RECONNECTING:
                        self._stocks_ready_deadline = (
                            self._clock.monotonic()
                            + self._reconnect_stocks_ready_timeout
                        )
            elif nKind == _STOCKS_READY:
                if current is ConnectionState.CONNECTED:
                    target = ConnectionState.STOCKS_READY
                    self._connected_deadline = None
                    self._stocks_ready_deadline = None
                    self._reconnect_attempts = 0
                    self._resubscribe_generation = generation
                    self._resubscribe_attempts = 0
                    self._resubscribe_due = self._clock.monotonic()
            elif nKind == _DISCONNECTED:
                if current in (
                    ConnectionState.CONNECTED,
                    ConnectionState.STOCKS_READY,
                    ConnectionState.SUBSCRIBED,
                ):
                    target = ConnectionState.DISCONNECTED
                    in_reconnect_attempt = (
                        self._stocks_ready_deadline is not None
                    )
                    self._connected_deadline = None
                    self._stocks_ready_deadline = None
            if target is not None:
                self._transition_locked(target)
                if target is ConnectionState.DISCONNECTED:
                    self._transition_locked(ConnectionState.RECONNECTING)
                    self._actual_quotes.clear()
                    self._actual_ticks.clear()
                    if not in_reconnect_attempt:
                        self._reconnect_attempts = 0
                    self._reconnect_due = (
                        self._clock.monotonic()
                        + self._reconnect_policy.backoff_seconds[0]
                    )
            elif is_current and nKind in (
                _CONNECTED,
                _STOCKS_READY,
                _DISCONNECTED,
            ):
                self._emit_diagnostic(
                    "adapter_error",
                    sequence,
                    received_at,
                    generation,
                    message="invalid connection transition",
                    error_code=nCode,
                    raw={"kind": nKind, "code": nCode},
                )
                self._health.degrade("invalid_connection_transition")
        payload = CapturedConnectionNotification(
            nKind, nCode, sequence, received_at
        )
        self._publish(
            CapturedKind.CONNECTION_NOTIFICATION,
            payload,
            sequence,
            received_at,
            generation,
            {"kind": nKind, "code": nCode},
        )

    def _handle_server_time(
        self,
        generation: int,
        sequence: int,
        sHour: int,
        sMinute: int,
        sSecond: int,
        nTotal: int,
    ) -> None:
        self._strict_callback_ints((sHour, sMinute, sSecond, nTotal))
        received_at = self._handler_time(generation, sequence)
        payload = CapturedServerTimeNotification(
            sHour, sMinute, sSecond, nTotal, sequence, received_at
        )
        self._publish(
            CapturedKind.SERVER_TIME_NOTIFICATION,
            payload,
            sequence,
            received_at,
            generation,
            {
                "hour": sHour,
                "minute": sMinute,
                "second": sSecond,
                "total": nTotal,
            },
        )

    def _handle_stock_list(
        self,
        generation: int,
        sequence: int,
        sMarketNo: int,
        bstrStockData: str | bytes,
    ) -> None:
        self._strict_callback_ints((sMarketNo,))
        if type(bstrStockData) not in (str, bytes):
            raise TypeError("stock list callback data must be str or bytes")
        received_at = self._handler_time(generation, sequence)
        payload = CapturedStockListNotification(
            sMarketNo, bstrStockData, sequence, received_at
        )
        self._publish(
            CapturedKind.STOCK_LIST_NOTIFICATION,
            payload,
            sequence,
            received_at,
            generation,
            {
                "market_no": sMarketNo,
                "stock_list": (
                    bstrStockData if type(bstrStockData) is str else None
                ),
                "stock_list_is_bytes": type(bstrStockData) is bytes,
            },
        )

    def _handle_quote(
        self,
        generation: int,
        sequence: int,
        market_no: int,
        stock_idx: int,
        is_long: bool,
    ) -> None:
        self._strict_callback_ints((market_no, stock_idx))
        if type(is_long) is not bool:
            raise TypeError("is_long must be bool")
        received_at = self._handler_time(generation, sequence)
        notification = StaLocalQuoteNotification(
            market_no, stock_idx, is_long, sequence, received_at
        )
        before = self._sta_queue.snapshot().total_depth
        decision = self._sta_queue.try_publish(notification)
        after = self._sta_queue.snapshot().total_depth
        retained = decision is StaIngressDecision.ACCEPTED or after > before
        if retained:
            if len(self._quote_generations) >= self._quote_generation_capacity:
                raise RuntimeError("quote generation sidecar capacity exhausted")
            self._quote_generations[id(notification)] = generation
        elif decision is StaIngressDecision.OVERFLOW:
            raise RuntimeError("quote handoff capacity exhausted")

    def _handle_tick(
        self,
        generation: int,
        sequence: int,
        sMarketNo: int,
        sStockIdx: int,
        nPtr: int,
        nDate: int,
        nTimehms: int,
        nTimemillismicros: int,
        nBid: int,
        nAsk: int,
        nClose: int,
        nQty: int,
        nSimulate: int,
        is_long: bool,
    ) -> None:
        self._capture_tick_bound(
            generation,
            sequence,
            sMarketNo,
            sStockIdx,
            nPtr,
            nDate,
            nTimehms,
            nTimemillismicros,
            nBid,
            nAsk,
            nClose,
            nQty,
            nSimulate,
            is_long,
        )

    def _capture_tick_bound(
        self, generation: int, sequence: int, *values: Any
    ) -> None:
        raw, is_long = values[:-1], values[-1]
        self._strict_callback_ints(raw)
        if type(is_long) is not bool:
            raise TypeError("is_long must be bool")
        received_at = self._handler_time(generation, sequence)
        payload = CapturedTickNotification(
            *raw, is_long, sequence, received_at
        )
        names = (
            "market_no",
            "stock_idx",
            "source_pointer",
            "date",
            "time_hms",
            "time_subsecond",
            "bid",
            "ask",
            "close",
            "quantity",
            "simulate",
        )
        raw_payload = dict(zip(names, raw, strict=True))
        raw_payload["is_long_callback"] = is_long
        self._publish(
            CapturedKind.TICK_NOTIFICATION,
            payload,
            sequence,
            received_at,
            generation,
            raw_payload,
        )

    def _drain_sta_quotes(self) -> None:
        overflow = self._sta_queue.try_pop_overflow()
        if overflow is not None:
            generation = self._pop_quote_generation(overflow)
            if generation is None:
                return
            self._emit_diagnostic(
                "adapter_error",
                overflow.callback_sequence,
                overflow.received_at,
                generation,
                message="sta quote notification overflow",
                raw={
                    "market_no": overflow.market_no_raw,
                    "stock_idx": overflow.stock_idx_raw,
                    "is_long_callback": overflow.is_long_callback,
                },
            )
        while True:
            notification = self._sta_queue.try_pop()
            if notification is None:
                return
            generation = self._pop_quote_generation(notification)
            if generation is None:
                return
            self._enrich_quote(notification, generation)

    def _pop_quote_generation(
        self, notification: StaLocalQuoteNotification
    ) -> int | None:
        generation = self._quote_generations.pop(id(notification), None)
        if generation is None:
            self._fatal("quote_generation_token_missing")
        return generation

    def _enrich_quote(
        self, notification: StaLocalQuoteNotification, generation: int
    ) -> None:
        with self._state_condition:
            is_current = generation == self._generation
        if not is_current:
            reason = "stale_quote_notification"
            self._emit_diagnostic(
                "adapter_error",
                notification.callback_sequence,
                notification.received_at,
                generation,
                message="stale quote notification discarded",
                raw={
                    "market_no": notification.market_no_raw,
                    "stock_idx": notification.stock_idx_raw,
                    "is_long_callback": notification.is_long_callback,
                },
            )
            self._health.degrade(reason)
            self._mark_incomplete(reason)
            return
        for attempt in range(1, self._quote_lookup_attempts + 1):
            try:
                quote = self._backend.lookup_quote(
                    notification.market_no_raw, notification.stock_idx_raw
                )
            except Exception:
                self._emit_diagnostic(
                    "quote_lookup_failure",
                    notification.callback_sequence,
                    notification.received_at,
                    generation,
                    message="quote lookup failed",
                    attempt=attempt,
                    raw={
                        "market_no": notification.market_no_raw,
                        "stock_idx": notification.stock_idx_raw,
                        "is_long_callback": notification.is_long_callback,
                    },
                )
                continue
            payload = CapturedQuoteSnapshot(
                notification.market_no_raw,
                notification.stock_idx_raw,
                quote.bid_raw,
                quote.ask_raw,
                quote.last_raw,
                quote.bid_qty_raw,
                quote.ask_qty_raw,
                quote.last_qty_raw,
                notification.is_long_callback,
                notification.callback_sequence,
                notification.received_at,
            )
            self._publish(
                CapturedKind.QUOTE_SNAPSHOT,
                payload,
                notification.callback_sequence,
                notification.received_at,
                generation,
                {
                    "total_qty": quote.total_qty_raw,
                    "stock_no": quote.stock_no,
                    "stock_name": quote.stock_name,
                },
            )
            return
        self._health.degrade("quote_lookup_failure")
        self._mark_incomplete("quote_lookup_failure")

    def _run_resubscribe(self) -> None:
        with self._state_condition:
            if (
                self._resubscribe_generation != self._generation
                or self._state not in _READY_STATES
                or self._resubscribe_due is None
                or self._clock.monotonic() < self._resubscribe_due
            ):
                return
            quote_symbols = tuple(
                sorted(self._desired_quotes - self._actual_quotes)
            )
            tick_symbols = tuple(sorted(self._desired_ticks - self._actual_ticks))
            if not quote_symbols and not tick_symbols:
                self._resubscribe_generation = None
                self._resubscribe_due = None
                self._refresh_subscription_state_locked()
                return
        for kind, symbols in (("quotes", quote_symbols), ("ticks", tick_symbols)):
            if not symbols:
                continue
            try:
                self._request_batch(kind, symbols)
            except SubscriptionError:
                self._schedule_resubscribe_failure()
                return
        with self._state_condition:
            if self._subscriptions_fulfilled_locked():
                self._resubscribe_generation = None
                self._resubscribe_due = None
                self._refresh_subscription_state_locked()

    def _schedule_resubscribe_failure(self) -> None:
        reason = "resubscription_failure"
        self._health.degrade(reason)
        self._mark_incomplete(reason)
        with self._state_condition:
            self._resubscribe_attempts += 1
            if (
                self._resubscribe_attempts
                >= self._reconnect_policy.max_attempts
            ):
                exhausted = True
            else:
                delay = self._reconnect_policy.backoff_seconds[
                    self._resubscribe_attempts
                ]
                self._resubscribe_due = self._clock.monotonic() + delay
                exhausted = False
        if exhausted:
            self._fatal("resubscription_exhausted")

    def _subscriptions_fulfilled_locked(self) -> bool:
        return (
            self._desired_quotes.issubset(self._actual_quotes)
            and self._desired_ticks.issubset(self._actual_ticks)
        )

    def _refresh_subscription_state_locked(self) -> None:
        has_desired = bool(self._desired_quotes or self._desired_ticks)
        fulfilled = self._subscriptions_fulfilled_locked()
        if (
            has_desired
            and fulfilled
            and self._state is ConnectionState.STOCKS_READY
        ):
            self._transition_locked(ConnectionState.SUBSCRIBED)
        elif (
            has_desired
            and not fulfilled
            and self._state is ConnectionState.SUBSCRIBED
        ):
            self._transition_locked(ConnectionState.STOCKS_READY)

    def _run_reconnect(self) -> None:
        with self._state_condition:
            if (
                self._state is not ConnectionState.RECONNECTING
                or self._reconnect_due is None
                or self._clock.monotonic() < self._reconnect_due
                or self._stop_event.is_set()
            ):
                return
            attempt = self._reconnect_attempts
        if self._monitor_active:
            try:
                leave_code = self._code(
                    self._backend.leave_monitor(), "leave monitor"
                )
            except Exception:
                leave_code = -1
            if leave_code != 0:
                self._schedule_reconnect_failure(attempt)
                return
            self._monitor_active = False
        try:
            self._enter_monitor(True)
        except MonitorError:
            self._schedule_reconnect_failure(attempt)
        else:
            with self._state_condition:
                self._reconnect_attempts = attempt + 1
                self._reconnect_due = None
                if self._state is ConnectionState.RECONNECTING:
                    self._connected_deadline = (
                        self._clock.monotonic()
                        + self._reconnect_connected_timeout
                    )

    def _run_reconnect_watchdog(self) -> None:
        now = self._clock.monotonic()
        with self._state_condition:
            connected_expired = (
                self._state is ConnectionState.RECONNECTING
                and self._connected_deadline is not None
                and now >= self._connected_deadline
            )
            stocks_expired = (
                self._state is ConnectionState.CONNECTED
                and self._stocks_ready_deadline is not None
                and now >= self._stocks_ready_deadline
            )
            if not connected_expired and not stocks_expired:
                return
            self._connected_deadline = None
            self._stocks_ready_deadline = None
            if self._state is ConnectionState.CONNECTED:
                self._transition_locked(ConnectionState.DISCONNECTED)
                self._transition_locked(ConnectionState.RECONNECTING)
            attempts = self._reconnect_attempts
            exhausted = attempts >= self._reconnect_policy.max_attempts
            if not exhausted:
                self._reconnect_due = now
        reason = (
            "reconnect_connected_timeout"
            if connected_expired
            else "reconnect_stocks_ready_timeout"
        )
        self._health.degrade(reason)
        self._mark_incomplete(reason)
        if exhausted:
            self._fatal("reconnect_exhausted")

    def _schedule_reconnect_failure(self, previous_attempts: int) -> None:
        reason = "reconnect_attempt_failure"
        self._health.degrade(reason)
        self._mark_incomplete(reason)
        with self._state_condition:
            self._reconnect_attempts = previous_attempts + 1
            if self._reconnect_attempts >= self._reconnect_policy.max_attempts:
                exhausted = True
            else:
                delay = self._reconnect_policy.backoff_seconds[
                    self._reconnect_attempts
                ]
                self._reconnect_due = self._clock.monotonic() + delay
                exhausted = False
        if exhausted:
            self._fatal("reconnect_exhausted")

    def _cleanup(self) -> None:
        with self._state_condition:
            self._accepting_commands = False
            self._reconnect_due = None
            self._connected_deadline = None
            self._stocks_ready_deadline = None
            if self._state not in (
                ConnectionState.STOPPING,
                ConnectionState.STOPPED,
            ):
                self._transition_locked(ConnectionState.STOPPING)
        self._fail_pending()
        for actual, method in (
            (self._actual_ticks, self._backend.cancel_ticks),
            (self._actual_quotes, self._backend.cancel_quotes),
        ):
            if actual:
                try:
                    self._code(method(",".join(sorted(actual))), "cleanup")
                except Exception:
                    pass
                actual.clear()
        if self._monitor_active:
            try:
                self._code(self._backend.leave_monitor(), "cleanup")
            except Exception:
                pass
            self._monitor_active = False
        if self._events_registered:
            try:
                self._backend.release_events()
            except Exception:
                pass
            self._events_registered = False
        if self._objects_initialized:
            try:
                self._backend.release_objects()
            except Exception:
                pass
            self._objects_initialized = False
        if self._co_initialized:
            try:
                self._backend.co_uninitialize()
            except Exception:
                pass
            self._co_initialized = False
        with self._state_condition:
            if self._state is not ConnectionState.STOPPED:
                self._transition_locked(ConnectionState.STOPPED)

    def _fail_pending(self) -> None:
        while True:
            try:
                command = self._commands.get_nowait()
            except Empty:
                return
            if not command.future.done():
                command.future.set_exception(AdapterStoppedError("adapter stopped"))

    def _fatal(self, reason: str) -> None:
        self._health.fail(reason)
        self._mark_incomplete(reason)
        self._shutdown.request_shutdown(reason)
        self._stop_event.set()
        with self._state_condition:
            self._accepting_commands = False
            if self._state not in (
                ConnectionState.ERROR,
                ConnectionState.STOPPING,
                ConnectionState.STOPPED,
            ):
                self._transition_locked(ConnectionState.ERROR)

    def _mark_incomplete(self, reason: str) -> None:
        try:
            self._impact.mark_incomplete(self._session_id, reason)
        except RuntimeError:
            self._health.fail("session_impact_capacity_exhausted")
            self._shutdown.request_shutdown("session_impact_capacity_exhausted")

    def _handler_time(self, generation: int, sequence: int) -> datetime:
        received_at = self._now()
        with self._state_condition:
            if generation == self._generation:
                self._callback_sequence = max(
                    self._callback_sequence, sequence + 1
                )
        return received_at

    def _callback_failure(
        self, generation: int, callback_name: str
    ) -> None:
        del generation, callback_name
        self._fatal("capital_callback_failure")

    def _now(self) -> datetime:
        value = self._clock.now()
        if type(value) is not datetime:
            raise TypeError("clock.now() must return datetime")
        if (
            value.tzinfo is None
            or value.utcoffset() is None
            or getattr(value.tzinfo, "key", None) != TAIPEI.key
        ):
            raise ValueError("clock.now() must use Asia/Taipei timezone")
        return value

    def _publish(
        self,
        kind: CapturedKind,
        payload: object,
        sequence: int,
        received_at: datetime,
        generation: int,
        raw_payload: dict[str, object],
        *,
        dedupe: str | None = None,
    ) -> None:
        event = CapturedMarketDataEvent(
            kind,
            payload,
            raw_payload,
            self._source,
            SourceMode.LIVE,
            self._session_id,
            generation,
            sequence,
            None,
            received_at,
            None,
            None,
            None,
            dedupe,
        )
        self._ingress.try_publish(event)

    def _emit_diagnostic(
        self,
        kind: str,
        sequence: int,
        received_at: datetime,
        generation: int,
        *,
        message: str,
        attempt: int = 1,
        error_code: int | None = None,
        raw: dict[str, object],
    ) -> None:
        payload = CapturedAdapterDiagnostic(
            kind,
            raw.get("market_no") if type(raw.get("market_no")) is int else None,
            raw.get("stock_idx") if type(raw.get("stock_idx")) is int else None,
            error_code,
            message,
            received_at,
            attempt,
            generation,
            sequence,
            raw,
        )
        dedupe = build_adapter_diagnostic_dedupe_key(
            self._source,
            self._session_id,
            generation,
            kind,
            sequence,
            attempt,
        )
        self._publish(
            CapturedKind.ADAPTER_DIAGNOSTIC,
            payload,
            sequence,
            received_at,
            generation,
            raw,
            dedupe=dedupe,
        )

    def _transition_locked(self, target: ConnectionState) -> None:
        if target not in _TRANSITIONS[self._state]:
            raise RuntimeError(
                f"invalid adapter state transition: {self._state.value} -> {target.value}"
            )
        self._state = target
        self._state_condition.notify_all()

    def _status_locked(self) -> ConnectionStatus:
        return ConnectionStatus(
            self._state,
            self._last_kind,
            self._last_code,
            None,
            self._state in _READY_STATES,
            self._now(),
            self._generation,
        )

    @staticmethod
    def _strict_callback_ints(values: Sequence[object]) -> None:
        if any(type(value) is not int for value in values):
            raise TypeError("callback raw numeric values must be integers")

    @staticmethod
    def _code(value: object, operation: str) -> int:
        if type(value) is not int:
            raise CapitalAdapterError(
                f"{operation} returned a non-integer status"
            )
        return value
