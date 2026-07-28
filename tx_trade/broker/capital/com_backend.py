"""Lazy actual COM backend. All methods are intended for one STA thread."""

from __future__ import annotations

import importlib
from typing import Any, Callable

from .contracts import LiveQuoteInitializationError, QuoteSnapshotRaw


def _return_code(value: object, operation: str) -> int:
    if type(value) is not int:
        raise LiveQuoteInitializationError(f"{operation} returned a non-integer status")
    return value


class ComtypesQuoteBackend:
    """Quote-only backend with an announcement-only reply event connection."""

    def __init__(self) -> None:
        self._runtime: Any = None
        self._client: Any = None
        self._module: Any = None
        self._center: Any = None
        self._quote: Any = None
        self._reply: Any = None
        self._quote_event_connection: Any = None
        self._reply_event_connection: Any = None

    def co_initialize(self) -> None:
        try:
            self._runtime = importlib.import_module("pythoncom")
            self._runtime.CoInitialize()
        except Exception as exc:
            raise LiveQuoteInitializationError("COM initialization failed") from exc

    def initialize(self, dll_path: str) -> None:
        if type(dll_path) is not str or not dll_path.strip():
            raise LiveQuoteInitializationError("DLL path must not be empty")
        try:
            self._client = importlib.import_module("comtypes.client")
            module = self._client.GetModule(dll_path)
            if module is None:
                module = importlib.import_module("comtypes.gen.SKCOMLib")
            self._module = module
            center_class = getattr(module, "SKCenterLib")
            center_interface = getattr(module, "ISKCenterLib")
            quote_class = getattr(module, "SKQuoteLib")
            quote_interface = getattr(module, "ISKQuoteLib")
            reply_class = getattr(module, "SKReplyLib")
            reply_interface = getattr(module, "ISKReplyLib")
            self._center = self._client.CreateObject(center_class, interface=center_interface)
            self._quote = self._client.CreateObject(quote_class, interface=quote_interface)
            self._reply = self._client.CreateObject(reply_class, interface=reply_interface)
        except Exception as exc:
            self.release_objects()
            raise LiveQuoteInitializationError("quote library initialization failed") from exc

    def register_events(self, sink: object) -> None:
        try:
            module = self._module

            class AnnouncementReplySink:
                _com_interfaces_ = [module._ISKReplyLibEvents]

                def OnReplyMessage(self, user_id: object, message: object) -> int:
                    del user_id, message
                    return -1

            self._quote_event_connection = self._client.GetEvents(self._quote, sink)
            self._reply_event_connection = self._client.GetEvents(
                self._reply, AnnouncementReplySink()
            )
        except Exception as exc:
            self.release_events()
            raise LiveQuoteInitializationError("quote event registration failed") from exc

    def _invoke(self, target: object, names: tuple[str, ...], *args: object) -> Any:
        for name in names:
            method = getattr(target, name, None)
            if callable(method):
                return method(*args)
        raise LiveQuoteInitializationError("required quote API method is unavailable")

    def login(self, account: str, password: str) -> int:
        return _return_code(
            self._invoke(
                self._center,
                ("SKCenterLib_Login", "SKCenterLib_login"),
                account,
                password,
            ),
            "login",
        )

    def enter_monitor(self) -> int:
        return _return_code(
            self._invoke(self._quote, ("SKQuoteLib_EnterMonitorLONG",)),
            "enter monitor",
        )

    def leave_monitor(self) -> int:
        return _return_code(
            self._invoke(self._quote, ("SKQuoteLib_LeaveMonitor",)),
            "leave monitor",
        )

    def _paging(self, method_name: str, symbols_csv: str) -> int:
        result = self._invoke(self._quote, (method_name,), 0, symbols_csv)
        if isinstance(result, (tuple, list)):
            if not result:
                raise LiveQuoteInitializationError("subscription returned an empty result")
            result = result[-1]
        return _return_code(result, "subscription")

    def request_quotes(self, symbols_csv: str) -> int:
        return self._paging("SKQuoteLib_RequestStocks", symbols_csv)

    def request_ticks(self, symbols_csv: str) -> int:
        return self._paging("SKQuoteLib_RequestTicks", symbols_csv)

    def cancel_quotes(self, symbols_csv: str) -> int:
        return _return_code(
            self._invoke(self._quote, ("SKQuoteLib_CancelRequestStocks",), symbols_csv),
            "cancel quotes",
        )

    def cancel_ticks(self, symbols_csv: str) -> int:
        return _return_code(
            self._invoke(self._quote, ("SKQuoteLib_CancelRequestTicks",), symbols_csv),
            "cancel ticks",
        )

    def lookup_quote(self, market_no: int, stock_idx: int) -> QuoteSnapshotRaw:
        try:
            stock = self._module.SKSTOCKLONG()
            try:
                result = self._invoke(
                    self._quote,
                    ("SKQuoteLib_GetStockByIndexLONG",),
                    market_no,
                    stock_idx,
                    stock,
                )
            except TypeError:
                result = self._invoke(
                    self._quote,
                    ("SKQuoteLib_GetStockByIndexLONG",),
                    market_no,
                    stock_idx,
                )
            returned_stock = stock
            if isinstance(result, (tuple, list)):
                if len(result) == 2:
                    first, second = result
                    if type(second) is int:
                        returned_stock, result = first, second
                    else:
                        result, returned_stock = first, second
                elif len(result) == 1:
                    result = result[0]
                else:
                    raise ValueError("unexpected quote lookup result")
            code = _return_code(result, "quote lookup")
            if code != 0:
                raise LiveQuoteInitializationError("quote lookup failed")
            return QuoteSnapshotRaw(
                bid_raw=int(returned_stock.nBid),
                ask_raw=int(returned_stock.nAsk),
                last_raw=int(returned_stock.nClose),
                bid_qty_raw=int(returned_stock.nBc),
                ask_qty_raw=int(returned_stock.nAc),
                last_qty_raw=None,
                total_qty_raw=int(returned_stock.nTQty),
                stock_no=self._optional_text(returned_stock, ("bstrStockNo", "strStockNo")),
                stock_name=self._optional_text(returned_stock, ("bstrStockName", "strStockName")),
            )
        except LiveQuoteInitializationError:
            raise
        except Exception as exc:
            raise LiveQuoteInitializationError("quote lookup failed") from exc

    @staticmethod
    def _optional_text(value: object, names: tuple[str, ...]) -> str | None:
        for name in names:
            candidate = getattr(value, name, None)
            if candidate is not None:
                return str(candidate)
        return None

    def pump_waiting_messages(self) -> None:
        if self._runtime is None:
            raise LiveQuoteInitializationError("COM runtime is not initialized")
        self._runtime.PumpWaitingMessages()

    def release_events(self) -> None:
        connections = (
            self._quote_event_connection,
            self._reply_event_connection,
        )
        self._quote_event_connection = None
        self._reply_event_connection = None
        for connection in connections:
            if connection is None:
                continue
            disconnect: Callable[[], object] | None = getattr(connection, "disconnect", None)
            if callable(disconnect):
                try:
                    disconnect()
                except Exception:
                    pass

    def release_objects(self) -> None:
        self._reply = None
        self._quote = None
        self._center = None
        self._module = None
        self._client = None

    def co_uninitialize(self) -> None:
        runtime, self._runtime = self._runtime, None
        if runtime is not None:
            runtime.CoUninitialize()
