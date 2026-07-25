import os
import time
from ctypes import POINTER, c_short, pointer
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import pythoncom
except Exception:  # pragma: no cover - environment dependent
    pythoncom = None

try:
    import comtypes.client
except Exception:  # pragma: no cover - environment dependent
    comtypes = None

from config import API_DLL_PATH, DEFAULT_SYMBOLS, DEFAULT_ACCOUNT, DEFAULT_PASSWORD

# OnConnection 事件的 nKind 代碼（見群益「策略王COM元件使用說明」6.代碼定義表）
SK_SUBJECT_CONNECTION_CONNECTED = 3001
SK_SUBJECT_CONNECTION_DISCONNECT = 3002
SK_SUBJECT_CONNECTION_STOCKS_READY = 3003  # 報價商品載入完成，此後才可訂閱/查詢商品


class QuoteClient:
    """最小報價客戶端，優先嘗試連接 Capital API，若缺少 DLL 則退回離線模式。"""

    def __init__(self, dll_path: Optional[str] = None):
        self.dll_path = dll_path or str(API_DLL_PATH)
        self._connected = False
        self._symbols: List[str] = []
        self._quote_service = None
        self._center_service = None
        self._order_service = None
        self._reply_service = None
        self._offline_mode = True
        self._last_error: Optional[str] = None
        self._sk_module = None
        self._api_objects: dict[str, Any] = {}
        self._dll_candidates = self._build_dll_candidates()
        self._reply_registered = False
        self._event_connection = None

    def _build_dll_candidates(self) -> List[str]:
        candidates: List[str] = []
        if self.dll_path:
            candidates.append(self.dll_path)

        base_dir = Path(__file__).resolve().parent
        candidates.extend([
            str(base_dir / "SKCOM.dll"),
            str(base_dir.parent / "SKCOM.dll"),
            str(Path.cwd() / "SKCOM.dll"),
            r"C:\CapitalAPI\SKCOM.dll",
            r"C:\Program Files\CapitalAPI\SKCOM.dll",
            r"C:\Program Files (x86)\CapitalAPI\SKCOM.dll",
        ])
        return list(dict.fromkeys(candidates))

    def _try_load_api(self) -> bool:
        if comtypes is None:
            self._last_error = "comtypes 未安裝"
            return False

        for candidate in self._dll_candidates:
            if not os.path.exists(candidate):
                continue
            try:
                comtypes.client.GetModule(candidate)
                import comtypes.gen.SKCOMLib as sk

                self._sk_module = sk
                self._api_objects = {
                    "center": comtypes.client.CreateObject(sk.SKCenterLib, interface=sk.ISKCenterLib),
                    "quote": comtypes.client.CreateObject(sk.SKQuoteLib, interface=sk.ISKQuoteLib),
                    "order": comtypes.client.CreateObject(sk.SKOrderLib, interface=sk.ISKOrderLib),
                    "reply": comtypes.client.CreateObject(sk.SKReplyLib, interface=sk.ISKReplyLib),
                }
                self._center_service = self._api_objects.get("center")
                self._quote_service = self._api_objects.get("quote")
                self._order_service = self._api_objects.get("order")
                self._reply_service = self._api_objects.get("reply")
                self._offline_mode = False
                self._last_error = None
                return True
            except Exception as exc:  # pragma: no cover - environment dependent
                self._last_error = f"載入 API 失敗: {candidate} -> {exc}"

        self._last_error = "找不到可用的 SKCOM.dll"
        return False

    def initialize(self) -> bool:
        """初始化 COM 物件與 API 連線；若缺少 DLL，改為離線模式。"""
        self._connected = False
        self._symbols = list(DEFAULT_SYMBOLS)
        if self._try_load_api():
            self._connected = True
            return True

        self._offline_mode = True
        self._connected = True
        return True

    def _invoke_method(self, service: Any, names: List[str], *args: Any) -> Any:
        if service is None:
            raise RuntimeError("Capital API 服務尚未初始化")
        for name in names:
            method = getattr(service, name, None)
            if callable(method):
                return method(*args)
        raise AttributeError(f"找不到可呼叫的方法: {names}")

    def _get_return_code_message(self, code: int) -> str:
        if self._center_service is None:
            return ""
        try:
            return self._invoke_method(self._center_service, ["SKCenterLib_GetReturnCodeMessage"], code)
        except Exception:
            return ""

    def _register_reply_callback(self) -> None:
        if self._reply_registered or self._reply_service is None:
            return
        try:
            if pythoncom is not None:
                pythoncom.CoInitialize()

            if self._sk_module is None:
                return

            class SKReplyLibEvent(object):
                _com_interfaces_ = [self._sk_module._ISKReplyLibEvents]

                def OnReplyMessage(self, bstrUserID, bstrMessage):
                    sConfirmCode = -1
                    return sConfirmCode

            sink = SKReplyLibEvent()
            self._event_connection = comtypes.client.GetEvents(self._reply_service, sink)
            self._reply_registered = True
        except Exception as exc:
            self._last_error = f"註冊 reply callback 失敗: {exc}"
            self._reply_registered = False

    def login(self, account: str, password: str) -> dict:
        """登入 Capital API；離線模式下回傳詳細狀態。"""
        if not account or not password:
            raise ValueError("帳號與密碼不可為空")
        if self._offline_mode:
            return {"success": True, "mode": "offline", "code": 0, "message": "offline mode"}

        steps = []
        log_dir = str(Path(__file__).resolve().parent / "logs")

        try:
            self._invoke_method(self._center_service, ["SKCenterLib_SetLogPath"], log_dir)
            steps.append("set_log_path")
        except Exception as exc:
            steps.append(f"set_log_path_failed:{exc}")
            return {"success": False, "mode": "api", "steps": steps, "message": str(exc)}

        try:
            self._invoke_method(self._center_service, ["SKCenterLib_Debug"], True)
            steps.append("debug")
        except Exception as exc:
            steps.append(f"debug_failed:{exc}")
            return {"success": False, "mode": "api", "steps": steps, "message": str(exc)}

        try:
            self._register_reply_callback()
            steps.append("register_reply")
        except Exception as exc:
            steps.append(f"register_reply_failed:{exc}")
            return {"success": False, "mode": "api", "steps": steps, "message": str(exc)}

        try:
            code = self._invoke_method(self._center_service, ["SKCenterLib_Login", "SKCenterLib_login"], account, password)
            steps.append(f"login_code:{code}")
        except Exception as exc:
            steps.append(f"login_failed:{exc}")
            return {"success": False, "mode": "api", "steps": steps, "message": str(exc)}

        message = ""
        try:
            message = self._invoke_method(self._center_service, ["SKCenterLib_GetReturnCodeMessage"], code)
        except Exception:
            message = ""
        return {"success": code == 0, "mode": "api", "code": code, "steps": steps, "message": message}

    def _register_quote_callback(self) -> None:
        if self._quote_service is None:
            return
        if hasattr(self, '_quote_registered') and self._quote_registered:
            return

        if self._sk_module is None:
            return

        class SKQuoteLibEvent(object):
            _com_interfaces_ = [self._sk_module._ISKQuoteLibEvents]

            def __init__(self, parent: 'QuoteClient'):
                self._parent = parent

            def OnConnection(self, nKind, nCode):
                state = self._parent._quote_connection_state
                state["last_kind"] = nKind
                state["last_code"] = nCode
                if nKind == SK_SUBJECT_CONNECTION_STOCKS_READY:
                    state["stocks_ready"] = True
                elif nKind == SK_SUBJECT_CONNECTION_DISCONNECT:
                    state["stocks_ready"] = False

            def OnNotifyServerTime(self, sHour, sMinute, sSecond, nTotal):
                self._parent._quote_event_data["server_time"] = {
                    "hour": sHour,
                    "minute": sMinute,
                    "second": sSecond,
                    "total": nTotal,
                }

            def OnNotifyStockList(self, sMarketNo, bstrStockData):
                self._parent._quote_event_data["stock_list"] = {
                    "market_no": sMarketNo,
                    "product_data": bstrStockData,
                }

            def OnNotifyQuote(self, sMarketNo, sStockIdx):
                self._parent._quote_event_data["quotes"].append({
                    "market_no": sMarketNo,
                    "stock_idx": sStockIdx,
                })

            def OnNotifyTicks(self, sMarketNo, sStockIdx, nPtr, nDate, nTimehms, nTimemillismicros, nBid, nAsk, nClose, nQty, nSimulate):
                self._parent._quote_event_data["ticks"].append({
                    "market_no": sMarketNo,
                    "stock_idx": sStockIdx,
                    "ptr": nPtr,
                    "date": nDate,
                    "timehms": nTimehms,
                    "timemillismicros": nTimemillismicros,
                    "bid": nBid,
                    "ask": nAsk,
                    "close": nClose,
                    "qty": nQty,
                    "simulate": nSimulate,
                })

            def OnNotifyQuoteLONG(self, sMarketNo, nStockIdx):
                self._parent._quote_event_data["quotes"].append({
                    "market_no": sMarketNo,
                    "stock_idx": nStockIdx,
                    "long": True,
                })

            def OnNotifyTicksLONG(self, sMarketNo, nStockIdx, nPtr, nDate, nTimehms, nTimemillismicros, nBid, nAsk, nClose, nQty, nSimulate):
                self._parent._quote_event_data["ticks"].append({
                    "market_no": sMarketNo,
                    "stock_idx": nStockIdx,
                    "ptr": nPtr,
                    "date": nDate,
                    "timehms": nTimehms,
                    "timemillismicros": nTimemillismicros,
                    "bid": nBid,
                    "ask": nAsk,
                    "close": nClose,
                    "qty": nQty,
                    "simulate": nSimulate,
                    "long": True,
                })

        self._quote_event_data = {
            "server_time": None,
            "stock_list": None,
            "quotes": [],
            "ticks": [],
        }
        self._quote_connection_state = {
            "stocks_ready": False,
            "last_kind": None,
            "last_code": None,
        }
        sink = SKQuoteLibEvent(self)
        self._quote_event_connection = comtypes.client.GetEvents(self._quote_service, sink)
        self._quote_registered = True

    def _wrap_paging_call(self, method_names: List[str], market_no: Optional[int], symbol_text: str) -> Dict[str, Any]:
        page_no = c_short(0)
        page_ptr = pointer(page_no)
        if market_no is None:
            result = self._invoke_method(self._quote_service, method_names, page_ptr, symbol_text)
        else:
            result = self._invoke_method(self._quote_service, method_names, page_ptr, c_short(market_no), symbol_text)

        def _extract_page_value(value: Any) -> int:
            if isinstance(value, c_short):
                return int(value.value)
            if hasattr(value, 'contents') and hasattr(value.contents, 'value'):
                try:
                    return int(value.contents.value)
                except Exception:
                    pass
            if isinstance(value, (int, float)):
                return int(value)
            return int(page_no.value)

        if isinstance(result, (tuple, list)) and len(result) == 2:
            maybe_page, ret_code = result
            parsed_page = _extract_page_value(maybe_page)
            return {
                "success": int(ret_code) == 0,
                "code": int(ret_code),
                "message": self._get_return_code_message(int(ret_code)),
                "page_no": parsed_page,
                "response": [],
            }
        if isinstance(result, (tuple, list)) and len(result) == 1:
            return {
                "success": int(result[0]) == 0,
                "code": int(result[0]),
                "message": self._get_return_code_message(int(result[0])),
                "page_no": int(page_no.value),
                "response": [],
            }
        return {
            "success": bool(result == 0),
            "code": int(result),
            "message": self._get_return_code_message(int(result)),
            "page_no": int(page_no.value),
            "response": [],
        }

    def _wrap_object_call(self, method_names: List[str], *args: Any) -> Dict[str, Any]:
        result = self._invoke_method(self._quote_service, method_names, *args)
        def _format_object_response(obj: Any, code: int) -> Dict[str, Any]:
            return {
                "success": int(code) == 0,
                "code": int(code),
                "message": self._get_return_code_message(int(code)),
                "object": obj,
            }

        if isinstance(result, (tuple, list)) and len(result) == 2:
            obj, ret_code = result
            return _format_object_response(obj, ret_code)
        if isinstance(result, (tuple, list)) and len(result) == 1:
            code = int(result[0])
            return _format_object_response(args[-1] if args else None, code)
        return _format_object_response(args[-1] if args else None, int(result))

    def enter_monitor(self) -> Dict[str, Any]:
        """進入行情監看並返回狀態。"""
        if not self._connected:
            raise RuntimeError("尚未初始化報價客戶端")
        if self._offline_mode:
            return {"success": True, "mode": "offline", "message": "enter monitor offline"}

        self._register_quote_callback()

        code = self._invoke_method(self._quote_service, ["SKQuoteLib_EnterMonitorLONG"])
        return {"success": code == 0, "code": int(code), "message": self._get_return_code_message(int(code))}

    def _pump_com_messages(self, duration: float) -> None:
        """在等待期間持續抽送 COM 訊息，讓 comtypes 事件回呼(OnConnection 等)得以被派送執行。"""
        if pythoncom is None:
            time.sleep(duration)
            return
        end = time.monotonic() + duration
        while time.monotonic() < end:
            pythoncom.PumpWaitingMessages()
            time.sleep(0.05)

    def is_quote_connected(self) -> Dict[str, Any]:
        """檢查報價商品是否已就緒（依 OnConnection 事件的 SK_SUBJECT_CONNECTION_STOCKS_READY 判斷）。"""
        if not self._connected:
            raise RuntimeError("尚未初始化報價客戶端")
        if self._offline_mode:
            return {"success": True, "connected": True, "status": "offline"}

        state = getattr(self, '_quote_connection_state', None) or {}
        return {
            "success": True,
            "connected": bool(state.get("stocks_ready")),
            "last_kind": state.get("last_kind"),
            "last_code": state.get("last_code"),
        }

    def wait_for_quote_ready(self, timeout: float = 12.0, interval: float = 1.0) -> Dict[str, Any]:
        """等待 OnConnection 事件回報 SK_SUBJECT_CONNECTION_STOCKS_READY(商品資料下載完成)。"""
        if not self._connected:
            raise RuntimeError("尚未初始化報價客戶端")
        if self._offline_mode:
            return {"success": True, "connected": True, "status": "offline", "elapsed": 0.0}

        start = time.monotonic()
        status: Dict[str, Any] = {"success": False, "connected": False}
        while time.monotonic() - start < timeout:
            status = self.is_quote_connected()
            if status.get("connected"):
                return {
                    "success": True,
                    "connected": True,
                    "status": status,
                    "elapsed": time.monotonic() - start,
                }
            self._pump_com_messages(interval)

        status["elapsed"] = time.monotonic() - start
        status["success"] = False
        return status

    def request_server_time(self) -> Dict[str, Any]:
        """回傳主機時間。"""
        if not self._connected:
            raise RuntimeError("尚未初始化報價客戶端")
        if self._offline_mode:
            return {"success": True, "server_time": "offline"}

        code = self._invoke_method(self._quote_service, ["SKQuoteLib_RequestServerTime"])
        return {"success": code == 0, "code": int(code)}

    def request_stocks(self, symbols: Optional[List[str]] = None, market_no: Optional[int] = None) -> Dict[str, Any]:
        """請求商品清單或商品查詢。"""
        if not self._connected:
            raise RuntimeError("尚未初始化報價客戶端")

        target_symbols = symbols or self._symbols
        if self._offline_mode:
            return {
                "success": True,
                "items": [
                    {"symbol": symbol, "market": "TX", "status": "offline"}
                    for symbol in target_symbols
                ],
            }

        symbol_text = ",".join(target_symbols)
        method_names = ["SKQuoteLib_RequestStocksWithMarketNo"] if market_no is not None else ["SKQuoteLib_RequestStocks"]
        payload = self._wrap_paging_call(method_names, market_no, symbol_text)
        payload["symbols"] = target_symbols
        return payload

    def request_ticks(self, symbols: Optional[List[str]] = None, market_no: Optional[int] = None) -> Dict[str, Any]:
        """請求 Tick 資料。"""
        if not self._connected:
            raise RuntimeError("尚未初始化報價客戶端")
        if self._offline_mode:
            return {"success": True, "items": []}

        target_symbols = symbols or self._symbols
        symbol_text = ",".join(target_symbols)
        method_names = ["SKQuoteLib_RequestTicksWithMarketNo"] if market_no is not None else ["SKQuoteLib_RequestTicks"]
        payload = self._wrap_paging_call(method_names, market_no, symbol_text)
        payload["symbols"] = target_symbols
        return payload

    def get_stock_by_symbol(self, symbol: str, market_no: Optional[int] = None) -> Dict[str, Any]:
        """根據代碼取得商品資訊。"""
        if not self._connected:
            raise RuntimeError("尚未初始化報價客戶端")
        if self._offline_mode:
            return {"success": True, "symbol": symbol, "market_no": market_no, "stock": None}

        stock_obj = self._sk_module.SKSTOCKLONG()
        if market_no is not None:
            payload = self._wrap_object_call(["SKQuoteLib_GetStockByMarketAndNo"], market_no, symbol, stock_obj)
        else:
            payload = self._wrap_object_call(["SKQuoteLib_GetStockByNoLONG"], symbol, stock_obj)
        payload.update({"symbol": symbol, "market_no": market_no})
        return payload

    def get_tick_long(self, market_no: int, stock_idx: int, n_ptr: int = 0) -> Dict[str, Any]:
        """取得長格式 tick 資料。"""
        if not self._connected:
            raise RuntimeError("尚未初始化報價客戶端")
        if self._offline_mode:
            return {"success": True, "ticker": None}

        tick_obj = self._sk_module.SKTICK()
        payload = self._wrap_object_call(["SKQuoteLib_GetTickLONG"], market_no, stock_idx, n_ptr, tick_obj)
        payload["ticker"] = tick_obj
        return payload

    def pump_events(self, duration: float = 1.0) -> None:
        """抽送 COM 訊息一段時間，讓已註冊的事件回呼(報價/Tick/OnConnection等)有機會被觸發執行。"""
        if self._offline_mode:
            time.sleep(duration)
            return
        self._pump_com_messages(duration)

    def get_latest_event_data(self) -> Dict[str, Any]:
        """回傳自事件回調蒐集到的最新報價資料。"""
        return getattr(self, '_quote_event_data', {})

    def connect_reply_by_id(self, user_id: str) -> Dict[str, Any]:
        """連線 ReplyLib，讓回報機制可用。"""
        if not self._connected:
            raise RuntimeError("Capital API 服務尚未初始化")
        if self._offline_mode:
            return {"success": True, "message": "offline reply connect"}

        code = self._invoke_method(self._reply_service, ["SKReplyLib_ConnectByID"], user_id)
        return {"success": code == 0, "code": int(code)}

    def order_initialize(self) -> Dict[str, Any]:
        """初始化下單元件。"""
        if not self._connected:
            raise RuntimeError("Capital API 服務尚未初始化")
        if self._offline_mode:
            return {"success": True, "message": "offline order initialize"}

        code = self._invoke_method(self._order_service, ["SKOrderLib_Initialize"])
        return {"success": code == 0, "code": int(code)}

    def order_load_commodity_gw(self, login_id: str) -> Dict[str, Any]:
        """載入 GW 下單商品資訊。"""
        if not self._connected:
            raise RuntimeError("Capital API 服務尚未初始化")
        if self._offline_mode:
            return {"success": True, "message": "offline order load commodity"}

        code = self._invoke_method(self._order_service, ["SKOrderLib_LoadOfCommodityGW"], login_id)
        return {"success": code == 0, "code": int(code)}

    def order_initial_proxy_by_id(self, login_id: str) -> Dict[str, Any]:
        """初始化 proxy 下單；適用特定帳號 ID。"""
        if not self._connected:
            raise RuntimeError("Capital API 服務尚未初始化")
        if self._offline_mode:
            return {"success": True, "message": "offline order proxy init"}

        code = self._invoke_method(self._order_service, ["SKOrderLib_InitialProxyByID"], login_id)
        return {"success": code == 0, "code": int(code)}

    def get_order_login_type(self, login_id: str) -> Dict[str, Any]:
        """查詢下單帳號類型。"""
        if not self._connected:
            raise RuntimeError("Capital API 服務尚未初始化")
        if self._offline_mode:
            return {"success": True, "login_type": None}

        code = self._invoke_method(self._order_service, ["SKOrderLib_GetLoginType"], login_id)
        return {"success": True, "login_type": code}


if __name__ == "__main__":
    client = QuoteClient()
    client.initialize()
    print(client.login("account", "password"))
    print(client.enter_monitor())
    print(client.request_server_time())
    print(client.request_stocks(["TX00"]))
