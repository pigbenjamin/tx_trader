import os
import time

from quote_client import QuoteClient
from config import DEFAULT_ACCOUNT, DEFAULT_PASSWORD


def main() -> None:
    account = os.getenv("TX_TRADE_ACCOUNT", DEFAULT_ACCOUNT)
    password = os.getenv("TX_TRADE_PASSWORD", DEFAULT_PASSWORD)

    client = QuoteClient()
    client.initialize()
    login_result = client.login(account, password)
    print("登入結果:", login_result)

    if login_result.get("success"):
        print("進入監看:", client.enter_monitor())

        ready_status = client.wait_for_quote_ready(timeout=12.0, interval=1.0)
        print("報價連線就緒狀態:", ready_status)

        print("主機時間:", client.request_server_time())
        print("商品查詢:", client.request_stocks(["TX00"]))
        print("商品詳情:", client.get_stock_by_symbol("TX00"))
        print("Tick 查詢:", client.request_ticks(["TX00"]))
        print("訂單元件初始化:", client.order_initialize())
        print("訂單商品載入:", client.order_load_commodity_gw(account))
        print("ReplyLib 連線:", client.connect_reply_by_id(account))

        print("等待報價事件推播中(10 秒)...")
        client.pump_events(10.0)
        print("收到的報價事件資料:", client.get_latest_event_data())
    else:
        print("登入失敗，無法繼續初始化報價與下單流程。")


if __name__ == "__main__":
    main()
