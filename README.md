# tx_trade

這個工作區將用來建立一個以微型台灣指數期貨為核心的程式交易報價系統。

## 目前目標
- 先確認可連線到 Capital API
- 先實作「登入、查商品、查行情」最小流程
- 之後再接入策略與下單邏輯

## 目前規劃
1. 建立可執行的 Python 報價骨架
2. 將範例中的登入與行情查詢流程整理成可維護模組
3. 先用模擬或假資料驗證流程，再接實際 API

## Live quote integration test

The SKCOM live integration test is disabled by default. It performs a real
login through `QuoteClient(quote_only=True)` and subscribes to quotes only.
In this mode the client creates only the SKCOM Center and Quote objects: it
does not create Order/Reply objects, register Reply callbacks, or send orders.

Prerequisites:

- Windows and the configured `SKCOM.dll`
- non-empty `TX_TRADE_ACCOUNT` and `TX_TRADE_PASSWORD` values (for example,
  loaded through the existing `.env` support)
- an explicit per-command opt-in

Run it manually from PowerShell:

```powershell
$env:TX_TRADE_RUN_LIVE_QUOTE_TEST = "1"
.\venv_tx_trade\Scripts\python.exe -m pytest tests\integration\test_skcom_quote_live.py -v
Remove-Item Env:TX_TRADE_RUN_LIVE_QUOTE_TEST
```

Without `TX_TRADE_RUN_LIVE_QUOTE_TEST=1`, the test is skipped. The test verifies
login, quote-monitor readiness, `TX00` lookup, and quote/tick subscription.
Because markets may be closed, receiving a live tick is not required.
