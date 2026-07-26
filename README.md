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

## Safe Phase 1 recorder

Running the repository entry point with no environment configuration records
the canonical deterministic offline fixture, verifies the SQLite readback, and
exits. It does not import the Capital backend, read credentials, or initialize
COM:

```powershell
.\venv_tx_trade\Scripts\python.exe .\main.py --db .\phase1_offline.sqlite3
```

Phase 1 rejects replay, paper execution, and live execution. Production live
quotes are fail-closed and require all of the following explicit settings:

```powershell
$env:TX_TRADE_RUNTIME_PRESET = "phase1_live_quote"
$env:TX_TRADE_ENABLE_LIVE_QUOTE = "1"
$env:TX_TRADE_ACCOUNT = "<account>"
$env:TX_TRADE_PASSWORD = "<password>"
$env:TX_TRADE_SKCOM_DLL_PATH = "C:\path\to\SKCOM.dll"
$env:TX_TRADE_SYMBOLS = "TX00"
.\venv_tx_trade\Scripts\python.exe .\main.py --db .\phase1_live.sqlite3
```

Only the Center/Quote backend is composed. The Phase 1 entry point never
creates Order/Reply objects and never sends orders. Credentials are read only
after the live preset and production opt-in validate; they are not printed,
persisted, or included in the configuration fingerprint.

The legacy quote snapshot projector is a non-authoritative compatibility view.
It runs after the pipeline sink accepts an event; with the asynchronous SQLite
writer this can precede durable storage. Writer failure therefore makes the
recording incomplete, and SQLite readback remains the authoritative result.

## Live quote integration test

The SKCOM live integration test is disabled by default. It performs a real
login through `QuoteClient(quote_only=True)` and subscribes to quotes only.
In this mode the client creates only the SKCOM Center and Quote objects: it
does not create Order/Reply objects, register Reply callbacks, or send orders.

Prerequisites:

- Windows and `TX_TRADE_SKCOM_DLL_PATH` naming the configured `SKCOM.dll`
- non-empty `TX_TRADE_ACCOUNT` and `TX_TRADE_PASSWORD` values
- an explicit per-command opt-in

Run it manually from PowerShell:

```powershell
$env:TX_TRADE_RUN_LIVE_QUOTE_TEST = "1"
$env:TX_TRADE_SKCOM_DLL_PATH = "C:\path\to\SKCOM.dll"
.\venv_tx_trade\Scripts\python.exe -m pytest tests\integration\test_skcom_quote_live.py -v
Remove-Item Env:TX_TRADE_RUN_LIVE_QUOTE_TEST
```

Without `TX_TRADE_RUN_LIVE_QUOTE_TEST=1`, the test is skipped. The test verifies
login, quote-monitor readiness, `TX00` lookup, and quote/tick subscription.
Because markets may be closed, receiving a live tick is not required.
