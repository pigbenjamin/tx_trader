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

## 開發環境

本專案支援 **Python 3.13 series**；請使用目前可取得且仍受維護的
Python 3.13 修補版本，不固定在單一 patch release。

在乾淨的 PowerShell 工作階段建立 fresh virtual environment，並依序完成
安裝、相依性檢查、import smoke test 與預設安全測試：

```powershell
py -3.13 -m venv .\venv_tx_trade_fresh
.\venv_tx_trade_fresh\Scripts\python.exe --version
.\venv_tx_trade_fresh\Scripts\python.exe -m pip install -r .\requirements.txt
.\venv_tx_trade_fresh\Scripts\python.exe -m pip check
.\venv_tx_trade_fresh\Scripts\python.exe -c "import comtypes, pytest, win32api, tzdata; import quote_client, tx_trade"
.\venv_tx_trade_fresh\Scripts\python.exe -m pytest -m "not legacy_com and not stress and not live"
```

The requirements force a pure-Python `mypy` installation because Windows
application-control policies may block its optional compiled extension. Run
the complete local quality gates with:

```powershell
.\venv_tx_trade_fresh\Scripts\python.exe -m ruff format --check tx_trade tests main.py quote_client.py config.py
.\venv_tx_trade_fresh\Scripts\python.exe -m ruff check tx_trade tests main.py quote_client.py config.py
.\venv_tx_trade_fresh\Scripts\python.exe -m mypy tx_trade
.\venv_tx_trade_fresh\Scripts\python.exe -m pytest -o addopts="" -m "not legacy_com"
```

`legacy_com`、`stress` 與 `live` 測試均分類為明確 opt-in；需要時分別以
`-m legacy_com`、`-m stress` 或 `-m live` 執行。`live` 是會登入真實
SKCOM 並訂閱市場資料的測試，不能因為要執行一般測試而順帶啟用。

## Safe Phase 1 recorder

Running the repository entry point with no environment configuration records
the canonical deterministic offline fixture, verifies the SQLite readback, and
exits. It does not import the Capital backend, read credentials, or initialize
COM:

```powershell
.\venv_tx_trade_fresh\Scripts\python.exe .\main.py --db .\phase1_offline.sqlite3
```

Phase 1 rejects replay, paper execution, and order execution. Production live
market data is fail-closed and requires the production preset plus its own
explicit production opt-in. This is independent from the test-only
`TX_TRADE_RUN_LIVE_QUOTE_TEST` opt-in.

The following PowerShell example prompts for credentials, so real credential
literals are not placed in shell history. It saves and restores every process
environment variable that it changes:

```powershell
$names = @(
    "TX_TRADE_RUNTIME_PRESET", "TX_TRADE_ENABLE_LIVE_QUOTE",
    "TX_TRADE_ACCOUNT", "TX_TRADE_PASSWORD",
    "TX_TRADE_SKCOM_DLL_PATH", "TX_TRADE_SYMBOLS"
)
$saved = @{}
foreach ($name in $names) {
    $saved[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
}
$account = Read-Host "SKCOM account"
$securePassword = Read-Host "SKCOM password" -AsSecureString
$passwordPtr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($securePassword)
try {
    $env:TX_TRADE_RUNTIME_PRESET = "phase1_live_quote"
    $env:TX_TRADE_ENABLE_LIVE_QUOTE = "1"
    $env:TX_TRADE_ACCOUNT = $account
    $env:TX_TRADE_PASSWORD = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($passwordPtr)
    $env:TX_TRADE_SKCOM_DLL_PATH = "C:\path\to\SKCOM.dll"
    $env:TX_TRADE_SYMBOLS = "TX00"
    .\venv_tx_trade_fresh\Scripts\python.exe .\main.py --db .\phase1_live.sqlite3
}
finally {
    [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($passwordPtr)
    $account = $null
    $securePassword = $null
    foreach ($name in $names) {
        [Environment]::SetEnvironmentVariable($name, $saved[$name], "Process")
    }
}
```

The Phase 1 backend creates Center and Quote plus one narrowly scoped Reply
object required by SKCOM login. Reply is used only to register
`OnReplyMessage` for announcements. The entry point never creates Order,
never calls `SKReplyLib_ConnectByID`, never registers order/fill callbacks, and
never sends orders. Credentials are read only after the live preset and
production opt-in validate; they are not printed, persisted, or included in
the configuration fingerprint.

The legacy quote snapshot projector is a non-authoritative compatibility view.
It runs after the pipeline sink accepts an event; with the asynchronous SQLite
writer this can precede durable storage. Writer failure therefore makes the
recording incomplete, and SQLite readback remains the authoritative result.

## Live quote integration test

The SKCOM live integration test is disabled by default. It performs a real
login through `QuoteClient(quote_only=True)` and is **market-data only**:
it subscribes to both quote and tick data. In this mode the client creates
Center, Quote, and an announcement-only Reply object. Reply only registers
`OnReplyMessage`, as required before SKCOM login; it never calls
`ConnectByID`, registers order/fill callbacks, creates Order, or sends orders.

Prerequisites:

- Windows and `TX_TRADE_SKCOM_DLL_PATH` naming the configured `SKCOM.dll`
- non-empty `TX_TRADE_ACCOUNT` and `TX_TRADE_PASSWORD` values
- an explicit per-command opt-in

Run it manually from PowerShell. As with production, prompt for credentials so
their real values are not recorded in shell history, and restore the process
environment afterward. The test opt-in below does not enable the production
runtime:

```powershell
$names = @(
    "TX_TRADE_RUN_LIVE_QUOTE_TEST", "TX_TRADE_SKCOM_DLL_PATH",
    "TX_TRADE_ACCOUNT", "TX_TRADE_PASSWORD"
)
$saved = @{}
foreach ($name in $names) {
    $saved[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
}
$account = Read-Host "SKCOM account"
$securePassword = Read-Host "SKCOM password" -AsSecureString
$passwordPtr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($securePassword)
try {
    $env:TX_TRADE_RUN_LIVE_QUOTE_TEST = "1"
    $env:TX_TRADE_SKCOM_DLL_PATH = "C:\path\to\SKCOM.dll"
    $env:TX_TRADE_ACCOUNT = $account
    $env:TX_TRADE_PASSWORD = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($passwordPtr)
    .\venv_tx_trade_fresh\Scripts\python.exe -m pytest -o addopts="" tests\integration\test_skcom_quote_live.py -v
}
finally {
    [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($passwordPtr)
    $account = $null
    $securePassword = $null
    foreach ($name in $names) {
        [Environment]::SetEnvironmentVariable($name, $saved[$name], "Process")
    }
}
```

Without `TX_TRADE_RUN_LIVE_QUOTE_TEST=1`, the test is skipped. The test verifies
login, quote-monitor readiness, `TX00` lookup, and quote/tick subscription.
Because markets may be closed, receiving a live tick is not required.

## Phase 2A deterministic replay

Phase 2A now provides the contract-first replay core in `tx_trade.replay`.
It replays only validated, complete, non-empty Phase 1 SQLite sessions and
publishes the original `MarketDataEnvelope` values in authoritative
`ingest_sequence` order.

The runtime supports fastest and paced playback, exclusive restart cursors,
acknowledged pause/resume/stop controls for callers outside the replay worker,
and fixed sanitized failure codes. A reentrant pause/stop call from
`sink.publish` records a request and is acknowledged after that callback
returns, avoiding a worker self-join.
Paced playback uses `event_at` only for timing and never changes event order.
It does not load SKCOM, read live credentials, connect Reply, create Order, or
send real orders.

Run the Phase 2A contract and integration tests with:

```powershell
.\venv_tx_trade_fresh\Scripts\python.exe -B -m pytest `
    -p no:cacheprovider `
    --basetemp .\.pytest_tmp\phase2a `
    -o addopts="" `
    tests\unit\test_replay_contracts.py `
    tests\unit\test_replay_runtime.py `
    tests\unit\test_phase2_import_guards.py `
    tests\integration\test_phase2_replay_sqlite.py `
    -q
```

Run one complete SQLite recording as canonical JSON Lines:

```powershell
$env:TX_TRADE_RUNTIME_PRESET = "phase2_replay"
$env:TX_TRADE_REPLAY_DB_PATH = "D:\path\to\recordings.sqlite3"
$env:TX_TRADE_REPLAY_SESSION_ID = "00000000-0000-0000-0000-000000000000"
$env:TX_TRADE_REPLAY_MODE = "fastest" # or paced
$env:TX_TRADE_REPLAY_SPEED = "1.0"
# Optional exclusive cursor:
# $env:TX_TRADE_REPLAY_AFTER_INGEST_SEQUENCE = "100"

.\venv_tx_trade_fresh\Scripts\python.exe -B -m tx_trade.app.phase2 `
    > .\replay.jsonl
```

The database must already exist, be offline, and have no active `-wal` or
`-shm` sidecar. Stop the recorder and allow SQLite to checkpoint before replay.
The CLI opens the source with SQLite `mode=ro&immutable=1`; source database
bytes and sidecars are not changed. Missing, active, incomplete, empty,
corrupt, or unsupported-schema sessions fail closed without creating a
database. Standard output contains JSON Lines only; fixed success or failure
summaries go to standard error. This entry point reads only the six replay
settings above and does not read live credentials.

## Phase 2B deterministic research paper replay

Phase 2B-1 through 2B-4 provide immutable paper-order contracts, a
deterministic broker and matching/fee policies, transactional strategy
decision batches, and a standalone research-paper replay CLI. A market
envelope and its ordered strategy commands commit to the in-memory paper
broker as one transaction. Orders created from envelope `N` are first
eligible on a later envelope, preventing look-ahead.

The research CLI is deliberately separate from the Phase 2A replay CLI:

```powershell
.\venv_tx_trade_fresh\Scripts\python.exe -B -m tx_trade.app.research_paper `
    > .\research-paper.jsonl
```

It requires the explicit `research_paper` preset and the strict
`TX_TRADE_RESEARCH_PAPER_*` setting set defined by
`tx_trade.app.research_paper_config`. All limits, execution policies, the
paper run UUID, and the built-in instrument-triggered order template must be
provided explicitly. Any replay cursor is rejected because a fresh in-memory
broker cannot safely resume partway through a session.

Output is buffered until replay and broker processing complete. Standard
output then contains versioned deterministic JSON Lines in `market`, `paper`,
and terminal `summary` records. The broker's internal event journal is the
authoritative paper event source. Durable broker checkpoints and a durable
output outbox are not implemented yet.

This mode does not import SKCOM, create Center/Quote/Reply/Order objects, read
live credentials or `TX_TRADE_SKCOM_DLL_PATH`, connect Reply, or submit a live
order.
