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
output outbox are available when restart mode is enabled.

Phase 2B-5 adds an independent writable paper-state SQLite database. The
Phase 1 recording remains immutable and read-only. To create a resumable run,
add the following settings to the complete `research_paper` environment:

```powershell
$env:TX_TRADE_RESEARCH_PAPER_RESTART_MODE = "create"
$env:TX_TRADE_RESEARCH_PAPER_STATE_DB_PATH = "D:\path\to\paper-state.sqlite3"
$env:TX_TRADE_RESEARCH_PAPER_MAX_STATE_MAIN_DB_BYTES = "268435456"
```

After an interrupted run, use the same semantic settings and paper run UUID,
then change only:

```powershell
$env:TX_TRADE_RESEARCH_PAPER_RESTART_MODE = "resume"
```

`create` refuses an existing state database, while `resume` requires one.
Source and state paths must be distinct local regular files; aliases,
hardlinks, symbolic links, reparse paths, and network paths fail closed. A raw
replay cursor remains forbidden: the exclusive resume cursor is derived only
from the validated durable state.

`MAX_STATE_MAIN_DB_BYTES` limits the SQLite main database's logical page
capacity. SQLite WAL/SHM sidecars and filesystem overhead are not included;
the state directory therefore needs additional free space and should be
protected by an appropriate filesystem quota when a hard total-disk limit is
required.

Each committed envelope stores the strategy decision, complete broker and
coordinator checkpoints, durable cursor, and output rows in one paper-state
transaction. Restart therefore preserves matching FIFO, N+1 eligibility,
idempotency fences, and cached strategy decisions. A completed resume emits
the complete artifact again.

The database provides exactly-once durable broker effects and outbox enqueue.
Delivery to stdout or a pipe remains at-least-once because a process can stop
after a successful external write but before that write can be acknowledged.
Consumers that require exactly-once effects must apply their own idempotency
boundary.

This mode does not import SKCOM, create Center/Quote/Reply/Order objects, read
live credentials or `TX_TRADE_SKCOM_DLL_PATH`, connect Reply, or submit a live
order.

## Phase 2 final acceptance

Phase 2 final acceptance exercises both module entry points in real child
processes, SQLite-backed replay lifecycle controls, deterministic paper
results, abrupt-process recovery, independent durable outbox readback and
local-path safety. It remains an offline gate and must not be used to opt in to
legacy COM, live quote, stress or trading behavior.

Run the dedicated acceptance tests with:

```powershell
.\venv_tx_trade_fresh\Scripts\python.exe -B -m pytest `
    -p no:cacheprovider `
    --basetemp .\.pytest_tmp\phase2-final `
    -o addopts="" `
    tests\integration\test_phase2_final_replay.py `
    tests\integration\test_phase2_final_paper.py `
    tests\integration\test_phase2_final_recovery.py `
    tests\unit\test_phase2_final_safety.py `
    -q
```

Run the complete safe offline regression explicitly excluding every opt-in
class:

```powershell
.\venv_tx_trade_fresh\Scripts\python.exe -B -m pytest `
    -p no:cacheprovider `
    --basetemp .\.pytest_tmp\phase2-final-full `
    -o addopts="" `
    -m "not legacy_com and not stress and not live" `
    -q
```

The accepted 2026-07-28 baseline is `14 passed, 3 skipped` for the dedicated
gate, `618 passed, 4 skipped` for the Phase 2 targeted suite, and
`888 passed, 4 skipped, 6 deselected` for the full safe offline regression.
Platform skips are explicit Windows symlink or mapped-drive fixture
limitations. The previously recorded Phase 1 quote-only live smoke remains
`8 passed`; final acceptance does not reconnect it.

## Phase 3A live-order contracts

Phase 3A defines side-effect-free live-order contracts without enabling live
execution. The domain models, pure reducer and ports live in:

- `tx_trade.orders.live_contracts`
- `tx_trade.orders.live_state_machine`
- `tx_trade.orders.live_ports`

Capital's normalized domestic-futures Reply contracts and strict
`OnNewData` parser live in:

- `tx_trade.broker.capital.trading_contracts`
- `tx_trade.broker.capital.reply_parser`

The parser follows the 49 explicitly named fields in the bundled Capital
`12.回報.docx`. It does not guess a local `client_order_id`; broker evidence
remains candidate or ambiguous until a later reconciliation layer confirms
the link. Dispatch success is transport evidence only and never changes an
order to `ACCEPTED`.

Phase 3A does not import or initialize COM, read credentials or DLL settings,
create `SKOrderLib`, call `SKReplyLib_ConnectByID`, register a live callback,
or send/cancel/amend any broker order. Importing the focused contracts/parser
modules also does not eagerly load the existing Phase 1 Paper or Capital
runtime modules.

Run the Phase 3A offline gate with:

```powershell
.\venv_tx_trade_fresh\Scripts\python.exe -B -m pytest `
    -p no:cacheprovider `
    --basetemp .\.pytest_tmp\phase3a `
    -o addopts="" `
    tests\unit\test_live_order_contracts.py `
    tests\unit\test_live_order_state_machine.py `
    tests\unit\test_live_order_ports.py `
    tests\unit\test_capital_trading_contracts.py `
    tests\unit\test_capital_reply_parser.py `
    tests\unit\test_phase3_import_guards.py `
    tests\unit\test_capital_import_guards.py `
    -q
```

The targeted Phase 3A baseline is `207 passed`. The integrated safe offline
regression is `1086 passed, 4 skipped, 6 deselected`.

## Phase 3B-1 durable live-order journal

Phase 3B-1 adds a fail-closed SQLite v1 journal without enabling a broker
connection. It persists immutable intents, exact commands, permanent dispatch
claims, transport receipts, raw observations, normalized broker facts,
application outcomes, fills, reconciliation requirements and materialized
order projections.

The journal uses a single global append sequence, versioned canonical payloads
and domain-separated digests. Resume validates the schema, every authoritative
table and its global-record mapping before allowing access. A committed claim
without a receipt is outcome-unknown and never authorizes automatic resend.

Run its offline gate with:

```powershell
.\venv_tx_trade_fresh\Scripts\python.exe -B -m pytest `
    -p no:cacheprovider `
    --basetemp .\.pytest_tmp\phase3b1 `
    -o addopts="" `
    tests\unit\test_live_order_journal_contracts.py `
    tests\unit\test_live_order_journal_codec.py `
    tests\unit\test_sqlite_live_order_journal.py `
    tests\unit\test_live_order_journal_recovery.py `
    tests\unit\test_phase3_import_guards.py `
    tests\integration\test_live_order_journal_process_recovery.py `
    -q
```

The targeted Phase 3B-1 gate is `87 passed`; the combined Phase 3A/3B-1 gate
is `274 passed`. The full safe offline regression is
`1153 passed, 4 skipped, 6 deselected`.

This slice does not import or initialize COM, read live credentials or DLL
settings, connect Reply, create Order, register callbacks, or invoke any
broker operation.

The journal path must be inside a deployment-controlled private local
directory. Python's standard SQLite API cannot bind a database connection to
the already-validated OS file handle across every supported platform, so an
adversary able to replace files concurrently inside that directory remains
outside this slice's threat model.

## Phase 3B-2 fake-only reconciliation

Phase 3B-2 adds immutable local and broker reconciliation snapshots, a pure
deterministic assessment, and thin fake-only snapshot orchestration. The
service makes one atomic snapshot-bundle call to a broker source; that source
owns and attests the coherent cut and cursor for its open-order, fill and
position evidence. The SQLite v1 journal exposes a read-only account snapshot
and recomputes strategy position attribution from durable fills already present
in this journal whenever the process resumes. This is not a production opening
balance or an independently authoritative portfolio position.

The assessment reports order, fill and position mismatches. It infers absence
only when the corresponding broker query evidence is complete, treats
candidate/conflicting identity as ambiguous and fails closed, and keeps an
order with a pending command from resuming. An assessment is diagnostic only:
`may_dispatch` is always `False`.

All three broker evidence sets must carry the same snapshot cut token and must
not predate the local snapshot. Durable recovery blockers, including
outcome-unknown claims, unresolved/conflicting/ambiguous observations and open
reconciliation requirements, also participate in `may_resume`. Local fills
must refer to a local order and match its typed account/strategy/instrument/side
identity, time bounds and aggregate filled quantity. Broker fill observations
map injectively to durable local fills, so one local fill cannot satisfy
multiple observations. Position totals use exact, context-independent
`Decimal` summation.

Run the focused Phase 3B-2 offline gate with:

```powershell
.\venv_tx_trade_fresh\Scripts\python.exe -B -m pytest `
    -p no:cacheprovider `
    --basetemp .\.pytest_tmp\phase3b2 `
    -o addopts="" `
    tests\unit\test_live_reconciliation_contracts.py `
    tests\unit\test_live_reconciliation.py `
    tests\integration\test_live_reconciliation_fake.py `
    -q
```

The focused new tests are `55 passed`; the combined Phase 3 gate is
`319 passed`. The full safe offline regression is
`1218 passed, 5 skipped, 2 deselected`. Its split is
`1119 passed, 4 skipped` unit tests and `96 passed, 1 deselected` integration
tests.

This slice does not import or initialize COM/SKCOM, read credentials or DLL
settings, connect Reply, create Order, register callbacks, perform a live
query, dispatch or submit an order. Assessment does not write broker evidence,
resolve a durable reconciliation requirement or dispatch claim, or change the
SQLite v1 schema; it is recomputed after restart. A committed claim without a
receipt remains outcome-unknown and is never resent automatically. Durable
reconciliation commit/resolution and its schema migration are a later planning
item. Any query-only SKCOM adapter and every real-order step still require a
separate PLAN and explicit authorization.

## Phase 3B-3 durable reconciliation commit

Phase 3B-3 adds an offline durable commit boundary for evidence-backed
reconciliation and migrates the live journal from SQLite schema v1 to v2. The
database schema version and payload codec version are intentionally separate:
new and migrated journals use schema v2, while existing canonical journal
payloads remain codec v1 and v1 journal data remains readable.

A reconciliation commit is one atomic compare-and-swap over the assessed
journal sequence, expected order versions, claim tokens and durable target
preconditions. Reusing a `commit_id` with the identical canonical request
returns the original durable result; different content is an ID conflict, and
stale cuts or versions fail closed. Resolution rows are append-only overlays:
the original claim, observation and requirement evidence is retained. The
supported resolutions are broker-order or broker-fill confirmation for a
claim/observation, and `satisfied` for a requirement. They require durable
evidence provenance, never synthesize a dispatch receipt and never authorize
redispatch. Recovery validates the overlays and excludes only successfully
resolved blockers; `may_dispatch` remains `False`.

The v2 implementation deliberately does not commit caller-projected orders:
any non-empty `order_projections` request returns `UNSUPPORTED_RESOLUTION`.
A dispatch claim can be resolved only for a NEW command whose local order was
already durably accepted with no pending command by a broker event, and whose
existence is then corroborated by an authoritative broker order/fill snapshot.

Run the focused offline contract, migration, repository and recovery gate with:

```powershell
.\venv_tx_trade_fresh\Scripts\python.exe -B -m pytest `
    -p no:cacheprovider `
    --basetemp .\.pytest_tmp\phase3b3 `
    -o addopts="" `
    tests\unit\test_live_reconciliation_commit_contracts.py `
    tests\unit\test_live_journal_v2_schema_sql.py `
    tests\unit\test_live_journal_v2_migration.py `
    tests\unit\test_live_journal_reconciliation_commit.py `
    tests\integration\test_live_reconciliation_commit_fake.py `
    tests\integration\test_live_order_journal_process_recovery.py `
    -q
```

The 2026-08-03 Commander offline quality gate passed: formatter check covered
149 files, scoped `ruff check tx_trade tests` passed, mypy reported no issues
in 65 source files, unit tests were `1171 passed, 4 skipped`, integration tests
were `105 passed, 1 skipped`, and the full default offline suite was
`1276 passed, 4 skipped, 6 deselected`. These integration tests use offline
SQLite/fake-broker paths; they do not exercise a real broker integration.

This remains an offline persistence and recovery slice. It adds no production
broker query adapter, COM/SKCOM initialization, credential or DLL access,
Reply/Order connection, callback registration, dispatch permission or real
order operation. Query-only SKCOM integration and every real-order step still
require a separate plan and explicit authorization.

## Phase 3B-4 authoritative recovery projection

Phase 3B-4 adds one deliberately narrow, offline recovery path. A pure
projector can produce an `ACCEPTED` order only when a recomputed authoritative
assessment contains exactly one unique confirmed, complete working open-order
observation for an existing zero-fill `SUBMISSION_UNKNOWN` or `RECONCILING`
order with its exact pending `NEW` command. Account, client order, instrument,
side, total and remaining quantity, limit price and observation time must all
match; correlated fills, duplicates, incomplete evidence and every unsupported
state fail closed.

The SQLite journal commits that canonical projection and the matching dispatch
claim resolution in one compare-and-swap transaction. It advances the order
exactly once, clears the pending command, retains the original evidence and
claim audit trail, and does not create a receipt, broker event, fill, broker ID
or dispatch permission. Exact retries reproduce the original projections and
write nothing. Recovery verifies the stored request, projection history and
claim-resolution mapping before excluding the resolved blocker.

The operator-facing workflow in this slice is also pure and offline: it plans
recovery actions and builds typed commit requests from supplied immutable
inputs. It does not inspect a production journal, query a broker or accept
operator JSON evidence. `may_dispatch` remains `False`, and no automatic resend
or real dispatch path exists.

The 2026-08-04 Commander gate passed: formatter check covered 163 files,
scoped Ruff passed, mypy reported no issues in 69 source files, import guards
were `45 passed`, unit tests were `1270 passed, 4 skipped`, integration tests
were `114 passed, 1 skipped`, and the full default offline suite was
`1384 passed, 4 skipped, 6 deselected`. All integration coverage here uses
offline/fake SQLite and broker evidence.

Phase 3 remains in progress. Production read-only journal inspection, a trusted
assessment source, an authorized operator/audit workflow (likely requiring
schema v3), query-only broker integration and every real-order operation remain
separately planned and gated. This slice adds no COM/SKCOM initialization,
credential or DLL access, broker query adapter, Reply/Order connection, live
callback, production CLI or real-order authorization.

## Phase 3B-5 production read-only journal inspection

Phase 3B-5 adds the one-shot library API
`inspect_sqlite_live_order_journal(path, account_id=...)`. It returns a strict,
frozen and redacted deterministic report, attributes recovery blockers to the
selected account or to journal-global state, and exposes only bounded opaque
target identifiers. Public failures use stable sanitized codes and messages.
The inspector installs SQLite authorizers for two bounded read stages and never
exposes a writable journal object. Every report keeps
`may_dispatch=False` and `commit_allowed=False`.

The supported source boundary is intentionally exact: only a cleanly closed
journal with no `-wal`, `-shm` or `-journal` sidecar is accepted. Any complete
or partial sidecar is classified as `ACTIVE_OR_UNCLEAN_SOURCE` before SQLite is
connected. The main database is then opened with
`mode=ro&immutable=1&cache=private`. This boundary was narrowed after empirical
testing showed that plain `mode=ro` could mutate existing SHM bytes; safe WAL
inspection therefore requires a separately designed snapshot/copy protocol.

Before any substantive inspection, a short source transaction performs only
bounded serialization and binds the SQLite serialized bytes to the already
verified descriptor digest. Those verified bytes are then loaded into an
isolated in-memory connection, where a separate transaction runs all
substantive schema, durable-payload and report queries against the sealed image.
A final descriptor hash check still rejects any source change observed during
inspection.

Schema v1 is validated only far enough to return
`SCHEMA_UPGRADE_REQUIRED`; inspection never migrates it or executes v2 queries.
For schema v2, the inspector verifies the complete durable payload before
building the canonical report. Integrity failures fail closed without leaking
account IDs, durable IDs, tokens, raw evidence, paths or SQLite diagnostics.

The Phase 3B-5 Commander gate passed: formatter check covered 172 files, Ruff
passed, mypy reported no issues in 72 source files, unit tests were
`1363 passed, 4 skipped`, offline integration tests were
`119 passed, 1 deselected`, and the full default offline suite was
`1482 passed, 4 skipped, 6 deselected`.

Phase 3 remains in progress. This slice adds no CLI, JSON evidence/input,
trusted assessment source, operator authorization/audit workflow, broker query,
COM/SKCOM integration, credential or DLL access, dispatch, resend,
receipt/event/fill synthesis or real order. Next planning items include a safe
WAL snapshot/copy protocol if desired, output-only CLI composition, a trusted
assessment source, authorization/audit schema work and query-only broker
integration.

## Phase 3B-5.1 inspection hardening (2026-08-10)

Phase 3B-5.1 hardens the existing inspection library without expanding its
functional scope. Both the main database and serialized image are limited to
64 MiB, the total durable-row budget is 25,000, and SQLite work is bounded by a
deterministic progress handler checked every 1,000 virtual-machine opcodes with
a maximum of 100,000 callbacks. Resource-limit and `MemoryError` paths expose
only the sanitized `CAPACITY_EXCEEDED` failure; if the final descriptor hash
also detects a changed source, `SOURCE_CHANGED` retains precedence.

The durable inspection reader is now a normal connection-bound read-only
object with stateless path helpers. The inspector no longer constructs a
journal through `object.__new__`: its caller owns the connection and
transaction. The writable journal keeps its existing connection lifecycle and
poisoning semantics.

Hardening coverage includes the complete SQLite authorizer deny/allow behavior
matrix, all 16 account-attribution cases, a deterministic query-growth gate,
and fresh-process tests whose parent runner starts outside the repository.
The attribution matrix preserves genuine ambiguous history and
resolves it through the public reconciliation commit path; the durable
verifier now accepts the corresponding `UNRESOLVED` application while retaining
the two-candidate ambiguity requirement. Resolution events must also identify
an actual durable ambiguity candidate; non-candidate events are rejected with
zero writes and forged durable resolutions fail closed on reopen. Progress and
cleanup fault paths preserve the documented callback and `MemoryError`
precedence boundaries. The final Commander gate passed:
formatter check covered 172 source/test files, Ruff passed, mypy reported no
issues in 72 source files, focused tests were `150 passed`, unit tests were
`1451 passed, 4 skipped`, offline integration tests were
`122 passed, 1 deselected`, and the full default offline suite was
`1573 passed, 4 skipped, 6 deselected`.

The clean-close/no-sidecar and sealed-image boundaries remain unchanged. This
hardening adds no CLI, WAL snapshot/copy, schema migration, broker or COM
integration, credential access, dispatch, resend, receipt/event/fill synthesis
or real order. The next planned functional slice remains the output-only CLI;
performance batch/stream/RSS benchmarking and a safe WAL snapshot remain
deferred.

## Phase 3B-5.2 output-only live-journal inspection CLI (2026-08-11)

Phase 3B-5.2 exposes the hardened read-only inspector through one output-only
module command:

```text
python -B -m tx_trade.live_journal_inspection_cli --journal PATH --account-id ID
```

This source-tree command is supported only with a trusted Python
interpreter/virtual environment, a trusted current working directory, and a
trusted, explicitly controlled `PYTHONPATH` and Python startup environment. Run
it from the repository root in this currently non-packaged repository; from a
trusted repo-external working directory, set `PYTHONPATH` explicitly to the
absolute trusted repository path. `-B` only disables bytecode writes; it is not
an isolation option. An untrusted current directory, `PYTHONPATH`,
`sitecustomize`/other startup customization, or shadow package is outside the
CLI security boundary and must not be used.

The CLI read-only, redaction, no-application-environment-input, and
no-side-effect guarantees begin only after that trusted Python bootstrap. From
that point, the command accepts no stdin or JSON evidence input and performs no
migration, reconciliation commit, broker/COM operation, credential or DLL
access, dispatch/resend, order/reply synthesis, or journal mutation. If a
trusted installable distribution is provided in the future, the production
recommendation is isolated mode:

```text
python -I -B -m tx_trade.live_journal_inspection_cli --journal PATH --account-id ID
```

This repository is not currently packaged or installable, and `-I` does not
make the module importable directly from this source tree.

Successful inspection output is exactly one canonical, ASCII, single-line JSON
document with `output_schema_version=1`, capped at 256 KiB. Its public allowlist
redacts account IDs, paths and raw durable IDs; recovery targets use bounded
opaque IDs. `inspection_digest` and target IDs identify inspection content and
targets only: they are not authentication tokens. Every report keeps
`may_dispatch=false` and `commit_allowed=false`.

The frozen process exit mapping is:

- `0`: `ready_no_action`
- `2`: invalid CLI request
- `10`: `recovery_required`
- `11`: `schema_upgrade_required`
- `12`: `account_not_found`
- `13`: `blocked_integrity_failure`
- `20`: typed inspection, internal, or output failure

`--help` also returns `0`, but exits without producing an inspection report.
All exit codes are diagnostic only and never authorize dispatch or a
reconciliation commit. Automation must require and validate the versioned
canonical report rather than treating exit status alone as authorization; a
valid report still contains `may_dispatch=false` and `commit_allowed=false`.

Current Commander evidence is: format check covered 175 files; Ruff passed;
mypy passed over 73 source files; the focused CLI gate was `89 passed`; unit
tests were `1520 passed, 4 skipped`; and offline integration tests were
`136 passed, 1 deselected`. The final full suite passed with
`1656 passed, 4 skipped, 6 deselected` in 224.53 seconds. The pytest cache
warning is limited to the environmental Windows `.pytest_cache` ACL; runs using
isolated basetemps succeeded.

Phase 3B-5.2 implementation, final correctness fix, and Commander final
full-suite validation are complete. The correctness, security/permissions, and
tests/maintainability final delta reviews are complete; no open
BLOCKER/HIGH/MEDIUM/LOW findings remain, and all reviewers recommend merge. The
Phase 3B-5.2 Commander quality gate is complete; no commit or merge is claimed.
Phase 3 remains in progress. The next planned functional slice is the existing
roadmap item for a trusted assessment source; authorization/audit schema work,
query-only broker integration, performance benchmarking and a safe WAL
snapshot/copy protocol remain separately planned or deferred.

## Trusted assessment source (2026-08-13)

The trusted assessment source adds the frozen one-shot library API
`tx_trade.orders.sqlite_live_reconciliation_assessment.assess_sqlite_live_order_journal(path, *, account_id, broker_snapshot_source, clock) -> InspectedReconciliationAssessment`.
It seals and validates the SQLite local journal, creates redacted inspection
provenance and the exact `LocalReconciliationSnapshot` from the same isolated
image and transaction, makes one call to the injected atomic
`BrokerReconciliationSnapshotSourcePort` bundle, and locally recomputes the
assessment.

The trusted application bootstrap or caller chooses the in-process broker
source; that selection is this slice's only broker-source trust decision. The
runtime Protocol proves callable shape only. `snapshot_id` and `source_cursor`
enforce internal atomic-bundle consistency only. The local sealed-image
SHA-256 detects local content change and integrity only. None authenticates
broker identity or provenance. This slice adds no cryptographic signatures or
key management. All validation uses fake broker evidence and local SQLite, so
it is not proof of production broker authentication or querying.

Only a schema-v2 journal whose inspection status is ready or
recovery-required, with the requested account present, proceeds to the broker
call. Schema v1, a missing account, journal integrity or sidecar failure, and
source failure stop before any broker call. Aggregate broker observations are
capped at 25,000. Nested broker contracts are revalidated even if forged
frozen values are supplied, and public failures use stable sanitized codes.

Every result has `may_dispatch=False` and `commit_allowed=False`. This API
does not authorize resume, reconciliation commit, dispatch or resend; the
downstream pure recovery planner and separate explicitly authorized durable
commit flow retain their existing independent checks. It performs no broker
adapter or live query, COM/SKCOM operation, configuration/environment/
credential/DLL/network access, stdin or operator JSON evidence ingestion,
migration or schema change, journal mutation, claim/receipt/commit, dispatch,
resend, synthesized receipt/event/fill, cache or retry.

Current Commander evidence is: format check covered 181 files; Ruff passed;
mypy passed over 75 source files; focused tests were `317 passed`; unit tests
were `1598 passed, 4 skipped`; and offline integration tests were
`157 passed, 1 deselected`. The final full suite passed with
`1755 passed, 4 skipped, 6 deselected` in 366.00 seconds. Implementation, the
final LOW missing-slot test fix, security/test hardening and all validation are
complete. Correctness and security delta reviews approved with no open
findings; tests/maintainability review approved merge. The suggested
`SOURCE_CHANGED` semantic refinement remains optional deferred work only. The
final reviewer gate is complete with no open BLOCKER/HIGH/MEDIUM/LOW findings,
and all reviewers recommend merge. The slice is committed; no merge is
claimed. Phase 3 remains in progress. No next functional slice has been selected;
authorization/audit schema work and query-only broker integration remain
separately gated, while performance batch/stream/RSS benchmarking and a safe
WAL snapshot/copy protocol remain deferred.
