# 專案工作與檢討紀錄

這份文件記錄每次工作結果。架構與長期方向請參考
[`TRADING_PLATFORM_ROADMAP.md`](TRADING_PLATFORM_ROADMAP.md)。

## 紀錄格式

每次工作結束後，新增一筆：

```text
## YYYY-MM-DD：工作主題

目標：

完成：

驗證：

未完成／風險：

決策：

下一步：
```

## 2026-07-25：建立平台建置藍圖

目標：

- 將 `tx_trade` 發展為多策略共用的行情與交易核心。

完成：

- 盤點現有程式的登入、行情、OrderLib 與 ReplyLib 能力。
- 確認專案內 `PythonExampleV2` 包含官方 Login、Quote、Order、Reply
  範例與完整說明文件。
- 指定國內期貨優先參考 `TFOrder.py`、`Reply.py`、`Quote.py` 及對應
  DOCX。
- 建立分階段架構與執行順序。
- 定義各階段驗收條件、實盤閘門及決策紀錄方式。

驗證：

- 本次只進行程式盤點及文件建立，未修改交易程式。

未完成／風險：

- 現有虛擬環境無法正常啟動。
- live/offline 行為尚未安全分離。
- 尚未完成真實委託與回報生命週期。

決策：

- 先執行 Phase 0，再進入穩定行情服務。
- 策略不得直接操作 SKCOM。
- Capital adapter 的介面實作以同版本 `PythonExampleV2` 官方資料為依據。

下一步：

- 開始 Phase 0 的環境、編碼與安全模式整理。

## 2026-07-26：建立 Phase 0 基線與完成 Phase 1 七個 implementation slice

目標：

- 建立可離線驗證、可持久化、可完整讀回且嚴格 quote-only 的 Phase 1
  行情 recorder。

完成：

- 建立 Phase 0 的依賴、安全模式與 live fail-closed 基線。
- 建立 Phase 1 架構契約與最初的 quote-only safety tests
  （`f2b24be`）。
- Slice 1：行情資料模型與設定 contract（`344d4be`）。
- Slice 2：pipeline ports、offline fixture、readback 與 sequencer contract
  （`b577c1a`）。
- Slice 3：SQLite event log、repository、writer 與 round-trip storage
  （`e5afe7b`）。
- Slice 4：bounded ingress pipeline、health、metrics 與 stress coverage
  （`2c2767b`）。
- Slice 5：只建立 Center/Quote 的 Capital STA adapter（`61f07cd`）。
- Slice 6：`QuoteClient` façade 相容層與 legacy snapshot
  （`b5cdcc3`）。
- Slice 7：預設 offline、live 明確 opt-in 的安全 recorder entry point
  （`ab9e5b9`）。

驗證：

- 各 slice 均附 deterministic unit/integration/guard tests。
- safety guard 覆蓋不建立 Order/Reply、不註冊 Reply callback、不下單，
  以及 live 任一步失敗不得 fallback 成 offline success。

未完成／風險：

- Phase 0 尚未完成：README 需補齊 Python 版本、建立 venv、執行
  `pip install` 的步驟，並需在 fresh environment 依文件完成乾淨環境
  重建與測試驗證。
- 真實 SKCOM quote-only smoke 需要有效 DLL、帳密、明確 opt-in 與合適
  市場時段，尚未列入離線完成條件。

決策：

- Phase 1 production composition 僅允許 Center/Quote；
  SKOrderLib/SKReplyLib、帳務、部位、委託與成交回報均不在此階段。
- Phase 1 在完成真實 live quote-only smoke 前維持「進行中」。

下一步：

- 執行最終離線 release validation，並安排市場時段 live quote-only smoke。

## 2026-07-27：Phase 1 離線 release validation 與 Phase 2 排程

目標：

- 確認七個 slice 整合後的離線 release 品質，並固定下一階段邊界。

完成：

- 完成 Phase 1 離線 release validation：`268 passed, 1 skipped`；唯一
  skip 為需明確 opt-in 的真實 SKCOM live quote 測試。
- 在同一資料庫執行兩次 offline recorder；兩個 session ID 不同、狀態
  都是 `complete`，每個 session 都有 6 個事件，最後
  `ingest_sequence=5`，完整讀回成功。
- 將 Phase 2 正式拆為 2A Replay Runtime、2B PaperBroker，並維持
  Center/Quote only 的既有安全邊界；Phase 2 不接 Order/Reply、不送單。

驗證：

- 全套離線測試：`268 passed, 1 skipped`。
- 兩次 offline session：distinct、`complete`、各 6 events、
  last sequence 5，readback integrity 通過。

未完成／風險：

- 真實 SKCOM quote-only smoke 延後到市場時段；需確認登入、quote ready、
  `TX00` lookup 及 quote/tick subscription。
- 此 live smoke 必須在 Phase 2 最終驗收前完成；完成前 Phase 1 不標示
  「已完成」。

決策：

- Phase 2 先固定 Replay Runtime 的 deterministic timing、cursor 與
  lifecycle，再在其上建立 PaperBroker。
- Phase 2 不建立 SKOrderLib/SKReplyLib、不註冊 Order/Reply callback，
  真實委託及回報留待 Phase 3。

下一步：

- 設計 Phase 2A `ReplayRuntime` public contract、狀態機、clock 與 cursor
  semantics，並安排市場時段 Phase 1 live quote-only smoke。

## 2026-07-27：新工作階段交接註記

目標：

- 讓新 Commander 僅依 roadmap 與 work log 即可續辦 Phase 0、Phase 1
  live smoke 與 Phase 2A，且不跨越 quote-only 安全邊界。

完成：

- 記錄接手基線：分支 `main`；交接開始時文件 `HEAD` 為 `3980b0d`，
  最新程式碼提交為 `ab9e5b9`。
- 確認既有未追蹤 `phase1_smoke.sqlite3` 是本機 release validation
  artifact，不得 commit。
- 將 fresh-env 與 live smoke 的完整接手 checklist 加入 roadmap
  第 15 節；未記錄任何 credential value。

驗證：

- 既有文件記錄的自動化證據為 `268 passed, 1 skipped`，本次未重跑。
- 既有文件記錄的 SQLite 證據為兩個 distinct、`complete` session，
  各 6 events、last sequence 5、readback integrity 通過，本次未重驗。
- `venv_tx_trade\pyvenv.cfg` 記錄 Python `3.13.14` known baseline；本次
  受限工具宿主因 Windows Store/App Execution Alias 限制無法重驗，不能
  據此判斷既有 venv 健康度，fresh-env 證據仍缺。

未完成／風險：

- Phase 0：README 尚待補入 Python 3.13 系列、新建 venv、requirements
  install、`pip check`、imports 與全套 pytest 步驟，並在 fresh env
  記錄 Python 完整版本及每一步的實際成功證據。`3.13.14` 是 known
  baseline，不是 exact patch pin，除非未來另有正式決策。
- Phase 0 的 README 待辦另包含安全化 live 明文 assignment 示例；目前
  test/app entry point 不會自動載入 `.env`，不得示範將真實 secret
  literal 寫入可能留存的 shell history。
- Phase 1：尚待在 Windows 市場時段以
  `TX_TRADE_RUN_LIVE_QUOTE_TEST=1` 執行 quote-only integration test；
  必須另有有效的 `TX_TRADE_SKCOM_DLL_PATH`、`TX_TRADE_ACCOUNT` 與
  `TX_TRADE_PASSWORD`。未 opt-in 時真實 live case 預期 skip。
- 測試 opt-in `TX_TRADE_RUN_LIVE_QUOTE_TEST` 與 production opt-in
  `TX_TRADE_ENABLE_LIVE_QUOTE` 不得互相替代；production app 還要求
  `TX_TRADE_RUNTIME_PRESET=phase1_live_quote` 與 `TX_TRADE_SYMBOLS`。

決策：

- live test 只驗證登入、quote ready、`TX00` lookup 與 quote/tick
  subscription；休市不要求收到 tick。全程只允許 Center/Quote，
  不建立 Order/Reply、不註冊 Reply callback、不送單。
- live secret 僅能由核准 secret store、無 history prompt 或專用短生命
  shell 注入；測試結束須清除或還原 test opt-in、帳密及 DLL path。
  production runtime 若在同一 shell 執行，其 opt-in、preset、symbols、
  DB path 與 timeout 變數也須清除或還原。
- live smoke 成功後，須在 roadmap 與本 log 補上日期、遮蔽敏感值的
  指令及證據，再把 Phase 1 標為「已完成」；且必須在 Phase 2 最終
  驗收前完成。
- Phase 2 下一個且唯一 slice 僅為 Phase 2A 設計 `ReplayRuntime`
  contract、狀態機、clock、cursor；offline fixtures 只可作此 slice 的
  acceptance。首個 slice 不設計或實作 PaperBroker，也不碰 COM 或
  Order/Reply；Phase 2B abstraction、fill policy、fees 必須等 Phase 2A
  contract 驗收通過後才設計。

下一步：

1. 完成 Phase 0 README 與 fresh-env 實證。
2. 安排 Phase 1 市場時段 quote-only live smoke，成功後更新兩份文件。
3. 僅啟動 Phase 2A 的 design-first contract slice；Phase 2B 保留在
   Phase 2A contract 驗收後的 backlog，且 Phase 2 final acceptance 前
   必須關閉前兩項缺口。

## 2026-07-27：完成 Phase 0 fresh-environment 基線

目標：

- 依文件從零建立可執行的 Python 3.13 環境，補齊品質工具、安全測試分類
  與不洩漏 credential 的 live 操作說明。

完成：

- 安裝 Python `3.13.14` 並建立全新的 `venv_tx_trade_fresh`；此完整版本是
  本次證據，不是永久 exact patch pin。
- README 補齊 venv、requirements、`pip check`、imports、Ruff、mypy 與
  pytest 指令；production/test live opt-in 仍相互獨立。
- `legacy_com`、`stress`、`live` 測試改為明確 marker，預設 suite 不會
  意外初始化 legacy COM、執行重型 stress 或連接真實 quote service。
- live credential 範例改為 prompt 後只注入目前 process，並在 `finally`
  還原；未將 credential 真值寫入文件、輸出或 commit。
- 修正 Phase 1 現有型別 narrowing，使 mypy 可對 26 個 source files
  完整通過；未改 schema、public API 或 Order/Reply 安全邊界。

驗證：

- `pip check`：無 broken requirements；必要 imports 成功。
- Ruff format check 與 lint：通過。
- mypy：`Success: no issues found in 26 source files`。
- 完整非交易 suite：`266 passed, 1 skipped, 2 deselected`；skip 是未
  opt-in 的真實 quote live case，deselected 是 legacy COM tests。
- 預設安全 suite：`263 passed, 6 deselected`。
- stress：`3 passed`。
- 同一 SQLite database 連續執行兩次 offline recorder：兩個不同 session
  均為 `complete`、各 6 events、last `ingest_sequence=5`，readback
  integrity 均為 valid。

未完成／風險：

- Phase 1 真實 SKCOM quote-only smoke 已嘗試執行；兩個不連線 safety
  tests 通過，真實 case 在 login code `2017` 失敗，未進入 quote ready
  或 subscription，因此不得視為通過。
- 首次失敗輸出暴露了 pytest fixture tuple 中的 credential repr；相關
  credential 必須撤銷／更換後才能重試。測試 fixture 已改為固定的
  redacted repr，後續失敗輸出不再顯示帳號或密碼。
- Phase 1 仍維持「進行中」，待 live login、quote ready、`TX00` lookup、
  quote/tick subscription 與 cleanup 證據完成後再更新狀態。

決策：

- Phase 0 標示為「已完成」。
- Phase 2 維持暫停；必須先完成並由使用者確認 Phase 0、Phase 1。

## 2026-07-28：完成 announcement-only Reply 例外與 Phase 1 live smoke

目標：

- 在不接回報主機、不建立 Order、不送單的前提下，滿足 SKCOM 登入前
  必須註冊公告 callback 的要求，並關閉 Phase 1 live gate。

完成：

- 隨附 SKCOM V2.13.58 文件確認 code `2017` 表示登入前必須建立
  `SKReplyLib` 並註冊 `OnReplyMessage`；文件同時明定不需先做回報連線。
- 經使用者明確核准，legacy quote-only façade 與 production backend
  建立 announcement-only Reply；sink 只包含 `OnReplyMessage` 並回傳
  `-1`。
- hard guards 禁止 `SKOrderLib`、`SKReplyLib_ConnectByID`、`OnNewData`、
  `OnStrategyData` 與送單；非 Phase 1 legacy façade 的相容行為保持不變。
- cleanup 在取消 quote/tick 後加入 bounded Windows message drain，避免
  native `SKQuoteLib_LeaveMonitor` 在非同步取消尚未完成時 access violation。

驗證：

- 本次 Reply／cleanup 安全回歸 targeted：`34 passed, 1 deselected`。
- 完整非交易 suite：`273 passed, 1 skipped, 2 deselected`。
- Ruff format/lint 與 mypy（26 source files）：通過。
- 真實 live smoke：`8 passed`；登入、quote ready、`TX00` lookup、
  quote/tick subscription、credential redaction 與 cleanup 均通過。
- live process 僅建立 Center、Quote、announcement-only Reply；未建立
  Order、未呼叫 `ConnectByID`、未註冊委託／成交 callback、未送單。

決策：

- Phase 1 標示為「已完成」。
- Phase 2 仍不自動開始，等待使用者確認 Phase 0、Phase 1 結果。

## 2026-07-28：開始 Phase 2A contract-first Replay Runtime

目標：

- 只實作 Phase 2A 第一切片：Replay contract、狀態機、可中斷播放時鐘、
  exclusive cursor 與 complete SQLite session gate。
- Phase 2B PaperBroker 維持未開始。

完成：

- 新增 immutable replay state/mode/options/session descriptor/snapshot 與
  固定 sanitized failure codes。
- 新增 FASTEST/PACED 背景 replay runtime；cursor 僅在成功 publish 後
  前移，pause/resume/stop 具 acknowledgment，terminal runtime 不重啟。
- PACED 只用 `event_at` 計時，不改變 `ingest_sequence` 權威順序；
  `event_at=None` 立即送出，倒退時間不產生負等待。
- 新增 SQLite fail-closed gate，只接受 complete、current-schema、
  non-empty、integrity-valid recording；未修改 Phase 1 readback/finalize。
- 新增同一 SQLite session 連續兩次回放的 deterministic integration test。
- 新增 Phase 2 專用 import/security guards，確認不載入 COM/Capital、
  不讀 live credential 或 `.env`，不建立 Order/Reply、不送單。

目前驗證：

- Phase 2A contracts/runtime/SQLite/security targeted：`80 passed`。
- 完整非交易 regression suite：`353 passed, 1 skipped, 2 deselected`。
- Ruff format/lint：通過；mypy：31 個 source files，0 errors。

決策：

- Phase 2A cursor 是最後成功 publish 的 `ingest_sequence`，恢復採
  exclusive 語意；交付保證為 at-least-once，不宣稱 exactly-once。
- 第一切片不新增 durable cursor database schema。
- Phase 2B 必須等待 Phase 2A 第一切片驗收及使用者確認後才開始。

## 2026-07-28：Phase 2A 獨立設定、composition 與 CLI

目標：

- 讓使用者能以既存 SQLite DB、session UUID、播放模式、速度與 exclusive
  cursor 實際啟動 ReplayRuntime。
- 保持 Phase 1 composition 與 live credential 路徑完全分離。

完成：

- 新增純函式 `parse_phase2_replay_settings`；只讀六個 replay 白名單鍵，
  不列舉 mapping、不讀帳密、DLL 或 `.env`，parse 時不碰 filesystem。
- 新增 frozen `Phase2ReplaySettings`，固定 preset=`phase2_replay`、
  execution=`disabled`。
- 新增 `ReplayRuntime.wait(timeout)`，等待自然終態而不提出 stop。
- 新增 `tx_trade.app.phase2` composition root 與
  `python -m tx_trade.app.phase2` CLI。
- DB 不存在時在 repository factory 前拒絕，不建立空 DB；repository 在
  成功、失敗與中斷路徑都關閉。
- replay repository 強制 SQLite `mode=ro&immutable=1`，不切換 WAL、不建
  schema；來源 DB bytes/sidecars 前後一致。偵測到 active `-wal`/`-shm`
  即拒絕，要求 recorder 停止並 checkpoint 後才 replay。
- CLI 使用同步 `ReplayRuntime.run()`，JSONL sink 與 source 都在主執行緒；
  KeyboardInterrupt 不會留下仍輸出或仍存取 repository 的背景 worker。
- stdout 僅輸出 canonical JSON Lines；summary/error 使用固定 sanitized
  stderr 訊息，成功 exit 0、失敗或中斷 exit 2。
- import/security guard 已擴及 replay app/config，確認不載入 COM、
  Capital、root config、Order/Reply 或 live credential。

目前驗證：

- Phase 2A 全部 contract/runtime/config/composition/app/security targeted：
  `150 passed`。
- 完整非交易 regression suite：`423 passed, 1 skipped, 2 deselected`。
- Ruff format/lint：通過；mypy：33 個 source files，0 errors。

決策：

- 不修改 root `main.py` 或 Phase 1 parser；Phase 2 使用獨立 module entry。
- 第一版 CLI 不接受任意命令列參數，所有設定經 replay-only whitelist
  parser；未知 argv fail closed。
- Phase 2B PaperBroker 仍未開始。
# 2026-07-28: Phase 2B-4 transactional research paper replay

Scope:

- Complete the next Phase 2B slice without changing the existing Phase 2A
  replay CLI or enabling live execution.

Delivered:

- Immutable ordered `PaperDecision` batches with decision fingerprints and
  retry/conflict fences.
- Atomic PaperBroker processing: existing-order matching and the current
  envelope's ordered strategy commands commit together.
- Declarative strategy contracts, deterministic coordinator caching, and an
  instrument-triggered built-in research strategy.
- Strict, explicit `TX_TRADE_RESEARCH_PAPER_*` settings; any replay cursor is
  rejected.
- A synchronous read-only SQLite composition and standalone
  `python -m tx_trade.app.research_paper` CLI.
- Buffered deterministic `market`, authoritative `paper`, and terminal
  `summary` JSONL records.
- Import/security and failure-injection coverage for COM isolation,
  transaction rollback, retry idempotency, and byte-identical reruns.

Boundaries:

- No SKCOM/Capital import, credentials, DLL path, Reply connection, Order
  object, callback, or live order path.
- The internal bounded broker journal is authoritative. Durable broker
  checkpoints and a durable output outbox are intentionally deferred.
- `phase1_smoke.sqlite3` is unrelated local data and remains untouched.

## 2026-07-28: Phase 2B-5 durable recovery and output outbox

Scope:

- Add process-restart recovery without weakening the immutable Phase 1 source,
  deterministic paper semantics, or the no-COM/no-live-order boundary.

Delivered:

- Strict persistence contracts for run identity, versioned broker/coordinator
  checkpoints, optimistic state versions, durable batches, and output rows.
- Canonical checkpoint codecs that restore all broker retry, matching,
  eligibility, journal, position, decision-cache, and pending-decision state.
- A separate writable SQLite paper-state repository with schema/application
  version checks, migration checksum, WAL/FULL durability, capacity limits,
  atomic batch commits, duplicate/conflict fences, and corruption detection.
- Restart modes `disabled`, `create`, and `resume`. Raw replay cursors remain
  rejected; resume derives its cursor from validated durable state.
- Full source-content and semantic configuration identity validation.
- Per-envelope atomic persistence of the strategy decision, broker and
  coordinator checkpoints, committed cursor, and market/paper outbox rows.
- Terminal summary persistence, completed-run re-emission, and byte equality
  against the existing schema-v1 JSONL materializer.
- Crash, stale-writer, corruption, flush failure, source immutability, COM
  isolation, and hardlink/symlink/reparse path safety coverage.

Delivery semantics:

- Broker effects and durable outbox enqueue are exactly-once within the
  paper-state database transaction.
- stdout/pipe delivery is at-least-once. A completed resume intentionally
  re-emits the complete artifact because stdout has no acknowledgment
  protocol.

Boundaries:

- The Phase 1 recording remains read-only and immutable.
- The state database must be a distinct local regular file.
- `MAX_STATE_MAIN_DB_BYTES` limits SQLite main-database logical pages only;
  WAL/SHM sidecars require separate filesystem capacity/quota planning.
- No SKCOM/Capital import, credential or DLL read, Reply/Order connection,
  callback registration, or live order path was added.
- `phase1_smoke.sqlite3` remains unrelated and untouched.

## 2026-07-28: Phase 2 final acceptance

Scope:

- Close Phase 2A and Phase 2B-1 through 2B-5 with process-level,
  recovery-level and full release evidence.
- Preserve the offline/no-live-order boundary. Existing Phase 1 quote-only
  smoke evidence is a prerequisite, not authorization to connect trading
  services during this gate.

Added acceptance evidence:

- Real module subprocess runs for `tx_trade.app.phase2` and
  `tx_trade.app.research_paper`, including exit status, stderr contract,
  canonical UTF-8 JSONL and byte-identical repeated stdout.
- SQLite-backed replay pause/resume and stop lifecycle tests using explicit
  acknowledgements rather than timing sleeps.
- Observation-only strategy evaluation with no paper broker effect, plus paper
  provenance, fill, fee and position-conservation assertions.
- A real abruptly killed durable child process followed by a new resume
  process. Recovered stdout, checkpoints, cursor, ledger and independently
  reopened outbox match a clean run exactly.
- Direct local-path safety coverage for UNC, mapped drives,
  junctions/reparse points, symlinks where permitted, and state/source
  hardlink collision before a writable repository is opened.

Validation:

- New final-acceptance tests: `14 passed, 3 skipped`.
- Phase 2 targeted gate: `618 passed, 4 skipped`.
- Full safe offline regression:
  `888 passed, 4 skipped, 6 deselected`.
- The skips are explicit platform cases where Windows symlink creation is not
  permitted or no mapped network drive exists.
- Phase 1 quote-only live smoke prerequisite remains the previously recorded
  `8 passed`.

Boundaries and decision:

- Phase 2 is accepted as complete.
- No production API, checkpoint format, output schema, SQLite schema or
  migration changed during final acceptance.
- Phase 1 source bytes and source WAL/SHM state remain unchanged.
- stdout/pipe delivery remains at-least-once; durable broker effects and
  outbox enqueue remain exactly-once inside the paper-state transaction.
- No SKCOM trading import, live credential or DLL read, Order object, trading
  Reply connection, order/fill callback or real order was added or exercised.
- Phase 3 requires a separate PLAN PHASE and explicit authorization.

## 2026-07-29: Phase 3A side-effect-free live-order contracts

Scope:

- Define the internal live-order boundary before implementing any broker
  connection, persistence or side effect.
- Preserve Phase 1 quote-only and Phase 2 replay/paper behavior.

Delivered:

- Strict immutable contracts for live intents, new/cancel/amend/decrease
  commands, order aggregates, fills, broker positions, strategy attribution,
  dispatch receipts, account readiness, correlation evidence and
  reconciliation discrepancies.
- Global-v1 `client_order_id` uniqueness semantics and independent
  `client_command_id` values for every command operation.
- A pure reducer covering created, validated, submitting, accepted, partial,
  filled, rejected, cancel-pending, cancelled, submission-unknown and
  reconciling states.
- Exact event retry/conflict handling. Candidate, ambiguous, mismatched or
  overfill evidence requests reconciliation without being recorded as an
  applied event, so later confirmed evidence can still be reduced.
- Separate runtime-checkable ports for account catalog, broker dispatch,
  Reply source, broker queries, durable journal, order service,
  reconciliation and normalized event sinks.
- Strict Capital TF `OnNewData` parsing based on the 49 explicitly named
  official fields. The documentation's `[4] N/A` BuySell position is not
  treated as an invented fifth encoded character.
- Fill price uses the official single-leg `Price1` field. Broker/account
  identity is supplied as an opaque account ID; raw broker account evidence is
  hidden from normal repr and parse errors.
- Domestic-futures `U` and `B` replies use the official `Qty` decrement rather
  than the securities-only `AfterQty`; amendment facts must exactly match the
  pending command's expected price and/or resulting total before they apply.
- Lazy compatibility exports for `tx_trade.orders` and
  `tx_trade.broker.capital`, preventing focused Phase 3A imports from eagerly
  loading Paper or Capital runtime modules.

Validation:

- Phase 3A contracts, reducer, ports, Capital parser and existing Phase 1
  Capital safety gate: `207 passed`.
- Existing lazy-export Paper/Phase 1 compatibility tests: `103 passed`.
- Full safe offline regression:
  `1086 passed, 4 skipped, 6 deselected`.
- Ruff passed; mypy reported no issues in 58 source files.

Boundaries:

- Dispatch success is transport evidence only; it never means broker
  acceptance.
- Broker callbacks do not contain local `client_order_id`; Phase 3A does not
  guess one.
- No SQLite live journal or production adapter was added.
- No COM import/initialization, credential or DLL read, Order creation,
  trading Reply connection, live callback registration or broker order call
  was added or exercised.
- Any SKCOM query-only integration and any real-order smoke require separate
  planning and explicit authorization.

## 2026-07-30: Phase 3B-1 durable live-order journal

Delivered:

- Added strict versioned live-journal contracts and canonical codec with
  bounded Decimal parsing, explicit type discriminators and domain-separated
  digests.
- Added a SQLite v1 durable journal with verified schema identity, read-only
  resume prevalidation, WAL/FULL durability, global fact sequence, optimistic
  order CAS, exact retry/conflict handling and poisoned fail-closed behavior.
- Commands and permanent dispatch claims commit before any future external
  side effect. A claim without a receipt remains outcome-unknown and never
  permits automatic resend.
- Raw observations commit before normalization. Normalized facts,
  application dispositions, reconciliation requirements, fills, order
  projections and global records commit atomically.
- Resume verifies reservations, complete order history, commands, claims,
  receipts, raw observations, normalized facts, event applications,
  reconciliation rows, fills and their global-record mappings.
- Added pure recovery classification and subprocess abrupt-exit tests. Pending
  commands, outstanding claims, conflict observations and reconciliation
  requirements keep startup in reconciliation-required state.

Validation:

- Phase 3B-1 targeted gate: `87 passed`.
- Combined Phase 3A and Phase 3B-1 gate: `274 passed`.
- Full safe offline regression:
  `1153 passed, 4 skipped, 6 deselected`.
- Ruff format/check passed; mypy reported no issues in 62 source files.

Boundaries:

- No COM initialization, credential/DLL read, Reply connection, Order
  creation, callback registration or broker operation was added or exercised.
- Full fake broker reconciliation remains Phase 3B-2.
- Journal files must live in a deployment-controlled private local directory;
  concurrent hostile path replacement inside that trusted directory is not
  fully preventable through Python's standard SQLite API.
- Query-only SKCOM integration and every real-order smoke still require
  separate planning and explicit authorization.

## 2026-08-02: Phase 3B-2 fake-only reconciliation

Delivered:

- Added strict immutable local and broker snapshots plus a pure deterministic
  reconciliation assessment.
- Added fake-only orchestration around one atomic snapshot-bundle call to the
  broker source. The source owns and attests the coherent cut and cursor; the
  service neither assembles three sequential query results nor manufactures a
  snapshot ID.
- Added a read-only SQLite account snapshot without changing schema v1.
  Strategy position attribution is recomputed after resume only from the net
  quantity of durable fills in this journal; it is not a production opening
  balance.
- Added order, fill and position mismatch reporting. Absence is inferred only
  from complete evidence; incomplete evidence and candidate/conflicting
  identity remain incomplete or ambiguous and fail closed.
- Pending commands block resume, and the diagnostic assessment's
  `may_dispatch` is always `False`.
- Required open-order, fill and position evidence to share the same broker cut
  token and to be no older than the local snapshot.
- Included durable outstanding claims, unresolved/conflicting/ambiguous
  observations and open reconciliation requirements as `may_resume` blockers.
- Hardened local fill referential integrity, time bounds and exact typed
  account/strategy/instrument/side identity. Fill and position aggregation now
  uses exact, context-independent `Decimal` summation. Broker fill observations
  map injectively to durable local fills.

Validation:

- Focused new Phase 3B-2 tests: `55 passed`.
- Combined Phase 3 gate: `319 passed`.
- Full safe offline regression: `1218 passed, 5 skipped, 2 deselected`.
- Commander final gate passed: Ruff format/check, mypy over 64 source files,
  `1119 passed, 4 skipped` unit tests and `96 passed, 1 deselected`
  integration tests.

Boundaries:

- No COM/SKCOM initialization, credential or DLL read, Reply connection,
  Order creation, callback registration, live query or dispatch was added or
  exercised.
- Assessment is read-only: it does not persist broker evidence, resolve a
  durable reconciliation requirement or dispatch claim, or change SQLite
  schema v1. It is recomputed after restart.
- A claim without a receipt remains outcome-unknown and is never automatically
  resent.
- The next candidate planning item is a durable reconciliation
  commit/resolution protocol plus schema migration. Any query-only SKCOM
  adapter and every real-order step require a separate PLAN and explicit
  authorization.

## 2026-08-03: Phase 3B-3 durable reconciliation commit and SQLite v2

Scope:

- Add an offline, atomic persistence boundary for committing an already
  assessed reconciliation result, plus a SQLite v1→v2 journal migration.
- Preserve all Phase 3 safety gates: no production broker integration and no
  dispatch permission.

Design decisions and delivered behavior:

- Database schema version and payload codec version are independent. Fresh and
  migrated journals use SQLite schema v2; canonical journal payload encoding
  stays at v1, preserving v1 payload compatibility and existing creation
  identity.
- A commit request binds one account to the assessment's durable journal cut,
  broker snapshot, expected order versions, claim tokens and typed target
  preconditions. The repository applies them in one transaction or makes no
  change.
- Exact retry is keyed by caller-supplied `commit_id` plus the canonical
  request. An identical retry returns the original durable result, including
  timestamp and resulting sequence; changed content is an ID conflict. Stale
  journal sequence, version or target preconditions fail closed.
- Resolution records are append-only overlays. Existing dispatch claims, raw
  observations and reconciliation requirements are retained for audit and are
  not rewritten into invented broker facts.
- Supported evidence-backed resolutions are broker-order/broker-fill
  confirmation for a claim or observation, and `satisfied` for a requirement.
  No dispatch receipt is synthesized. Claim resolution never authorizes resend
  or changes the invariant that an unresolved outcome is never redispatched.
- The conservative v2 implementation does not write caller-projected orders;
  any non-empty `order_projections` request returns `UNSUPPORTED_RESOLUTION`.
  A claim resolution is limited to a NEW claim whose local order was already
  durably accepted with no pending command by a broker event, and whose
  existence is then corroborated by an authoritative broker order/fill
  snapshot.
- Recovery validates the migration history, commit request provenance and
  resolution overlays. Only successfully resolved blockers leave the pending
  recovery view; `may_dispatch` remains `False`.

Offline validation commands:

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

Validation status:

- Commander offline quality gate passed.
- Ruff formatter check covered 149 files; scoped `ruff check tx_trade tests`
  passed; mypy reported no issues in 65 source files.
- Unit suite: `1171 passed, 4 skipped`.
- Offline integration suite: `105 passed, 1 skipped`.
- Full default offline suite: `1276 passed, 4 skipped, 6 deselected`.
- Repository-wide Ruff still reports three pre-existing errors confined to
  unmodified root debug scripts; the scoped maintained-code gate above passed.
- The integration suite used SQLite and fake broker evidence only. It did not
  connect to or validate a real broker integration.

Explicit exclusions and safety boundary:

- No COM/SKCOM initialization, credential or DLL read, Reply/Order connection,
  callback registration, production broker query, dispatch permission or real
  order is enabled or exercised.
- `may_dispatch=False` remains a diagnostic safety invariant. Durable commit
  records evidence-backed resolution; it is not permission to dispatch.
- Query-only SKCOM work, full Reply/Order integration and every real-order
  smoke require a separate plan and explicit authorization.
- Next candidate work is a pure authoritative order projector plus an
  operator-facing recovery workflow.

## 2026-08-04: Phase 3B-4 authoritative recovery projection

Scope and completed safe slice:

- Added a pure authoritative order projector for the single supported recovery
  case: an existing zero-fill `SUBMISSION_UNKNOWN` or `RECONCILING` order with
  its exact pending `NEW` command and one unique confirmed complete working
  broker open-order observation.
- The evidence must match account, client order ID, instrument, side, total and
  remaining quantity, limit price and time. Remaining must equal total, and no
  correlated fill may exist. Every other state/evidence shape fails closed.
- The canonical projection advances the durable order exactly once to
  `ACCEPTED`, clears the pending command and sets `accepted_at` and `updated_at`
  to the broker observation time while preserving intent and zero-fill values.
- Added pure offline operator recovery planning and typed request building.
  It consumes explicit immutable inputs only and performs no production
  journal inspection, broker query or operator JSON evidence ingestion.
- Extended the SQLite v2 repository to atomically compare the assessed journal
  cut, order version, pending NEW command, claim token, authoritative plan and
  matching claim resolution; then persist projection history, claim overlay
  and reconciliation commit as one transaction.
- Exact retry returns the original timestamp, sequence and projected order with
  no write. Durable resume recomputes the projector and verifies projection
  history plus the projection-to-claim-resolution mapping. Tampered, missing or
  projection-only repaired records fail closed.
- Projection creates no dispatch receipt, normalized broker event, fill,
  broker ID, clock, UUID or permission. Recovery removes the resolved blocker,
  but `may_dispatch=False` and the command cannot be resent automatically.

Commander validation status:

- Ruff formatter check covered 163 files; scoped Ruff passed.
- Mypy reported no issues in 69 source files.
- Import guards: `45 passed`.
- Unit suite: `1270 passed, 4 skipped`.
- Offline integration suite: `114 passed, 1 skipped`.
- Full default offline suite: `1384 passed, 4 skipped, 6 deselected`.
- Tests used offline/fake SQLite and broker evidence only.

Explicit exclusions and remaining Phase 3 work:

- No COM/SKCOM initialization, credential/DLL access, broker query adapter,
  Reply/Order connection, live callback, receipt/event/fill synthesis,
  automatic resend, production CLI, real dispatch or real order was added or
  exercised.
- Production read-only journal inspection, a trusted assessment source,
  authorized operator/audit persistence (likely schema v3), query-only broker
  integration and every real-order step remain deferred and separately gated.
- Phase 3 remains in progress; Phase 3B-4 completes only this offline safe
  projection and recovery-planning slice.

## 2026-08-09: Phase 3B-5 production read-only journal inspection

Scope and completed safe slice:

- Added the one-shot library API
  `inspect_sqlite_live_order_journal(path, account_id=...)`. It returns a
  strict frozen/redacted deterministic report and never exposes a writable
  journal object.
- Reports distinguish selected-account recovery work from journal-global
  blockers, use bounded opaque target identifiers, and expose only sanitized
  failure codes/messages. SQLite authorizers constrain both bounded read
  stages.
- The accepted source boundary is exactly a cleanly closed journal with no
  `-wal`, `-shm` or `-journal`. Any complete or partial sidecar returns
  `ACTIVE_OR_UNCLEAN_SOURCE` before SQLite connect. The main database opens as
  `mode=ro&immutable=1&cache=private`.
- The boundary was narrowed after empirical testing found that plain
  `mode=ro` could mutate existing SHM bytes. This slice therefore makes no
  claim that a live or WAL-backed journal can be inspected safely in place.
- A short source transaction performs only bounded serialization and binds the
  SQLite serialized bytes to the verified descriptor digest. Those bytes are
  loaded into an isolated in-memory connection, where a separate transaction
  runs every substantive schema, durable-payload and report query against the
  sealed image. A final descriptor hash check still detects source changes.
- Schema v1 receives validation only and returns
  `SCHEMA_UPGRADE_REQUIRED`; the inspector performs no migration and issues no
  v2 query against it. Schema v2 must verify its complete durable payload
  before building the canonical report.
- Every returned report has `may_dispatch=False` and
  `commit_allowed=False`.

Commander validation status:

- Ruff formatter check covered 172 files; Ruff passed.
- Mypy reported no issues in 72 source files.
- Unit suite: `1363 passed, 4 skipped`.
- Offline integration suite: `119 passed, 1 deselected`.
- Full default offline suite: `1482 passed, 4 skipped, 6 deselected`.
- The initial system-Python Ruff command failed because Ruff was not installed;
  rerunning with the project virtual environment passed. Initial pytest setup
  errors came from a missing nested `--basetemp` parent; after creating a
  dedicated parent directory, all suites passed. Pytest cache permission
  warnings did not affect the results.

Explicit exclusions and remaining Phase 3 work:

- No CLI, JSON evidence/input, trusted assessment source, operator
  authorization/audit, broker query, COM/SKCOM initialization, credential/DLL
  access, dispatch, resend, receipt/event/fill synthesis or real order was
  added or exercised.
- Phase 3 remains in progress. Next planning items include a safe WAL
  snapshot/copy protocol if desired, output-only CLI composition, a trusted
  assessment source, authorization/audit schema work and query-only broker
  integration.
