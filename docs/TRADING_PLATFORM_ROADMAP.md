# 程式交易平台建置藍圖

> 文件狀態：執行中
> 建立日期：2026-07-25  
> 最後更新：2026-07-27
> 適用專案：`tx_trade`

## 1. 文件目的

本文件是本專案的長期執行基準，用來：

- 記錄系統要解決的問題與架構方向。
- 安排實作順序，避免在基礎未穩定前直接進入實盤。
- 定義每個階段的完成條件。
- 保存重要技術決策及其原因。
- 讓日後的開發、檢討與交接有共同依據。

每次完成重要功能或改變方向時，都應同步更新本文件。

## 2. 專案目標

把目前直接操作群益 Capital SKCOM 的程式，發展成一個可供多個策略共用的交易核心。

交易核心負責：

- 連接券商。
- 提供即時報價。
- 接收策略委託。
- 執行統一風控。
- 送出、修改及取消委託。
- 處理委託與成交回報。
- 維護委託、成交及部位狀態。
- 處理斷線、重連與重啟後對帳。
- 提供監控、紀錄及緊急停止能力。

策略只負責：

- 訂閱所需行情。
- 計算交易訊號。
- 送出標準化委託意圖。
- 依標準化回報更新策略內部狀態。

策略不得直接呼叫 SKCOM，避免各策略各自處理 COM、帳號、重連和風控。

## 3. 目前狀態

目前已具備：

- Phase 0 的依賴與安全模式基線；fresh-env 建立文件及乾淨環境驗證尚待
  完成。
- Phase 1 的標準行情模型、pipeline contract、SQLite event log/readback、
  bounded ingress、health/metrics、Capital quote-only STA adapter、相容 façade
  與安全 recorder entry point。
- 預設 `offline/disabled`；live quote 必須明確 opt-in，失敗時不會降級成
  offline success。
- 離線錄製可建立彼此獨立的 session，依 session-global
  `ingest_sequence` 完整讀回。
- live quote composition 建立 SKCOM Center/Quote，以及登入必要且只註冊
  `OnReplyMessage` 的 announcement-only Reply；不建立 Order、不呼叫
  `ConnectByID`、不註冊委託／成交 callback，也不提供下單能力。

目前主要缺口：

- Phase 1 真實 SKCOM quote-only smoke 已於 2026-07-28 完成：登入、
  ready、`TX00` lookup、quote/tick subscription 與 cleanup 均成功。
- Phase 2A contract-first Replay Runtime 第一切片已實作、正在最終驗收；
  Phase 2B PaperBroker 尚未開始。
- 舊 `QuoteClient` façade 仍保留相容行為，後續須在不破壞既有 contract
  的前提下逐步收斂。
- 尚未實作完整的新單、改單、刪單。
- 尚未解析完整委託及成交回報。
- 沒有委託狀態機、冪等控制及重啟對帳。
- 沒有部位與損益管理。
- 沒有集中式風控。

## 3.1 API 官方參考資料

本專案內的 `PythonExampleV2/` 是群益 Capital API 商提供的說明與
Python 範例，後續實作應優先參考該版本：

- `PythonExampleV2/Login/LoginForm.py`：登入、帳號事件與初始化流程。
- `PythonExampleV2/Quote/Quote/Quote.py`：國內報價、Tick 與連線事件。
- `PythonExampleV2/Order/TF/TFOrder/TFOrder.py`：國內期貨一般委託、
  刪單、減量、改價、未平倉及權益數。
- `PythonExampleV2/Order/TF/TFStrategyOrder/TFStrategyOrder.py`：
  國內期貨智慧單；第一版一般委託穩定後再評估。
- `PythonExampleV2/Reply/Reply.py`：委託與成交回報事件。
- `PythonExampleV2/策略王COM元件使用說明_PythonExampleV2/`：
  環境、登入、下單、回報與國內報價的正式說明文件。

參考資料的使用原則：

1. 方法簽章、COM 結構欄位、事件名稱與代碼意義，以同版本官方說明為準。
2. 官方 GUI 範例用於確認正確呼叫順序，不直接整份複製進交易核心。
3. 將 GUI、全域變數與顯示邏輯移除，轉換為可測試的 Capital adapter。
4. 每個封裝方法應在註解或測試中標明所依據的官方檔案與函式。
5. 官方範例與 DOCX 不一致時，先記錄差異並以受控測試確認，不自行猜測。
6. API 或 DLL 升級時，先比較新版範例和說明，再調整 adapter。
7. `PythonExampleV2` 視為唯讀參考來源；專案功能不得直接修改或依賴其 GUI。

## 4. 目標架構

```text
策略 A ─┐
策略 B ─┼─> Strategy API / Python SDK
策略 C ─┘                │
                         ▼
                   Trading Core
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
     Market Data    Order Manager    Risk Manager
          │              │              │
          └──────────────┼──────────────┘
                         ▼
                  Capital Adapter
                         │
              Quote / Order / Reply COM
```

建議的程式結構：

```text
tx_trade/
  broker/
    base.py
    capital/
      connection.py
      quote_adapter.py
      order_adapter.py
      reply_adapter.py
  market_data/
    models.py
    service.py
    subscriptions.py
  orders/
    models.py
    service.py
    state_machine.py
  portfolio/
    models.py
    reconciliation.py
  risk/
    rules.py
    service.py
  strategies/
    base.py
    runner.py
  api/
    server.py
    client.py
  storage/
    repository.py
  monitoring/
    health.py
    metrics.py
  tests/
```

## 5. 核心設計原則

### 5.1 券商隔離

只有 `broker/capital` 可以使用 SKCOM 型別、方法名稱及錯誤碼。其他模組只使用專案自己的資料模型。

### 5.2 模式必須明確

系統模式分成：

- `replay`：歷史行情回放。
- `paper`：即時或回放行情搭配模擬成交。
- `live`：真實行情與真實委託。

`live` 模式下，DLL、登入、報價、Order 或 Reply 任一必要服務異常時，必須拒絕送單。不得自動退回 offline 並假裝成功。

### 5.3 預設拒絕

帳號、行情、部位、Reply 狀態或風控資料不完整時，一律拒絕新委託。

### 5.4 單一委託入口

所有策略委託都必須經過：

```text
輸入驗證 → 冪等檢查 → 風控 → 紀錄 → 券商送單 → 回報更新
```

### 5.5 回報才是最終依據

券商 API 呼叫成功只表示請求已送出，不代表委託已接受或成交。最終委託狀態以 Reply、成交回報及券商查詢結果為準。

### 5.6 可恢復

程序重啟後，必須能透過本地 journal 加上券商未成交單、成交與部位查詢恢復狀態。

## 6. 標準資料模型

至少建立：

- `Quote`
- `Tick`
- `ConnectionStatus`
- `OrderRequest`
- `Order`
- `OrderEvent`
- `Fill`
- `Position`
- `AccountSnapshot`
- `RiskDecision`

每筆委託至少包含：

- `strategy_id`
- `client_order_id`
- `account`
- `symbol`
- `side`
- `quantity`
- `order_type`
- `price`
- `time_in_force`
- `day_trade`
- `created_at`

`client_order_id` 必須唯一，用來防止重試造成重複下單。

## 7. 委託狀態機

```text
CREATED
   │
   ▼
VALIDATED
   │
   ▼
SUBMITTING
   ├──> ACCEPTED ──> PARTIALLY_FILLED ──> FILLED
   │          └────> CANCEL_PENDING ─────> CANCELLED
   └──> REJECTED
```

實作時必須定義：

- 每種券商回報對應哪個內部狀態。
- 哪些狀態轉換合法。
- 重複及亂序回報如何處理。
- 斷線期間收到成交後如何補回。
- 改單在券商端實際是修改還是刪單重送。

## 8. 執行階段與驗收條件

### Phase 0：專案基線與安全修正

目前交付狀態（2026-07-27）：

- 依賴檔、安全模式與 live fail-closed 基線已建立。
- README 已補齊 Python 3.13 series、fresh venv、requirements、品質檢查及
  安全測試流程，並改用不把 credential literal 留在 shell history 的
  process-scope 注入與還原範例。
- 已以全新 Python 3.13.14 與 `venv_tx_trade_fresh` 從零安裝依賴；
  `pip check`、imports、Ruff format/lint、mypy、完整非交易測試及 stress
  tests 均通過，因此 Phase 0 標示為「已完成」。

工作內容：

- 重建可正常啟動的 Python 虛擬環境。
- 增加依賴檔與環境建立說明。
- 統一 UTF-8 編碼。
- 修正 README。
- 將 live、paper、replay 模式明確分開。
- live 初始化失敗時採 fail-fast。
- 確認 `.env`、帳密與 log 不會進入版本控制。
- 建立基本 logging 與設定驗證。

驗收條件：

- 新環境可依文件一次建立完成。
- 測試可穩定執行。
- 未提供真實帳密時不會誤進實盤。
- DLL 或登入失敗時，live 模式不會回傳成功。

### Phase 1：穩定行情服務

目前交付狀態（2026-07-27）：

- 七個 implementation slice 已完成：資料模型與設定、ports/offline
  fixture/readback、SQLite storage、bounded pipeline 與監控、Capital
  quote-only STA adapter、相容 façade、安全 recorder entry point。
- 離線 release validation 為 `268 passed, 1 skipped`；skip 是需明確
  opt-in 的真實 SKCOM live quote 測試。
- 同一資料庫連續執行兩次 offline recorder，得到兩個不同且
  `complete` 的 session；每個 session 都有 6 個事件，最後
  `ingest_sequence=5`，完整讀回驗證成功。
- 安全邊界固定為 Center/Quote 加 announcement-only Reply：Reply 只註冊
  登入必要的 `OnReplyMessage`；不建立 SKOrderLib、不呼叫
  `SKReplyLib_ConnectByID`、不註冊委託／成交 callback，也不送單。
- 真實 SKCOM quote-only smoke 已於 2026-07-28 通過，Phase 1 標示為
  「已完成」。

工作內容：

- 拆出 Capital connection 與 quote adapter。
- 建立標準化 Quote、Tick 和連線事件。
- 將無限制 list 改為 bounded queue 或 callback。
- 建立訂閱及退訂管理。
- 建立商品代碼與券商索引映射。
- COM 操作與 message pumping 固定在專用執行緒。
- 實作心跳、斷線偵測、自動重連及重新訂閱。
- 增加行情延遲與最後更新時間監控。

驗收條件：

- 策略可透過共用介面訂閱 `TX00`。
- 連續運作日盤與夜盤後記憶體保持穩定。
- 人工斷線後能恢復連線及訂閱。
- 過期行情可被辨識，且不能供實盤送單使用。

### Phase 2：Replay Runtime 與 PaperBroker

Phase 2 固定依序交付 2A、2B。整個 Phase 2 不建立或呼叫
SKOrderLib/SKReplyLib，不註冊 Order/Reply callback，不連接真實委託與
成交回報，也不送出真實委託。

#### Phase 2A：Replay Runtime

契約：

- 輸入沿用 Phase 1 `ReplaySource` 與 session-global
  `ingest_sequence`，不另造不相容的行情格式。
- 輸出沿用 Phase 1 `MarketDataEnvelope`/consumer contract，讓下游不需
  分辨資料來自 offline、replay 或 live quote。
- runtime 明確定義 session 選擇、cursor、播放時鐘、速度、啟動、暫停、
  繼續、停止與完成狀態。

工作範圍：

- 依 authoritative event order 讀取已完成的 recording session。
- 實作 deterministic/最快速播放與依事件時間 pacing。
- 實作 pause/resume/stop、游標與完成狀態，不在本階段加入策略或成交
  模擬。
- 對 incomplete/corrupt session、非法速度及讀回 integrity failure
  採 fail-closed。

驗收條件：

- 固定 session 每次輸出相同事件、相同順序與相同終止狀態。
- 最快速模式不依 wall clock 產生非決定性；pacing 模式可由 fake clock
  穩定測試。
- pause/resume 不重複、不遺漏事件；stop 後不再發送事件。
- replay runtime 不載入 SKCOM，亦不觸及 Order/Reply。

#### Phase 2B：PaperBroker

契約：

- 只接受專案內部的模擬委託意圖與行情 envelope；不接受 SKCOM 型別。
- 以 deterministic 規則產生 paper order/fill/position 結果，並明確標示
  `paper` provenance，不能冒充券商 Reply。
- 費用、滑價、成交優先序與部分成交規則必須由設定注入並可重現。

工作範圍：

- 定義供策略使用的最小 broker abstraction 與 PaperBroker。
- 以 replay 行情實作基本成交、拒絕、取消、部分成交、滑價、手續費及
  部位更新。
- 讓同一策略可經設定切換 replay-only observation 與 paper execution，
  不修改策略程式。

驗收條件：

- 相同 session、策略輸入與設定得到完全相同的 paper 結果。
- 成交、費用與部位守恆，非法委託有可追蹤的拒絕原因。
- 測試以 hard guard 證明不建立、不呼叫 SKOrderLib/SKReplyLib，且不
  註冊 Order/Reply callback、不送真單。
- Phase 1 真實 SKCOM quote-only smoke 已完成，才可宣告 Phase 2
  最終驗收通過。

### Phase 3：完整委託及回報

工作內容：

- 查詢與選擇交易帳號。
- 實作期貨新單、刪單、改價及改量。
- 完整解析 ReplyLib 委託與成交事件。
- 建立委託狀態機。
- 保存券商序號、策略序號和成交明細。
- 建立 `client_order_id` 冪等控制。
- 將送單意圖與結果寫入 SQLite journal。
- 實作啟動時未成交單、成交及部位對帳。

驗收條件：

- 測試環境或嚴格限制下可完成一口新單與刪單。
- 可正確處理接受、拒絕、部分成交、完全成交和取消。
- 重送相同 `client_order_id` 不會產生第二筆真實委託。
- 程序重啟後能恢復委託與部位狀態。

### Phase 4：集中式風控

第一版風控：

- 每筆最大口數。
- 每商品、每策略與全帳戶最大部位。
- 每日最大虧損。
- 最大送單頻率。
- 委託價偏離最新行情限制。
- 行情過期禁止送單。
- Quote、Order 或 Reply 未就緒時禁止送單。
- 交易時段與商品白名單。
- 重複委託偵測。
- 全域 kill switch。
- 僅允許平倉模式。

驗收條件：

- 每條風控都有單元測試。
- 每次允許或拒絕都有可追蹤原因。
- kill switch 啟動後不能建立新曝險。
- 風控服務異常時預設拒絕委託。

### Phase 5：多策略共用介面

工作內容：

- 先提供同程序 Python SDK。
- 穩定後建立獨立 Trading Core 常駐程序。
- REST 用於查詢與控制。
- WebSocket 用於行情、委託及成交推播。
- 每個策略使用獨立 `strategy_id`。
- 建立權限、限流及策略資源隔離。

驗收條件：

- 多個策略可同時訂閱行情。
- 某一策略異常不會拖垮交易核心。
- 委託、部位和風控紀錄可依策略區分。
- 慢速策略不會阻塞 COM 事件處理。

### Phase 6：營運與實盤強化

工作內容：

- 健康檢查與監控面板。
- 重要錯誤通知。
- 每日啟動、收盤與對帳流程。
- log rotation、資料備份與保留政策。
- 小額實盤操作手冊。
- 災難恢復演練。

驗收條件：

- 可快速判斷 Quote、Order、Reply 是否健康。
- 異常時能安全停止新增曝險。
- 每日券商部位、成交與本地紀錄一致。
- 有經過演練的人工接管與恢復流程。

## 9. 實盤上線閘門

在以下條件全部完成前，不開放無人值守實盤：

- Phase 0 至 Phase 4 驗收通過。
- Paper 模式連續穩定執行。
- 已驗證斷線、重連、重複回報及程序重啟。
- 每筆委託有唯一識別碼與完整稽核紀錄。
- 部位能與券商對帳。
- kill switch 經過實際演練。
- 商品、帳號、數量及交易時段均有白名單。
- 第一階段 live 限定一個策略、一個商品、單筆一口。
- 實盤初期必須人工監看。

## 10. 每次開發的標準流程

每個功能依下列順序執行：

1. 在本文件或 issue 定義需求、範圍與驗收條件。
2. 先寫資料模型、介面和失敗情境。
3. 實作單元測試。
4. 實作功能。
5. 執行格式檢查、型別檢查及測試。
6. 在 paper/replay 模式驗證。
7. 記錄觀察結果與未解風險。
8. 更新進度與決策紀錄。
9. 涉及實盤時，另外完成上線檢查表。

## 11. 週期性檢討方式

建議每完成一個 Phase 或每兩週檢討一次：

- 已完成什麼？
- 是否符合原驗收條件？
- 發生過哪些異常？
- 哪些假設已被證明錯誤？
- 新增了哪些風險或技術負債？
- 是否需要改變優先順序？
- 下一個最小可交付成果是什麼？

檢討結果應加入下方的進度或決策紀錄，而不是只保留在聊天內容。

## 12. 進度追蹤

| 階段 | 狀態 | 開始日期 | 完成日期 | 備註 |
|---|---|---|---|---|
| Phase 0 專案基線 | 已完成 | 2026-07-25 | 2026-07-27 | fresh Python 3.13.14 環境、依賴、品質檢查與安全測試已驗證 |
| Phase 1 穩定行情 | 已完成 | 2026-07-26 | 2026-07-28 | 離線驗證與真實 quote-only live smoke 均完成 |
| Phase 2 Replay/Paper | 進行中 | 2026-07-28 |  | Phase 2A Replay Runtime 與獨立 CLI 已實作、待整體驗收；Phase 2B 尚未開始 |
| Phase 3 委託與回報 | 待開始 |  |  |  |
| Phase 4 集中式風控 | 待開始 |  |  |  |
| Phase 5 多策略介面 | 待開始 |  |  |  |
| Phase 6 營運強化 | 待開始 |  |  |  |

狀態統一使用：`待開始`、`進行中`、`受阻`、`已完成`。

## 13. 決策紀錄

### ADR-001：策略不得直接操作 SKCOM

- 日期：2026-07-25
- 狀態：接受
- 決策：SKCOM 只存在於 Capital adapter。
- 原因：集中處理 COM 執行緒、重連、錯誤碼、帳號與風控。

### ADR-002：先完成行情穩定性，再開發真實下單

- 日期：2026-07-25
- 狀態：接受
- 原因：行情時效、連線狀態和事件處理是安全下單的必要基礎。

### ADR-003：第一版採單機、SQLite 與簡單 queue

- 日期：2026-07-25
- 狀態：暫定
- 原因：目前規模不需要先引入 Kafka、Redis 等分散式基礎設施。
- 重新評估條件：跨主機策略、吞吐不足或需要高可用時。

### ADR-004：實盤採 fail-closed

- 日期：2026-07-25
- 狀態：接受
- 決策：必要資訊不完整或服務異常時拒絕新增委託。

### ADR-005：Capital adapter 以 PythonExampleV2 官方資料為介面依據

- 日期：2026-07-25
- 狀態：接受
- 決策：SKCOM 呼叫順序、結構欄位與事件格式優先依據專案內
  `PythonExampleV2` 及其同版本說明文件。
- 原因：降低依靠猜測或不同版本範例造成的實盤風險。
- 限制：官方範例是 GUI 示範程式，仍需轉換為隔離、可測試且 fail-closed
  的 adapter，不直接作為正式交易核心使用。

### ADR-006：Phase 2 先完成 Replay Runtime，再建立 PaperBroker

- 日期：2026-07-27
- 狀態：接受
- 決策：Phase 2 分為 2A Replay Runtime 與 2B PaperBroker，依序交付。
- 原因：先固定 deterministic 行情時鐘、cursor 與 lifecycle，PaperBroker
  才能在穩定且可重現的輸入上驗證成交規則。
- 安全邊界：Phase 2 不使用 SKOrderLib/SKReplyLib 或真實 Order/Reply
  callback；真實委託與回報留在 Phase 3。

## 14. 下一個工作項目

目前 implementation slice 是：

1. 驗收 Phase 2A `ReplayRuntime` 的 public contract、狀態機、clock、
   cursor semantics 與 complete SQLite session deterministic replay；
   不得藉此擴大到 Phase 2B。

Phase 2B 的 broker abstraction、paper fill policy 與費用模型屬後續
backlog；必須等 Phase 2A contract 通過驗收後才能開始設計。

獨立的 acceptance gate：在市場時段完成 Phase 1 live quote-only smoke；
此項不是上述 implementation slice 的範圍，但必須在 Phase 2 最終驗收前
完成。

## 15. 新工作階段交接清單（2026-07-27）

### 15.1 接手基線

- 分支為 `main`；交接開始時 `HEAD` 為文件提交 `3980b0d`，其前一個
  `ab9e5b9` 是目前最新的程式碼提交，Phase 1 七個 slice 的程式碼提交
  範圍為 `f2b24be` 至 `ab9e5b9`。
- 既有文件記錄的自動化證據為 `268 passed, 1 skipped`；唯一 skip 是
  預設停用的真實 SKCOM quote-only integration test，本次交接未重跑。
- 既有文件記錄的 release validation SQLite 證據：同一資料庫內兩個不同 session，
  狀態皆為 `complete`，各 6 個事件，最後 `ingest_sequence=5`，完整
  readback 成功；本次交接未重驗。
- repository root 既有未追蹤檔 `phase1_smoke.sqlite3` 是本機 release
  validation artifact，不得加入 commit。

### 15.2 Phase 0 fresh-environment 完成證據

既有 `venv_tx_trade` 的 Windows Store/App Execution Alias 已失效，未作為
完成證據。2026-07-27 另行安裝 Python `3.13.14`，依 README 從零建立
`venv_tx_trade_fresh`；Python policy 仍是 Python 3.13 系列，不將
`3.13.14` 視為永久 exact patch pin。實際完成的重建步驟為：

```powershell
py -3.13 -m venv .\venv_tx_trade_fresh
.\venv_tx_trade_fresh\Scripts\python.exe --version
.\venv_tx_trade_fresh\Scripts\python.exe -m pip install -r .\requirements.txt
.\venv_tx_trade_fresh\Scripts\python.exe -m pip check
.\venv_tx_trade_fresh\Scripts\python.exe -c "import comtypes, pytest, win32api, tzdata"
.\venv_tx_trade_fresh\Scripts\python.exe -m ruff format --check tx_trade tests main.py quote_client.py config.py
.\venv_tx_trade_fresh\Scripts\python.exe -m ruff check tx_trade tests main.py quote_client.py config.py
.\venv_tx_trade_fresh\Scripts\python.exe -m mypy tx_trade
.\venv_tx_trade_fresh\Scripts\python.exe -m pytest -o addopts="" -m "not legacy_com" -q
```

實際結果：`pip check` 無 broken requirements；imports 成功；Ruff
format/lint 通過；mypy 對 26 個 source files 為 0 errors；完整非交易
suite 為 `266 passed, 1 skipped, 2 deselected`，其中 skip 是未 opt-in 的
真實 quote live case，deselected 是 legacy COM tests；獨立 stress gate
為 `3 passed`。同一 fresh venv 的預設安全 suite 另為
`263 passed, 6 deselected`。

README 的 production/test live 範例現以無 history prompt 取得 credential，
並在 `finally` 還原 process environment；文件及驗證輸出未記錄真值。

### 15.3 Phase 1 live quote-only smoke

測試與 production 使用兩個不同的 opt-in，禁止混用：

- integration test opt-in：`TX_TRADE_RUN_LIVE_QUOTE_TEST=1`。測試還要求
  Windows、`TX_TRADE_SKCOM_DLL_PATH` 指向存在的 DLL，以及非空的
  `TX_TRADE_ACCOUNT`、`TX_TRADE_PASSWORD`。symbol 固定為 `TX00`。
- production recorder opt-in：`TX_TRADE_RUNTIME_PRESET=phase1_live_quote`
  與 `TX_TRADE_ENABLE_LIVE_QUOTE=1`；app 另外要求
  `TX_TRADE_ACCOUNT`、`TX_TRADE_PASSWORD`、
  `TX_TRADE_SKCOM_DLL_PATH`、`TX_TRADE_SYMBOLS`。可選設定為
  `TX_TRADE_RECORDING_DB_PATH`、`TX_TRADE_LIVE_READY_TIMEOUT_SECONDS`
  與 `TX_TRADE_LIVE_STOP_TIMEOUT_SECONDS`。
- integration test 不使用 `TX_TRADE_ENABLE_LIVE_QUOTE`；production app
  也不以 `TX_TRADE_RUN_LIVE_QUOTE_TEST` 授權 live。

`.env` 不會由目前的 integration test 或 app entry point 自動載入。帳密
與 DLL path 必須由核准的 secret store、無 history prompt，或專用的短
生命 shell 注入目前 process；不得使用包含真值的 literal assignment，
也不得把值寫入文件、command history、log 或 commit。下例假設必要值已
安全注入，並在結束時清除或還原所有觸及的環境變數：

```powershell
$names = @(
    "TX_TRADE_RUN_LIVE_QUOTE_TEST",
    "TX_TRADE_ACCOUNT",
    "TX_TRADE_PASSWORD",
    "TX_TRADE_SKCOM_DLL_PATH"
)
$saved = @{}
foreach ($name in $names) {
    $saved[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
}
try {
    if (-not $env:TX_TRADE_ACCOUNT -or -not $env:TX_TRADE_PASSWORD) { throw "SKCOM credentials are not configured" }
    if (-not (Test-Path -LiteralPath $env:TX_TRADE_SKCOM_DLL_PATH -PathType Leaf)) { throw "SKCOM DLL path is invalid" }
    $env:TX_TRADE_RUN_LIVE_QUOTE_TEST = "1"
    .\venv_tx_trade_fresh\Scripts\python.exe -m pytest -o addopts="" .\tests\integration\test_skcom_quote_live.py -v
} finally {
    foreach ($name in $names) {
        if ($null -eq $saved[$name]) {
            [Environment]::SetEnvironmentVariable($name, $null, "Process")
        } else {
            [Environment]::SetEnvironmentVariable($name, $saved[$name], "Process")
        }
    }
}
```

若同一短生命 shell 也執行 production recorder，須以相同方式清除或還原
`TX_TRADE_RUNTIME_PRESET`、`TX_TRADE_ENABLE_LIVE_QUOTE`、
`TX_TRADE_SYMBOLS`、`TX_TRADE_RECORDING_DB_PATH` 與 timeout 變數。

未設定 `TX_TRADE_RUN_LIVE_QUOTE_TEST=1` 時，真實 live case 預期 skip；整份
測試檔仍會執行兩個不連線的 safety tests。啟用後的驗收是登入、quote
monitor ready、`TX00` lookup、quote 與 tick subscription 成功；休市時
不要求收到 tick。此路徑建立 Center/Quote，以及只註冊
`OnReplyMessage` 的 announcement-only Reply；不建立 Order、不呼叫
`ConnectByID`、不註冊委託／成交 callback，也不送單。

live smoke 成功後，必須把日期、指令（遮蔽敏感值）、結果與上述四項證據
補入本 roadmap 及 `WORK_LOG.md`，再把 Phase 1 標為「已完成」。此 smoke
可與 Phase 2A 設計並行，但必須在 Phase 2 最終驗收前完成。

2026-07-27 首次實際執行結果為兩個 safety tests 通過、真實 case 在 login
code `2017` 失敗；尚未到達 quote ready、`TX00` lookup 或 subscription，
故 Phase 1 維持「進行中」。首次 failure rendering 亦發現 fixture tuple
會顯示 credential，測試已改用固定 redacted repr；當次 credential 必須
撤銷／更換後才可重試，文件不保存其真值。

2026-07-28 依隨附 SKCOM V2.13.58 文件確認，登入前必須建立 `SKReplyLib`
並註冊 `OnReplyMessage`，但不需連接回報主機。經使用者明確核准後加入此
最小安全例外；hard guards 仍禁止 `SKOrderLib`、
`SKReplyLib_ConnectByID`、`OnNewData`、`OnStrategyData` 與送單。
受限網路環境曾回傳 `1097`；在核准的正常網路環境執行後，整份 live
integration test 為 `8 passed`，登入、quote ready、`TX00` lookup、
quote/tick subscription 與 cleanup 均成功。

### 15.4 Phase 2 下一個安全切片

Phase 2A 第一個 slice 已進入實作與驗收：`ReplayRuntime` 提供
FASTEST/PACED、可中斷 clock、狀態機、exclusive cursor、pause/resume/stop
及 sanitized failure；SQLite gate 僅接受 complete、current-schema、
non-empty、integrity-valid session。第一個 slice 不實作或設計
PaperBroker；Phase 2B 的 abstraction、fill policy、fees 必須等 Phase 2A
contract 驗收通過後才設計。Phase 2A 全程不得載入 COM、建立
SKOrderLib/SKReplyLib、註冊 Order/Reply callback 或送單。

Phase 2A 第二個 slice 提供獨立的 `phase2_replay` 設定 parser、
composition root 與 `python -m tx_trade.app.phase2` CLI。它只接受既存
SQLite DB、明確 session UUID、FASTEST/PACED、speed 與 optional exclusive
cursor；stdout 只輸出 canonical JSON Lines。此入口不沿用 Phase 1
composition、不讀 live credential、不載入 COM。
# Phase 2B-4 status update (2026-07-28)

- Phase 2B-1 through 2B-3 are complete: paper order/state contracts,
  deterministic matching and execution policies, and paper positions.
- Phase 2B-4 now adds transactional market-plus-strategy decision batches, a
  declarative strategy coordinator, a strict `research_paper` configuration,
  a read-only SQLite composition root, and buffered versioned JSONL output.
- The paper broker's bounded in-memory event journal is the authoritative
  event source. A replay cursor is rejected for paper runs because durable
  broker checkpoint restoration does not exist yet.
- Durable paper checkpoints and a durable output outbox remain future work.
- Research paper mode remains isolated from SKCOM, live credentials,
  Center/Quote/Reply/Order creation, Reply connections, and live orders.
