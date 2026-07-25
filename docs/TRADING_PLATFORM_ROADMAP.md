# 程式交易平台建置藍圖

> 文件狀態：初版  
> 建立日期：2026-07-25  
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

- 載入 `SKCOM.dll`。
- 登入 Capital API。
- 進入報價監控。
- 訂閱商品 Quote 與 Tick。
- 接收報價、Tick、伺服器時間及連線事件。
- OrderLib 初始化與商品資料載入。
- ReplyLib 連線入口。

目前主要缺口：

- `QuoteClient` 同時負責連線、行情、委託與回報，責任過多。
- 尚未實作完整的新單、改單、刪單。
- 尚未解析完整委託及成交回報。
- 沒有委託狀態機、冪等控制及重啟對帳。
- 沒有部位與損益管理。
- 沒有集中式風控。
- 行情事件持續存入 list，長時間運行有記憶體風險。
- API 初始化失敗時可能進入 offline 並回報成功，不適合實盤。
- 自動化測試只涵蓋少量回傳格式。
- Python 虛擬環境目前無法正常啟動，環境不可重現。
- README 與部分原始碼註解有文字編碼異常。

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

### Phase 2：PaperBroker 與回放

工作內容：

- 定義券商抽象介面。
- 建立 PaperBroker。
- 建立歷史 Tick 回放。
- 實作基本模擬成交規則、滑價與手續費。
- 讓策略不修改程式即可切換 broker。

驗收條件：

- 同一策略可在 replay、paper 模式執行。
- 固定資料重播可得到可重現結果。
- paper 模式完全不會呼叫真實下單 API。

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
| Phase 0 專案基線 | 待開始 |  |  | 優先處理 |
| Phase 1 穩定行情 | 待開始 |  |  |  |
| Phase 2 Paper/Replay | 待開始 |  |  |  |
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

## 14. 下一個工作項目

下一步執行 Phase 0：

1. 盤點並鎖定 Python 與套件版本。
2. 重建可用虛擬環境。
3. 修正 live/offline 行為。
4. 統一原始碼與文件編碼。
5. 建立設定模型、啟動前檢查及基本測試。
6. 更新 README，加入環境建立和安全啟動方式。
