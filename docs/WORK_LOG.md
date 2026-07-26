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
