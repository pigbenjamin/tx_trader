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
