import comtypes.client
comtypes.client.GetModule(r'SKCOM.dll')
import comtypes.gen.SKCOMLib as sk
import ctypes
# 畫視窗用物件
import tkinter as tk
import tkinter.ttk as ttk

from tkinter import messagebox
from tkinter import filedialog
# 引入設定檔 (Settings for Combobox)
import config

# 群益API元件導入Python code內用的物件宣告
m_pSKCenter = comtypes.client.CreateObject(sk.SKCenterLib,interface=sk.ISKCenterLib)
m_pSKReply = comtypes.client.CreateObject(sk.SKReplyLib,interface=sk.ISKReplyLib)
# SKOrderLib component (used by LOG upload)
m_pSKOrder = comtypes.client.CreateObject(sk.SKOrderLib, interface=sk.ISKOrderLib)
# SKQuoteLib component (國內報價)
m_pSKQuote = comtypes.client.CreateObject(sk.SKQuoteLib, interface=sk.ISKQuoteLib)



# UI
class SKCOMTester(tk.Frame):
    def __init__(self, master = None):
        tk.Frame.__init__(self, master)
        self.grid()
        self.createWidgets()
    def createWidgets(self):
######################################################################################################################################
        #這層放Widgets設定
        # button

        # buttonSKOrderLib_LogUpload
        self.buttonSKOrderLib_LogUpload = tk.Button(self)
        self.buttonSKOrderLib_LogUpload["text"] = "LOG打包"
        self.buttonSKOrderLib_LogUpload["command"] = self.buttonSKOrderLib_LogUpload_Click
        self.buttonSKOrderLib_LogUpload.grid(column = 0, row = 3)

######################################################################################################################################
        #richTextBox
        # richTextBoxMethodMessage
        self.richTextBoxMethodMessage = tk.Listbox(self, height=5)
        self.richTextBoxMethodMessage.grid(column = 0, row = 0, columnspan=5, sticky = tk.E + tk.W)

        global richTextBoxMethodMessage
        richTextBoxMethodMessage = self.richTextBoxMethodMessage

         # richTextBoxMessage
        self.richTextBoxMessage = tk.Listbox(self, height=5)
        self.richTextBoxMessage.grid(column = 0, row = 10, columnspan=5, sticky = tk.E + tk.W)

        global richTextBoxMessage
        richTextBoxMessage = self.richTextBoxMessage
        
        # textBoxUserID
        self.labelUserID = tk.Label(self)
        self.labelUserID["text"] = "UserID："
        self.labelUserID.grid(column=1,row=2)
            #輸入框
        self.textBoxUserID = tk.Entry(self)
        self.textBoxUserID.grid(column = 2, row = 2)

        # CustCertID
        self.labelCustCertID = tk.Label(self)
        self.labelCustCertID["text"] = "CustCertID:"
        self.labelCustCertID.grid(column=1, row=4)
        self.textBoxCustCertID = tk.Entry(self)
        self.textBoxCustCertID.grid(column=2, row=4)

        # textBoxPassword
        self.labelPassword = tk.Label(self)
        self.labelPassword["text"] = "Password："
        self.labelPassword.grid(column = 1, row = 3)
            #輸入框
        self.textBoxPassword = tk.Entry(self)
        self.textBoxPassword['show'] = '*'
        self.textBoxPassword.grid(column = 2, row = 3)
        # buttonSKCenterLib_Login
        self.buttonSKCenterLib_Login = tk.Button(self)
        self.buttonSKCenterLib_Login["text"] = "Login"
        self.buttonSKCenterLib_Login["command"] = self.buttonSKCenterLib_Login_Click
        self.buttonSKCenterLib_Login.grid(column = 2, row = 6)
        # buttonSKCenterLib_GenerateKeyCert
        self.buttonSKCenterLib_GenerateKeyCert = tk.Button(self)
        self.buttonSKCenterLib_GenerateKeyCert["text"] = "雙因子驗證KEY"
        self.buttonSKCenterLib_GenerateKeyCert["command"] = self.buttonSKCenterLib_GenerateKeyCert_Click
        self.buttonSKCenterLib_GenerateKeyCert.grid(column = 1, row = 6)
        # buttonSKCenterLib_SetLogPath
        self.buttonSKCenterLib_SetLogPath = tk.Button(self)
        self.buttonSKCenterLib_SetLogPath["text"] = "變更LOG路徑"
        self.buttonSKCenterLib_SetLogPath["command"] = self.buttonSKCenterLib_SetLogPath_Click
        self.buttonSKCenterLib_SetLogPath.grid(column = 0, row = 7)

        # buttonSKReplyLib_ConnectByID
        self.buttonSKReplyLib_ConnectByID = tk.Button(self)
        self.buttonSKReplyLib_ConnectByID["text"] = "Reply連線"
        self.buttonSKReplyLib_ConnectByID["command"] = self.buttonSKReplyLib_ConnectByID_Click
        self.buttonSKReplyLib_ConnectByID.grid(column = 1, row = 7)

        # buttonSKOrderLib_Initialize
        self.buttonSKOrderLib_Initialize = tk.Button(self)
        self.buttonSKOrderLib_Initialize["text"] = "Order初始化"
        self.buttonSKOrderLib_Initialize["command"] = self.buttonSKOrderLib_Initialize_Click
        self.buttonSKOrderLib_Initialize.grid(column = 2, row = 7)

        # buttonSKOrderLib_LoadOfCommodityGW
        self.buttonSKOrderLib_LoadOfCommodityGW = tk.Button(self)
        self.buttonSKOrderLib_LoadOfCommodityGW["text"] = "載入商品GW"
        self.buttonSKOrderLib_LoadOfCommodityGW["command"] = self.buttonSKOrderLib_LoadOfCommodityGW_Click
        self.buttonSKOrderLib_LoadOfCommodityGW.grid(column = 0, row = 8)

        # buttonSKOrderLib_InitialProxyByID
        self.buttonSKOrderLib_InitialProxyByID = tk.Button(self)
        self.buttonSKOrderLib_InitialProxyByID["text"] = "Proxy初始化"
        self.buttonSKOrderLib_InitialProxyByID["command"] = self.buttonSKOrderLib_InitialProxyByID_Click
        self.buttonSKOrderLib_InitialProxyByID.grid(column = 1, row = 8)

        # buttonSKOrderLib_GetLoginType
        self.buttonSKOrderLib_GetLoginType = tk.Button(self)
        self.buttonSKOrderLib_GetLoginType["text"] = "查登入類型"
        self.buttonSKOrderLib_GetLoginType["command"] = self.buttonSKOrderLib_GetLoginType_Click
        self.buttonSKOrderLib_GetLoginType.grid(column = 2, row = 8)

        # --- Order inputs (準備下單) - FUTUREPROXYORDER fields ---
        self.labelOrderSymbol = tk.Label(self)
        self.labelOrderSymbol["text"] = "Symbol(OrderID):"
        self.labelOrderSymbol.grid(column=0, row=9)
        self.textBoxOrderSymbol = tk.Entry(self)
        self.textBoxOrderSymbol.grid(column=1, row=9)

        self.labelOrderPrice = tk.Label(self)
        self.labelOrderPrice["text"] = "Price:"
        self.labelOrderPrice.grid(column=0, row=10)
        self.textBoxOrderPrice = tk.Entry(self)
        self.textBoxOrderPrice.grid(column=1, row=10)

        self.labelOrderQty = tk.Label(self)
        self.labelOrderQty["text"] = "Qty:"
        self.labelOrderQty.grid(column=0, row=11)
        self.textBoxOrderQty = tk.Entry(self)
        self.textBoxOrderQty.grid(column=1, row=11)

        self.labelOrderSide = tk.Label(self)
        self.labelOrderSide["text"] = "Side(BuySell):"
        self.labelOrderSide.grid(column=2, row=9)
        self.varOrderSide = tk.StringVar(value="B")
        self.radioBuy = tk.Radiobutton(self, text="Buy", variable=self.varOrderSide, value="B")
        self.radioSell = tk.Radiobutton(self, text="Sell", variable=self.varOrderSide, value="S")
        self.radioBuy.grid(column=3, row=9)
        self.radioSell.grid(column=4, row=9)

        # Row 12: OrderPriceType
        self.labelOrderPriceType = tk.Label(self)
        self.labelOrderPriceType["text"] = "PriceType:"
        self.labelOrderPriceType.grid(column=0, row=12)
        self.varOrderPriceType = tk.StringVar(value="0")
        self.comboOrderPriceType = ttk.Combobox(self, textvariable=self.varOrderPriceType, 
                                                values=["0", "1", "2", "3", "M", "P"], width=10)
        self.comboOrderPriceType.grid(column=1, row=12)

        # Row 12: OrderCond  
        self.labelOrderCond = tk.Label(self)
        self.labelOrderCond["text"] = "OrderCond:"
        self.labelOrderCond.grid(column=2, row=12)
        self.varOrderCond = tk.StringVar(value="0")
        self.comboOrderCond = ttk.Combobox(self, textvariable=self.varOrderCond,
                                          values=["0", "1", "2", "3", "4", "5"], width=10)
        self.comboOrderCond.grid(column=3, row=12)

        # Row 13: OrderOffset
        self.labelOrderOffset = tk.Label(self)
        self.labelOrderOffset["text"] = "Offset:"
        self.labelOrderOffset.grid(column=0, row=13)
        self.varOrderOffset = tk.StringVar(value="0")
        self.comboOrderOffset = ttk.Combobox(self, textvariable=self.varOrderOffset,
                                            values=["0", "1", "2"], width=10)
        self.comboOrderOffset.grid(column=1, row=13)

        # Row 13: TradeSession
        self.labelTradeSession = tk.Label(self)
        self.labelTradeSession["text"] = "TradeSession:"
        self.labelTradeSession.grid(column=2, row=13)
        self.varTradeSession = tk.StringVar(value="0")
        self.comboTradeSession = ttk.Combobox(self, textvariable=self.varTradeSession,
                                             values=["0", "1", "2"], width=10)
        self.comboTradeSession.grid(column=3, row=13)

        # Row 14: UID
        self.labelUID = tk.Label(self)
        self.labelUID["text"] = "UID:"
        self.labelUID.grid(column=0, row=14)
        self.textBoxUID = tk.Entry(self)
        self.textBoxUID.grid(column=1, row=14)

        # Row 14: BalanceType
        self.labelBalanceType = tk.Label(self)
        self.labelBalanceType["text"] = "BalanceType:"
        self.labelBalanceType.grid(column=2, row=14)
        self.varBalanceType = tk.StringVar(value="0")
        self.comboBalanceType = ttk.Combobox(self, textvariable=self.varBalanceType,
                                            values=["0", "1", "2", "3"], width=10)
        self.comboBalanceType.grid(column=3, row=14)

        # Preview / Send buttons
        self.buttonPrepareFutureOrder = tk.Button(self)
        self.buttonPrepareFutureOrder["text"] = "預覽期貨委託"
        self.buttonPrepareFutureOrder["command"] = self.buttonPrepareFutureOrder_Click
        self.buttonPrepareFutureOrder.grid(column=0, row=15)

        self.buttonSendFutureOrder = tk.Button(self)
        self.buttonSendFutureOrder["text"] = "送出期貨委託(Proxy)"
        self.buttonSendFutureOrder["command"] = self.buttonSendFutureOrder_Click
        self.buttonSendFutureOrder.grid(column=1, row=15)

######################################################################################################################################
        # SKQuoteLib (國內報價 - 期貨核心報價)
        self.labelQuoteSection = tk.Label(self)
        self.labelQuoteSection["text"] = "===== SKQuoteLib 國內報價(期貨) ====="
        self.labelQuoteSection.grid(column=0, row=17, columnspan=5)

        # 連線相關
        self.buttonSKQuoteLib_EnterMonitorLONG = tk.Button(self)
        self.buttonSKQuoteLib_EnterMonitorLONG["text"] = "連線報價主機"
        self.buttonSKQuoteLib_EnterMonitorLONG["command"] = self.buttonSKQuoteLib_EnterMonitorLONG_Click
        self.buttonSKQuoteLib_EnterMonitorLONG.grid(column=0, row=18)

        self.buttonSKQuoteLib_LeaveMonitor = tk.Button(self)
        self.buttonSKQuoteLib_LeaveMonitor["text"] = "斷線報價主機(ALL)"
        self.buttonSKQuoteLib_LeaveMonitor["command"] = self.buttonSKQuoteLib_LeaveMonitor_Click
        self.buttonSKQuoteLib_LeaveMonitor.grid(column=1, row=18)

        self.buttonSKQuoteLib_IsConnected = tk.Button(self)
        self.buttonSKQuoteLib_IsConnected["text"] = "檢查連線狀態"
        self.buttonSKQuoteLib_IsConnected["command"] = self.buttonSKQuoteLib_IsConnected_Click
        self.buttonSKQuoteLib_IsConnected.grid(column=2, row=18)

        self.buttonSKQuoteLib_GetQuoteStatus = tk.Button(self)
        self.buttonSKQuoteLib_GetQuoteStatus["text"] = "連線數資訊/限制"
        self.buttonSKQuoteLib_GetQuoteStatus["command"] = self.buttonSKQuoteLib_GetQuoteStatus_Click
        self.buttonSKQuoteLib_GetQuoteStatus.grid(column=3, row=18)

        self.buttonSKQuoteLib_RequestServerTime = tk.Button(self)
        self.buttonSKQuoteLib_RequestServerTime["text"] = "報價主機現在時間"
        self.buttonSKQuoteLib_RequestServerTime["command"] = self.buttonSKQuoteLib_RequestServerTime_Click
        self.buttonSKQuoteLib_RequestServerTime.grid(column=4, row=18)

        # 即時報價訂閱 (不支援盤中零股)
        self.labelQuotePageNo = tk.Label(self)
        self.labelQuotePageNo["text"] = "Page:"
        self.labelQuotePageNo.grid(column=0, row=19)
        self.textBoxQuotePageNo = tk.Entry(self)
        self.textBoxQuotePageNo.insert(0, "1")
        self.textBoxQuotePageNo.grid(column=1, row=19)

        self.labelQuoteStockNos = tk.Label(self)
        self.labelQuoteStockNos["text"] = "商品代號(可用,分隔多檔,如TX00):"
        self.labelQuoteStockNos.grid(column=2, row=19)
        self.textBoxQuoteStockNos = tk.Entry(self)
        self.textBoxQuoteStockNos.grid(column=3, row=19)

        self.buttonSKQuoteLib_RequestStocks = tk.Button(self)
        self.buttonSKQuoteLib_RequestStocks["text"] = "訂閱報價"
        self.buttonSKQuoteLib_RequestStocks["command"] = self.buttonSKQuoteLib_RequestStocks_Click
        self.buttonSKQuoteLib_RequestStocks.grid(column=0, row=20)

        self.buttonSKQuoteLib_CancelRequestStocks = tk.Button(self)
        self.buttonSKQuoteLib_CancelRequestStocks["text"] = "取消訂閱報價"
        self.buttonSKQuoteLib_CancelRequestStocks["command"] = self.buttonSKQuoteLib_CancelRequestStocks_Click
        self.buttonSKQuoteLib_CancelRequestStocks.grid(column=1, row=20)

        # Tick 及五檔訂閱 (一個Page僅能索取一檔，不支援盤中零股)
        self.labelTickPageNo = tk.Label(self)
        self.labelTickPageNo["text"] = "Page(從0開始):"
        self.labelTickPageNo.grid(column=0, row=21)
        self.textBoxTickPageNo = tk.Entry(self)
        self.textBoxTickPageNo.insert(0, "0")
        self.textBoxTickPageNo.grid(column=1, row=21)

        self.labelTickStockNo = tk.Label(self)
        self.labelTickStockNo["text"] = "商品代號(僅1檔,如TX00):"
        self.labelTickStockNo.grid(column=2, row=21)
        self.textBoxTickStockNo = tk.Entry(self)
        self.textBoxTickStockNo.grid(column=3, row=21)

        self.buttonSKQuoteLib_RequestTicks = tk.Button(self)
        self.buttonSKQuoteLib_RequestTicks["text"] = "訂閱Tick及五檔"
        self.buttonSKQuoteLib_RequestTicks["command"] = self.buttonSKQuoteLib_RequestTicks_Click
        self.buttonSKQuoteLib_RequestTicks.grid(column=0, row=22)

        self.buttonSKQuoteLib_CancelRequestTicks = tk.Button(self)
        self.buttonSKQuoteLib_CancelRequestTicks["text"] = "取消訂閱Tick及五檔"
        self.buttonSKQuoteLib_CancelRequestTicks["command"] = self.buttonSKQuoteLib_CancelRequestTicks_Click
        self.buttonSKQuoteLib_CancelRequestTicks.grid(column=1, row=22)

        # 手動查詢 Tick / 五檔 (index來自OnNotifyQuoteLONG/OnNotifyTicksLONG/OnNotifyBest5LONG事件回傳的nIndex)
        self.labelGetTickMarketNo = tk.Label(self)
        self.labelGetTickMarketNo["text"] = "市場別(期貨=2):"
        self.labelGetTickMarketNo.grid(column=0, row=23)
        self.textBoxGetTickMarketNo = tk.Entry(self)
        self.textBoxGetTickMarketNo.insert(0, "2")
        self.textBoxGetTickMarketNo.grid(column=1, row=23)

        self.labelGetTickIndex = tk.Label(self)
        self.labelGetTickIndex["text"] = "Index:"
        self.labelGetTickIndex.grid(column=2, row=23)
        self.textBoxGetTickIndex = tk.Entry(self)
        self.textBoxGetTickIndex.grid(column=3, row=23)

        self.labelGetTickPtr = tk.Label(self)
        self.labelGetTickPtr["text"] = "Ptr(Tick用,從0開始):"
        self.labelGetTickPtr.grid(column=4, row=23)
        self.textBoxGetTickPtr = tk.Entry(self)
        self.textBoxGetTickPtr.insert(0, "0")
        self.textBoxGetTickPtr.grid(column=5, row=23)

        self.buttonSKQuoteLib_GetTickLONG = tk.Button(self)
        self.buttonSKQuoteLib_GetTickLONG["text"] = "取得Tick(手動)"
        self.buttonSKQuoteLib_GetTickLONG["command"] = self.buttonSKQuoteLib_GetTickLONG_Click
        self.buttonSKQuoteLib_GetTickLONG.grid(column=0, row=24)

        self.buttonSKQuoteLib_GetBest5LONG = tk.Button(self)
        self.buttonSKQuoteLib_GetBest5LONG["text"] = "取得五檔(手動)"
        self.buttonSKQuoteLib_GetBest5LONG["command"] = self.buttonSKQuoteLib_GetBest5LONG_Click
        self.buttonSKQuoteLib_GetBest5LONG.grid(column=1, row=24)

        # 期貨交易資訊 (需簽署期貨API下單同意書)
        self.labelFutureInfoPageNo = tk.Label(self)
        self.labelFutureInfoPageNo["text"] = "Page:"
        self.labelFutureInfoPageNo.grid(column=0, row=25)
        self.textBoxFutureInfoPageNo = tk.Entry(self)
        self.textBoxFutureInfoPageNo.insert(0, "1")
        self.textBoxFutureInfoPageNo.grid(column=1, row=25)

        self.labelFutureInfoStockNo = tk.Label(self)
        self.labelFutureInfoStockNo["text"] = "商品代號(僅1檔,如TX00):"
        self.labelFutureInfoStockNo.grid(column=2, row=25)
        self.textBoxFutureInfoStockNo = tk.Entry(self)
        self.textBoxFutureInfoStockNo.grid(column=3, row=25)

        self.buttonSKQuoteLib_RequestFutureTradeInfo = tk.Button(self)
        self.buttonSKQuoteLib_RequestFutureTradeInfo["text"] = "訂閱期貨交易資訊"
        self.buttonSKQuoteLib_RequestFutureTradeInfo["command"] = self.buttonSKQuoteLib_RequestFutureTradeInfo_Click
        self.buttonSKQuoteLib_RequestFutureTradeInfo.grid(column=4, row=25)
######################################################################################################################################

    # buttonSKCenterLib_Login
    def buttonSKCenterLib_Login_Click(self):
        nCode = m_pSKCenter.SKCenterLib_Login(self.textBoxUserID.get(),self.textBoxPassword.get())

        msg = "【SKCenterLib_Login】" + m_pSKCenter.SKCenterLib_GetReturnCodeMessage(nCode)
        richTextBoxMethodMessage.insert('end',  msg + "\n")
        richTextBoxMethodMessage.see('end')

    # buttonSKCenterLib_GenerateKeyCert
    def buttonSKCenterLib_GenerateKeyCert_Click(self):
        # 傳入 UserID 與已安裝之 CustCertID（憑證 ID）來產生雙因子驗證 KEY
        # 使用前請確保已在系統中安裝並選取正確的憑證
        nCode = m_pSKCenter.SKCenterLib_GenerateKeyCert(self.textBoxUserID.get(), self.textBoxCustCertID.get())

        msg = "[SKCenterLib_GenerateKeyCert] " + m_pSKCenter.SKCenterLib_GetReturnCodeMessage(nCode)
        richTextBoxMethodMessage.insert('end', msg + "\n")
        richTextBoxMethodMessage.see('end')

    # buttonSKCenterLib_SetLogPath
    def buttonSKCenterLib_SetLogPath_Click(self):
        def select_folder():
            bstrPath = ""
            folder_selected = filedialog.askdirectory(title="選擇資料夾")
            if folder_selected:
                bstrPath = folder_selected
                messagebox.showinfo("選擇的資料夾", "選擇的資料夾: " + bstrPath)
            return bstrPath

        bstrPath = select_folder()
        if not bstrPath:
            messagebox.showwarning("未選擇資料夾!", "未選擇資料夾!")
        else:
            nCode = m_pSKCenter.SKCenterLib_SetLogPath(bstrPath)
            msg = "[SKCenterLib_SetLogPath] " + m_pSKCenter.SKCenterLib_GetReturnCodeMessage(nCode)
            richTextBoxMethodMessage.insert('end', msg + "\n")
            richTextBoxMethodMessage.see('end')

    # buttonSKReplyLib_ConnectByID
    def buttonSKReplyLib_ConnectByID_Click(self):
        nCode = m_pSKReply.SKReplyLib_ConnectByID(self.textBoxUserID.get())
        msg = "【SKReplyLib_ConnectByID】" + m_pSKCenter.SKCenterLib_GetReturnCodeMessage(nCode)
        richTextBoxMethodMessage.insert('end', msg + "\n")
        richTextBoxMethodMessage.see('end')

    # buttonSKOrderLib_Initialize
    def buttonSKOrderLib_Initialize_Click(self):
        nCode = m_pSKOrder.SKOrderLib_Initialize()
        msg = "【SKOrderLib_Initialize】" + m_pSKCenter.SKCenterLib_GetReturnCodeMessage(nCode)
        richTextBoxMethodMessage.insert('end', msg + "\n")
        richTextBoxMethodMessage.see('end')

    # buttonSKOrderLib_LoadOfCommodityGW
    def buttonSKOrderLib_LoadOfCommodityGW_Click(self):
        nCode = m_pSKOrder.SKOrderLib_LoadOfCommodityGW(self.textBoxUserID.get())
        msg = "【SKOrderLib_LoadOfCommodityGW】" + m_pSKCenter.SKCenterLib_GetReturnCodeMessage(nCode)
        richTextBoxMethodMessage.insert('end', msg + "\n")
        richTextBoxMethodMessage.see('end')

    # buttonSKOrderLib_InitialProxyByID
    def buttonSKOrderLib_InitialProxyByID_Click(self):
        nCode = m_pSKOrder.SKOrderLib_InitialProxyByID(self.textBoxUserID.get())
        msg = "【SKOrderLib_InitialProxyByID】" + m_pSKCenter.SKCenterLib_GetReturnCodeMessage(nCode)
        richTextBoxMethodMessage.insert('end', msg + "\n")
        richTextBoxMethodMessage.see('end')

    # buttonSKOrderLib_GetLoginType
    def buttonSKOrderLib_GetLoginType_Click(self):
        login_type = m_pSKOrder.SKOrderLib_GetLoginType(self.textBoxUserID.get())
        msg = "【SKOrderLib_GetLoginType】" + str(login_type)
        richTextBoxMethodMessage.insert('end', msg + "\n")
        richTextBoxMethodMessage.see('end')

    # Prepare FUTURE order preview (construct struct or dict)
    def buttonPrepareFutureOrder_Click(self):
        symbol = self.textBoxOrderSymbol.get()
        price = self.textBoxOrderPrice.get()
        qty = self.textBoxOrderQty.get()
        side = self.varOrderSide.get()
        price_type = self.varOrderPriceType.get()
        order_cond = self.varOrderCond.get()
        order_offset = self.varOrderOffset.get()
        trade_session = self.varTradeSession.get()
        uid = self.textBoxUID.get()
        balance_type = self.varBalanceType.get()

        try:
            # try to build COM struct if available in generated module
            order_obj = None
            field_map = {
                'OrderID': symbol,
                'Price': float(price) if price else 0.0,
                'Qty': int(qty) if qty else 0,
                'BuySell': side,
                'OrderPriceType': price_type,
                'OrderCond': order_cond,
                'OrderOffset': order_offset,
                'TradeSession': trade_session,
                'UID': uid,
                'BalanceType': balance_type,
            }
            
            try:
                order_obj = sk.FUTUREPROXYORDER()
                # best-effort assign all fields if they exist
                for attr, val in field_map.items():
                    if hasattr(order_obj, attr):
                        try:
                            setattr(order_obj, attr, val)
                        except Exception:
                            pass
            except Exception:
                order_obj = None

            if order_obj is not None:
                richTextBoxMethodMessage.insert('end', f"Prepared FUTUREPROXYORDER (COM struct) for {symbol}\n")
                richTextBoxMethodMessage.insert('end', str(order_obj) + "\n")
                # Also show field values
                richTextBoxMethodMessage.insert('end', "Fields assigned:\n")
                for k, v in field_map.items():
                    richTextBoxMethodMessage.insert('end', f"  {k}={v}\n")
            else:
                richTextBoxMethodMessage.insert('end', f"Prepared FUTURE order payload: {field_map}\n")

        except Exception as e:
            richTextBoxMethodMessage.insert('end', f"Prepare order error: {e}\n")
        richTextBoxMethodMessage.see('end')

    # Attempt to send FUTURE order via Proxy (best-effort with graceful fallback)
    def buttonSendFutureOrder_Click(self):
        symbol = self.textBoxOrderSymbol.get()
        price = self.textBoxOrderPrice.get()
        qty = self.textBoxOrderQty.get()
        side = self.varOrderSide.get()
        price_type = self.varOrderPriceType.get()
        order_cond = self.varOrderCond.get()
        order_offset = self.varOrderOffset.get()
        trade_session = self.varTradeSession.get()
        uid = self.textBoxUID.get()
        balance_type = self.varBalanceType.get()

        # Build a complete payload and try the documented method
        try:
            # try COM struct
            try:
                p = sk.FUTUREPROXYORDER()
                field_map = {
                    'OrderID': symbol,
                    'Price': float(price) if price else 0.0,
                    'Qty': int(qty) if qty else 0,
                    'BuySell': side,
                    'OrderPriceType': price_type,
                    'OrderCond': order_cond,
                    'OrderOffset': order_offset,
                    'TradeSession': trade_session,
                    'UID': uid,
                    'BalanceType': balance_type,
                }
                
                for attr, val in field_map.items():
                    if hasattr(p, attr):
                        try:
                            setattr(p, attr, val)
                        except Exception as e:
                            richTextBoxMethodMessage.insert('end', f"  Warning: Failed to set {attr}: {e}\n")
                order_obj = p
            except Exception:
                order_obj = None

            # call SendFutureProxyOrderCLR(login_id, order_obj, out_message)
            try:
                if order_obj is not None:
                    res = m_pSKOrder.SendFutureProxyOrderCLR(self.textBoxUserID.get(), order_obj)
                else:
                    # fallback: try to call with placeholders (may raise)
                    res = m_pSKOrder.SendFutureProxyOrderCLR(self.textBoxUserID.get(), None)
                richTextBoxMethodMessage.insert('end', f"【SendFutureProxyOrderCLR】Result: {res}\n")
            except AttributeError:
                richTextBoxMethodMessage.insert('end', "API SendFutureProxyOrderCLR not available on this SKOrderLib object.\n")
            except Exception as e:
                richTextBoxMethodMessage.insert('end', f"Send error: {e}\n")

        except Exception as e:
            richTextBoxMethodMessage.insert('end', f"Unhandled send error: {e}\n")
        richTextBoxMethodMessage.see('end')

######################################################################################################################################
    # SKQuoteLib (國內報價 - 期貨核心報價) 按鈕事件

    # buttonSKQuoteLib_EnterMonitorLONG
    def buttonSKQuoteLib_EnterMonitorLONG_Click(self):
        # 與報價伺服器建立連線(含盤中零股市場商品)
        nCode = m_pSKQuote.SKQuoteLib_EnterMonitorLONG()

        msg = "【SKQuoteLib_EnterMonitorLONG】" + m_pSKCenter.SKCenterLib_GetReturnCodeMessage(nCode)
        richTextBoxMethodMessage.insert('end', msg + "\n")
        richTextBoxMethodMessage.see('end')

    # buttonSKQuoteLib_LeaveMonitor
    def buttonSKQuoteLib_LeaveMonitor_Click(self):
        # 中斷所有Solace伺服器連線(報價與回報)
        nCode = m_pSKQuote.SKQuoteLib_LeaveMonitor()

        msg = "【SKQuoteLib_LeaveMonitor】" + m_pSKCenter.SKCenterLib_GetReturnCodeMessage(nCode)
        richTextBoxMethodMessage.insert('end', msg + "\n")
        richTextBoxMethodMessage.see('end')

    # buttonSKQuoteLib_IsConnected
    def buttonSKQuoteLib_IsConnected_Click(self):
        # 檢查目前報價的連線狀態
        nCode = m_pSKQuote.SKQuoteLib_IsConnected()

        if nCode == 0:
            msg = "斷線"
        elif nCode == 1:
            msg = "連線中"
        elif nCode == 2:
            msg = "下載中"
        else:
            msg = "出錯啦"

        msg = "【SKQuoteLib_IsConnected】" + msg
        richTextBoxMethodMessage.insert('end', msg + "\n")
        richTextBoxMethodMessage.see('end')

    # buttonSKQuoteLib_GetQuoteStatus
    def buttonSKQuoteLib_GetQuoteStatus_Click(self):
        pnConnectionCount = 0
        pbIsOutLimit = False

        # 查詢報價連線狀態(是否超過報價連線限制,連線數資訊)
        pnConnectionCount, pbIsOutLimit, nCode = m_pSKQuote.SKQuoteLib_GetQuoteStatus(pnConnectionCount, pbIsOutLimit)

        msg = ("【SKQuoteLib_GetQuoteStatus】" + m_pSKCenter.SKCenterLib_GetReturnCodeMessage(nCode) +
               " 連線數:" + str(pnConnectionCount) + " 超過限制:" + str(pbIsOutLimit))
        richTextBoxMethodMessage.insert('end', msg + "\n")
        richTextBoxMethodMessage.see('end')

    # buttonSKQuoteLib_RequestServerTime
    def buttonSKQuoteLib_RequestServerTime_Click(self):
        # 要求報價主機傳送目前時間
        nCode = m_pSKQuote.SKQuoteLib_RequestServerTime()

        msg = "【SKQuoteLib_RequestServerTime】" + m_pSKCenter.SKCenterLib_GetReturnCodeMessage(nCode)
        richTextBoxMethodMessage.insert('end', msg + "\n")
        richTextBoxMethodMessage.see('end')

    # buttonSKQuoteLib_RequestStocks
    def buttonSKQuoteLib_RequestStocks_Click(self):
        psPageNo = int(self.textBoxQuotePageNo.get())
        # 訂閱指定商品即時報價(不支援盤中零股)
        psPageNo, nCode = m_pSKQuote.SKQuoteLib_RequestStocks(psPageNo, self.textBoxQuoteStockNos.get())

        msg = "【SKQuoteLib_RequestStocks】" + m_pSKCenter.SKCenterLib_GetReturnCodeMessage(nCode)
        richTextBoxMethodMessage.insert('end', msg + "\n")
        richTextBoxMethodMessage.see('end')

    # buttonSKQuoteLib_CancelRequestStocks
    def buttonSKQuoteLib_CancelRequestStocks_Click(self):
        # 取消訂閱SKQuoteLib_RequestStocks的報價通知，並停止更新商品報價
        nCode = m_pSKQuote.SKQuoteLib_CancelRequestStocks(self.textBoxQuoteStockNos.get())

        msg = "【SKQuoteLib_CancelRequestStocks】" + m_pSKCenter.SKCenterLib_GetReturnCodeMessage(nCode)
        richTextBoxMethodMessage.insert('end', msg + "\n")
        richTextBoxMethodMessage.see('end')

    # buttonSKQuoteLib_RequestTicks
    def buttonSKQuoteLib_RequestTicks_Click(self):
        psPageNo = int(self.textBoxTickPageNo.get())
        # 訂閱要求傳送成交明細以及五檔(不支援盤中零股，一個Page僅能索取一檔)
        psPageNo, nCode = m_pSKQuote.SKQuoteLib_RequestTicks(psPageNo, self.textBoxTickStockNo.get())

        msg = "【SKQuoteLib_RequestTicks】" + m_pSKCenter.SKCenterLib_GetReturnCodeMessage(nCode)
        richTextBoxMethodMessage.insert('end', msg + "\n")
        richTextBoxMethodMessage.see('end')

    # buttonSKQuoteLib_CancelRequestTicks
    def buttonSKQuoteLib_CancelRequestTicks_Click(self):
        # 取消訂閱RequestTicks的成交明細及五檔
        nCode = m_pSKQuote.SKQuoteLib_CancelRequestTicks(self.textBoxTickStockNo.get())

        msg = "【SKQuoteLib_CancelRequestTicks】" + m_pSKCenter.SKCenterLib_GetReturnCodeMessage(nCode)
        richTextBoxMethodMessage.insert('end', msg + "\n")
        richTextBoxMethodMessage.see('end')

    # buttonSKQuoteLib_GetTickLONG
    def buttonSKQuoteLib_GetTickLONG_Click(self):
        pSKTick = sk.SKTICK()
        sMarketNo = int(self.textBoxGetTickMarketNo.get())
        nIndex = int(self.textBoxGetTickIndex.get())
        nPtr = int(self.textBoxGetTickPtr.get())

        # (需先訂閱即時成交明細RequestTicks)取得指定第幾筆成交明細資訊
        pSKTick, nCode = m_pSKQuote.SKQuoteLib_GetTickLONG(sMarketNo, nIndex, nPtr, pSKTick)

        msg = "【SKQuoteLib_GetTickLONG】" + m_pSKCenter.SKCenterLib_GetReturnCodeMessage(nCode)
        richTextBoxMethodMessage.insert('end', msg + "\n")
        richTextBoxMethodMessage.see('end')

        msg = ("Ptr:" + str(pSKTick.nPtr) +
               " 時間:" + str(pSKTick.nTimehms) +
               " 買價:" + str(pSKTick.nBid / 100.0) +
               " 賣價:" + str(pSKTick.nAsk / 100.0) +
               " 成交價:" + str(pSKTick.nClose / 100.0) +
               " 量:" + str(pSKTick.nQty))
        richTextBoxMessage.insert('end', msg + "\n")
        richTextBoxMessage.see('end')

    # buttonSKQuoteLib_GetBest5LONG
    def buttonSKQuoteLib_GetBest5LONG_Click(self):
        pSKBest5 = sk.SKBEST5()
        sMarketNo = int(self.textBoxGetTickMarketNo.get())
        nIndex = int(self.textBoxGetTickIndex.get())

        # (需先訂閱最佳五檔RequestTicks)取得最佳五檔價格資訊
        pSKBest5, nCode = m_pSKQuote.SKQuoteLib_GetBest5LONG(sMarketNo, nIndex, pSKBest5)

        msg = "【SKQuoteLib_GetBest5LONG】" + m_pSKCenter.SKCenterLib_GetReturnCodeMessage(nCode)
        richTextBoxMethodMessage.insert('end', msg + "\n")
        richTextBoxMethodMessage.see('end')

        msg = ("買一:" + str(pSKBest5.nBid1 / 100.0) + "(" + str(pSKBest5.nBidQty1) + ")" +
               " 賣一:" + str(pSKBest5.nAsk1 / 100.0) + "(" + str(pSKBest5.nAskQty1) + ")")
        richTextBoxMessage.insert('end', msg + "\n")
        richTextBoxMessage.see('end')

    # buttonSKQuoteLib_RequestFutureTradeInfo
    def buttonSKQuoteLib_RequestFutureTradeInfo_Click(self):
        psPageNo = ctypes.c_short(int(self.textBoxFutureInfoPageNo.get()))
        # 取得報價函式庫註冊接收期貨商品的交易資訊(需簽署期貨API下單同意書，否則錯誤代碼3031)
        nCode = m_pSKQuote.SKQuoteLib_RequestFutureTradeInfo(psPageNo, self.textBoxFutureInfoStockNo.get())

        msg = "【SKQuoteLib_RequestFutureTradeInfo】" + m_pSKCenter.SKCenterLib_GetReturnCodeMessage(nCode)
        richTextBoxMethodMessage.insert('end', msg + "\n")
        richTextBoxMethodMessage.see('end')

######################################################################################################################################
    #這層放Widgets觸發的command
    def buttonSKOrderLib_LogUpload_Click(self):
        nCode = m_pSKOrder.SKOrderLib_LogUpload()

        msg = "【SKOrderLib_LogUpload】" + m_pSKCenter.SKCenterLib_GetReturnCodeMessage(nCode)
        richTextBoxMethodMessage.insert('end',  msg + "\n")
        richTextBoxMethodMessage.see('end')

######################################################################################################################################

# ReplyLib事件
class SKReplyLibEvent():
    def OnReplyMessage(self, bstrUserID, bstrMessages):
        nConfirmCode = -1
        msg = "【註冊公告OnReplyMessage】" + bstrUserID + "_" + bstrMessages;
        richTextBoxMessage.insert('end', msg + "\n")
        richTextBoxMessage.see('end')
        return nConfirmCode
    
    def OnReplyMessage(self, bstrUserID, bstrMessages):
            nConfirmCode = -1
            msg = "【註冊公告OnReplyMessage】" + bstrUserID + "_" + bstrMessages;
            richTextBoxMessage.insert('end', msg + "\n")
            richTextBoxMessage.see('end')
            return nConfirmCode
        
        
SKReplyEvent = SKReplyLibEvent();
SKReplyLibEventHandler = comtypes.client.GetEvents(m_pSKReply, SKReplyEvent);


######################################################################################################################################

# QuoteLib事件 (國內報價 - 期貨核心報價)
class SKQuoteLibEvent():
    def OnConnection(self, nKind, nCode):
        msg = "【OnConnection】" + m_pSKCenter.SKCenterLib_GetReturnCodeMessage(nKind) + "_" + m_pSKCenter.SKCenterLib_GetReturnCodeMessage(nCode)
        richTextBoxMessage.insert('end', msg + "\n")
        richTextBoxMessage.see('end')

    def OnNotifyServerTime(self, sHour, sMinute, sSecond, nTotal):
        msg = "【OnNotifyServerTime】" + str(sHour) + ":" + str(sMinute) + ":" + str(sSecond) + " 總秒數:" + str(nTotal)
        richTextBoxMessage.insert('end', msg + "\n")
        richTextBoxMessage.see('end')

    def OnNotifyQuoteLONG(self, sMarketNo, nIndex):
        pSKStock = sk.SKSTOCKLONG()
        pSKStock, nCode = m_pSKQuote.SKQuoteLib_GetStockByIndexLONG(sMarketNo, nIndex, pSKStock)

        if pSKStock.nBid == m_pSKQuote.SKQuoteLib_GetMarketPriceTS():
            nBidValue = "市價"
        else:
            nBidValue = pSKStock.nBid / 100.0

        if pSKStock.nAsk == m_pSKQuote.SKQuoteLib_GetMarketPriceTS():
            nAskValue = "市價"
        else:
            nAskValue = pSKStock.nAsk / 100.0

        msg = ("【OnNotifyQuoteLONG】" +
               " 市場別" + str(sMarketNo) +
               " Index" + str(nIndex) +
               " 商品代碼" + str(pSKStock.bstrStockNo) +
               " 名稱" + str(pSKStock.bstrStockName) +
               " 開盤價" + str(pSKStock.nOpen / 100.0) +
               " 成交價" + str(pSKStock.nClose / 100.0) +
               " 最高" + str(pSKStock.nHigh / 100.0) +
               " 最低" + str(pSKStock.nLow / 100.0) +
               " 買價" + str(nBidValue) +
               " 買量" + str(pSKStock.nBc) +
               " 賣價" + str(nAskValue) +
               " 賣量" + str(pSKStock.nAc) +
               " 總量" + str(pSKStock.nTQty) +
               " 昨收" + str(pSKStock.nRef / 100.0))
        richTextBoxMessage.insert('end', msg + "\n")
        richTextBoxMessage.see('end')

    def OnNotifyTicksLONG(self, sMarketNo, nIndex, nPtr, nDate, nTimehms, nTimemillismicros, nBid, nAsk, nClose, nQty, nSimulate):
        msg = ("【OnNotifyTicksLONG】" +
               " 日期:" + str(nDate) +
               " 時間:" + str(nTimehms) +
               " 買價:" + str(nBid / 100.0) +
               " 賣價:" + str(nAsk / 100.0) +
               " 成交價:" + str(nClose / 100.0) +
               " 成交量:" + str(nQty))
        richTextBoxMessage.insert('end', msg + "\n")
        richTextBoxMessage.see('end')

    def OnNotifyHistoryTicksLONG(self, sMarketNo, nIndex, nPtr, nDate, nTimehms, nTimemillismicros, nBid, nAsk, nClose, nQty, nSimulate):
        msg = ("【OnNotifyHistoryTicksLONG(回補)】" +
               " 日期:" + str(nDate) +
               " 時間:" + str(nTimehms) +
               " 買價:" + str(nBid / 100.0) +
               " 賣價:" + str(nAsk / 100.0) +
               " 成交價:" + str(nClose / 100.0) +
               " 成交量:" + str(nQty))
        richTextBoxMessage.insert('end', msg + "\n")
        richTextBoxMessage.see('end')

    def OnNotifyBest5LONG(self, sMarketNo, nStockidx, nBestBid1, nBestBidQty1, nBestBid2, nBestBidQty2,
                           nBestBid3, nBestBidQty3, nBestBid4, nBestBidQty4, nBestBid5, nBestBidQty5,
                           nExtendBid, nExtendBidQty, nBestAsk1, nBestAskQty1, nBestAsk2, nBestAskQty2,
                           nBestAsk3, nBestAskQty3, nBestAsk4, nBestAskQty4, nBestAsk5, nBestAskQty5,
                           nExtendAsk, nExtendAskQty, nSimulate):
        msg = ("【OnNotifyBest5LONG】" +
               " 買一:" + str(nBestBid1 / 100.0) + "(" + str(nBestBidQty1) + ")" +
               " 賣一:" + str(nBestAsk1 / 100.0) + "(" + str(nBestAskQty1) + ")" +
               " 買二:" + str(nBestBid2 / 100.0) + "(" + str(nBestBidQty2) + ")" +
               " 賣二:" + str(nBestAsk2 / 100.0) + "(" + str(nBestAskQty2) + ")" +
               " 買三:" + str(nBestBid3 / 100.0) + "(" + str(nBestBidQty3) + ")" +
               " 賣三:" + str(nBestAsk3 / 100.0) + "(" + str(nBestAskQty3) + ")" +
               " 買四:" + str(nBestBid4 / 100.0) + "(" + str(nBestBidQty4) + ")" +
               " 賣四:" + str(nBestAsk4 / 100.0) + "(" + str(nBestAskQty4) + ")" +
               " 買五:" + str(nBestBid5 / 100.0) + "(" + str(nBestBidQty5) + ")" +
               " 賣五:" + str(nBestAsk5 / 100.0) + "(" + str(nBestAskQty5) + ")")
        richTextBoxMessage.insert('end', msg + "\n")
        richTextBoxMessage.see('end')

    def OnNotifyFutureTradeInfoLONG(self, bstrStockNo, sMarketNo, nStockidx, nBuyTotalCount, nSellTotalCount,
                                     nBuyTotalQty, nSellTotalQty, nBuyDealTotalCount, nSellDealTotalCount):
        msg = ("【OnNotifyFutureTradeInfoLONG】" + str(bstrStockNo) +
               " 委買筆數:" + str(nBuyTotalCount) +
               " 委賣筆數:" + str(nSellTotalCount) +
               " 委買口數:" + str(nBuyTotalQty) +
               " 委賣口數:" + str(nSellTotalQty) +
               " 成交買筆:" + str(nBuyDealTotalCount) +
               " 成交賣筆:" + str(nSellDealTotalCount))
        richTextBoxMessage.insert('end', msg + "\n")
        richTextBoxMessage.see('end')


SKQuoteEvent = SKQuoteLibEvent()
SKQuoteLibEventHandler = comtypes.client.GetEvents(m_pSKQuote, SKQuoteEvent)



#開啟Tk視窗
if __name__ == '__main__':
    root = tk.Tk()
    root.title("SKCOMTester")
    
    SKCOMTester(master = root)
    root.mainloop()
