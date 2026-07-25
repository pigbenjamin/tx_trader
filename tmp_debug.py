import comtypes.client
from ctypes import c_short, pointer

comtypes.client.GetModule(r'D:\Stock\program_trading\capital_api\CapitalAPI_2.13.58\元件\x64\SKCOM.dll')
import comtypes.gen.SKCOMLib as sk
q = comtypes.client.CreateObject(sk.SKQuoteLib, interface=sk.ISKQuoteLib)
page = c_short(0)
page_ptr = pointer(page)
print('method', q.SKQuoteLib_RequestStocks)
try:
    result = q.SKQuoteLib_RequestStocks(page_ptr, 'TX00')
    print('type', type(result), 'result', result)
except Exception as e:
    print('error', type(e), e)
print('page', page.value)
