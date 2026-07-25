import zipfile
import os
from xml.etree import ElementTree as ET

p = r'D:\Stock\program_trading\CapitalAPI_2.13.58_PythonExample\策略王COM元件使用說明_V2.13.58.docx'
print('exists', os.path.exists(p))
if not os.path.exists(p):
    raise SystemExit(0)

with zipfile.ZipFile(p) as z:
    doc_name = [n for n in z.namelist() if n.endswith('document.xml')][0]
    xml = z.read(doc_name)

root = ET.fromstring(xml)
ns = {'w': 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'}
paras = []
for para in root.findall('.//w:p', ns):
    texts = []
    for t in para.findall('.//w:t', ns):
        texts.append(t.text or '')
    s = ''.join(texts)
    if s.strip():
        paras.append(s)

keywords = ['SKCenterLib', 'SKReplyLib', 'SKOrderLib', '登入', '回報', '回傳', '註冊', '雙因子', 'Log', 'Generate', 'SetLog', 'Order', 'Reply', 'OnReply']
for i, s in enumerate(paras, 1):
    if any(k in s for k in keywords):
        print(f'PARA {i}: {s[:1800]}')
        print('---')
