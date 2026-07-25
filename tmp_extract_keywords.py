import zipfile
from xml.etree import ElementTree as ET
p = r'D:\Stock\program_trading\CapitalAPI_2.13.58_PythonExample\策略王COM元件使用說明_V2.13.58.docx'
z = zipfile.ZipFile(p)
xml = z.read([n for n in z.namelist() if n.endswith('document.xml')][0])
root = ET.fromstring(xml)
ns = {'w': 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'}
paras = []
for para in root.findall('.//w:p', ns):
    texts = []
    for t in para.findall('.//w:t', ns):
        texts.append(t.text or '')
    paras.append(''.join(texts))
keywords = ['Reply', 'OnReply', 'OnReplyMessage', '登入', '回報', '回傳', '註冊', 'SKReplyLib', 'SKCenterLib']
for i, s in enumerate(paras):
    if any(k in s for k in keywords):
        print('PARA', i+1)
        print(s[:1200])
        print('---')
