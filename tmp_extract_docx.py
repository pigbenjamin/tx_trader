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
for i, s in enumerate(paras):
    if '3.2' in s:
        print('PARA', i+1)
        print(s[:500])
        print('---')
