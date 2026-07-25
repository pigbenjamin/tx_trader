import zipfile, xml.etree.ElementTree as ET
p=r'D:\Stock\program_trading\CapitalAPI_2.13.58_PythonExample\策略王COM元件使用說明_V2.13.58.docx'
z=zipfile.ZipFile(p)
xml=z.read([n for n in z.namelist() if n.endswith('document.xml')][0])
root=ET.fromstring(xml)
ns={'w':'http://schemas.openxmlformats.org/wordprocessingml/2006/main'}
paras=[]
for para in root.findall('.//w:p', ns):
    texts=[t.text or '' for t in para.findall('.//w:t', ns)]
    s=''.join(texts).strip()
    if s:
        paras.append(s)

# Find FUTUREPROXYORDER sections
for i,s in enumerate(paras,1):
    if 'FUTUREPROXYORDER' in s:
        print(f'===LINE {i}===')
        start=max(0, i-2)
        end=min(len(paras), i+40)
        for j in range(start, end):
            print(f'{j}: {paras[j-1][:200]}')
        print()
