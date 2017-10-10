import xml.etree.ElementTree as ET
tree = ET.parse("test01.xml")

root = tree.getroot()


#删除node
for country in root.findall('country'):
    print(country.tag,country.attrib)
    rank = int(country.find('rank').text)
    print(rank)
    if rank > 50:
        root.remove(country)     

tree.write('output.xml')