import lxml
from lxml import etree
import json

def element_path(xmltreeelement):
    # AttributeError: 'NoneType' object has no attribute 'getparent'
    try :
        r = element_path(xmltreeelement.getparent())
    except AttributeError: return xmltreeelement.tag
    try :
        r += "/" + xmltreeelement.tag()
    except TypeError :
        try :
            r += "/" + type(xmltreeelement.tag()).__name__
        except TypeError :
            r += "/" + type(xmltreeelement.tag).__name__
    return r

input_file= open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/Astroweb/Astroweb.html', "r")
output_file="/Users/ldebisschop/Documents/GitHub/FacilityList/data/Astroweb/Astroweb.json"


tree = etree.parse(input_file)
print(tree)
#print("tree.getroot() =", tree.getroot())
root = tree.getroot()
#print("root.findall ('./RESOURCE/TABLE/DATA/') =",root.findall ('./RESOURCE/TABLE/DATA/'))
#print("root.xpath('.') =", root.xpath('.'))
#print("root.items() =", root.items())
for k in root.keys() :
    print("root[" + k + "] =", root.get(k))
print("root.tag =", root.tag)
print("element_path(root) =", element_path(root))
print('root.findall("RESOURCE") =', root.findall("RESOURCE"))
#print('root.findall("{http://www.ivoa.net/xml/VOTable/v1.2}RESOURCE") =', root.findall("{http://www.ivoa.net/xml/VOTable/v1.2}RESOURCE"))
for child in root.iterchildren() :
    print(element_path(child))
#TAG=root.tag.replace("VOTABLE","")
#print("found TAG : " + TAG)
findall_path='./'+ TAG + 'RESOURCE/'+ TAG + 'TABLE/'+ TAG + 'DATA'
print("root.findall(findall_path) =", root.findall(findall_path))
print("element_path(root.findall(findall_path)) =", element_path(root.findall(findall_path)[0]))


# get table fields
print()
print("### get table fields :")
print()
fields=root.findall('./' + 'RESOURCE/'+ TAG + 'TABLE/'+ TAG + 'FIELD')
try :
    fields_descriptions=[ x.find( TAG + "DESCRIPTION" ).text.strip() for x in fields ]
except AttributeError :
    fields_descriptions=[ x.get("name").strip() for x in fields ]

for description in fields_descriptions :
    print(description)

# get table data
print()
print("### get table data :")
print()
table_data = root.findall('./'+ TAG + 'RESOURCE/'+ TAG + 'TABLE/'+ TAG + 'DATA/' + TAG + "TABLEDATA")[0]
print("element_path(table_data) = ", element_path(table_data))

rows_list=[]
# iterate over tr nodes (lines)
for tr in table_data.findall( TAG + "TR" ) :
    # for each line, add an enememt to the rows list
    row = {}
    for i , f in enumerate(tr.findall( TAG + "TD" )) :
        if f.text is None : continue # skip to next field if field is empty
        row[fields_descriptions[i]] = f.text.strip()
    rows_list.append(row)
    
    
print(json.dumps(rows_list, indent=4))

with open(output_file,"w") as out_f:
    out_f.write(json.dumps(rows_list, indent=4))

#print("root.values()=",root.values())
