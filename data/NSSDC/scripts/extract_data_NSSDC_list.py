import lxml
from lxml import etree
import json

def element_path(xmltreeelement):
    # AttributeError: 'NoneType' object has no attribute 'getparent'
    try :
        r = element_path(xmltreeelement.getparent())
    except AttributeError: return xmltreeelement.tag
    return r + "/" + xmltreeelement.tag
 
conversions=[
    ( "/Users/ldebisschop/Documents/GitHub/FacilityList/data/NSSDC/NSSDC_Planetery-Science.xml",
        "/Users/ldebisschop/Documents/GitHub/FacilityList/data/NSSDC/NSSDC_Planetery-Science.json"),
    ( "/Users/ldebisschop/Documents/GitHub/FacilityList/data/NSSDC/NSSDC_Solar-Physics.xml",
        "/Users/ldebisschop/Documents/GitHub/FacilityList/data/NSSDC/NSSDC_Solar-Physics.json"),
    ( "/Users/ldebisschop/Documents/GitHub/FacilityList/data/NSSDC/NSSDC_Space-Physics.xml",
        "/Users/ldebisschop/Documents/GitHub/FacilityList/data/NSSDC/NSSDC_Space-Physics.json")
    ]
 
def my_votable2json(input_file, output_file) :
    print("input_file="+input_file)
    print("output_file="+output_file)
    
    tree = etree.parse(input_file)
    print(tree)
    print("tree.getroot() =", tree.getroot())
    root = tree.getroot()
    print("root.findall ('./RESOURCE/TABLE/DATA/') =",root.findall ('./RESOURCE/TABLE/DATA/'))
    print("root.xpath('.') =", root.xpath('.'))
    print("root.items() =", root.items())
    for k in root.keys() :
        print("root[" + k + "] =", root.get(k))
    print("root.tag =", root.tag)
    print("element_path(root) =", element_path(root))
    print('root.findall("RESOURCE") =', root.findall("RESOURCE"))
    print('root.findall("{http://www.ivoa.net/xml/VOTable/v1.2}RESOURCE") =', root.findall("{http://www.ivoa.net/xml/VOTable/v1.2}RESOURCE"))
    for child in root.iterchildren() :
        print(element_path(child))
    TAG="{http://www.ivoa.net/xml/VOTable/v1.2}"
    findall_path='./'+ TAG + 'RESOURCE/'+ TAG + 'TABLE/'+ TAG + 'DATA'
    #print("root.findall(findall_path) =", root.findall(findall_path))
    #print("element_path(root.findall(findall_path)) =", element_path(root.findall(findall_path)[0]))


    # get table fields
    print()
    print("### get table fields :")
    print()
    fields=root.findall('./'+ TAG + 'RESOURCE/'+ TAG + 'TABLE/'+ TAG + 'FIELD')
    fields_names=[ x.get( "name" ) for x in fields ]
    fields_descriptions=[ x.find( TAG + "DESCRIPTION" ).text.strip() for x in fields ]
    fields_descriptions=[ x.replace("\n                   ","") for x in fields_descriptions  ]
    for description in fields_descriptions :
        print(description)

    # get table data
    print()
    print("### get table data :")
    print()
    table_data = root.findall('./'+ TAG + 'RESOURCE/'+ TAG + 'TABLE/'+ TAG + 'DATA/' + TAG + "TABLEDATA")[0]
    #print("element_path(table_data) = ", element_path(table_data))

    rows_list=[]
    # iterate over tr nodes (lines)
    for tr in table_data.findall( TAG + "TR" ) :
        # for each line, add an enememt to the rows list
        row = {}
        for i , f in enumerate(tr.findall( TAG + "TD" )) :
            if f.text is None : continue # skip to next field if field is empty
            if i == 0 : # the first two fields are actually in the first TD [1]
                splitted_text=f.text.strip().split()
                row[fields_names[0]]=splitted_text[0]
                row[fields_names[1]]=" ".join(splitted_text[1:])
            else :
                # the content of every other TD referss to field n+1 (because [1])
                row[fields_names[i+1]] = f.text.strip()
        rows_list.append(row)
    #print(json.dumps(rows_list, indent=4))

    with open(output_file,"w") as out_f:
        out_f.write(json.dumps(rows_list, indent=4))

    #print("root.values()=",root.values())
for input_file, output_file in  conversions :
    my_votable2json(input_file, output_file)
