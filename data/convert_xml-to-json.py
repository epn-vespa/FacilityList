# Python3 code convert file xml to file json

import xmltodict
import json

with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/AAS/AAS.xml', 'r') as myfile:
    obj = xmltodict.parse(myfile.read())
json_content=json.dumps(obj, indent=4)
with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/AAS/AAS.json', 'w') as outfile:
    outfile.write(json_content)
    
