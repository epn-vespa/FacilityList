# Python3 code convert file xml to file json

import xmltodict
import json

with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/PDS/Collection_instrument_host_esa_v2.0.xml', 'r') as myfile:
    obj = xmltodict.parse(myfile.read())
json_content=json.dumps(obj, indent=4)
with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/PDS/Collection_instrument_host_esa_v2.0.json', 'w') as outfile:
    outfile.write(json_content)
    
