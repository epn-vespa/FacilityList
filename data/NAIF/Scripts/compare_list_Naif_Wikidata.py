import json
with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/WIKIDATA/list_observatories_spacecrafts.json', 'r') as f:
    data = json.load(f)
    
for item in data:
    if item["all_NAIF_ID"] != "":
        print(item)
        
with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/NAIF/NAIF.json', 'r') as f:
    naif_list = json.load(f)

naif_list = [item for item in data if item["all_NAIF_ID"] != ""]
#print(naif_list)
#print(len(naif_list))
    
with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/NAIF/NAIF_DSN/DSN.json', 'r') as f:
    naif_list = naif_list + json.load(f)
#print(data)


wikidata_naif_list = naif_list

naif_lookup = {}
for item in naif_list:
    if 'code' in item.keys():
        item['ID'] = item['code']
    if item["ID"] in naif_lookup.keys():
         naif_lookup[item["ID"]].append(item["Name"].strip("'"))
    else:
        naif_lookup[item["ID"]] = [item["Name"].strip("'")]
#print(naif_lookup)

for item in wikidata_naif_list:
    naif_id = item['all_NAIF_ID']
    if naif_id not in naif_lookup.keys():
        print("extra naif ID not in NAIF LIST (Wikidata naifid={naif_id}")
        #print(item)
    else:
       aliases = item['aliases'].split('|')
       for name in naif_lookup[naif_id]:
            if name not in aliases:
                print("NAIF name not found({naif_id}, {name})")
            else:
                print(f"NAIF name found ({naif_id}, {name})")
                
    