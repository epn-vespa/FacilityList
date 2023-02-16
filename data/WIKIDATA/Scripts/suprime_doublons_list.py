
import json

with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/NSSDC/NSSDC_list.json') as f:
    wikidata = json.load(f)

r_new = []

for e in wikidata:
    if e not in r_new:
        r_new.append(e)
print(r_new)
with open("NSSDC_list1.json", 'w') as fout:
    fout.write(json.dumps(r_new, indent=4))