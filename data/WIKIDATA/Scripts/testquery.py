
import json

with open ('/Users/ldebisschop/Documents/GitHub/FacilityList/data/WIKIDATA/list_observatories_spacecrafts1.json') as f:
    r =json.load(f)

for item in r:
    item['itemLabel'] = item['itemLabel'].replace(' ', '-')

print(r)
