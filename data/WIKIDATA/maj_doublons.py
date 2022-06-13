import json
import sys
from collections import Counter

with open("/Users/ldebisschop/Documents/GitHub/FacilityList/data/WIKIDATA/list_observatories_spacecrafts1.json") as fin:
    r = json.load(fin)
names = [a['itemLabel'] for a in r]
duplicate_names = [k for (k,v) in Counter(names).items() if v > 1]
print(duplicate_names)

duplicatas = {}
for name in duplicate_names :
    duplicatas[name] = [ e["item"] for e in r if e["itemLabel"]==name ]

print(duplicatas)


with open("memes_noms.json", 'w') as fout:
    fout.write(json.dumps(duplicatas, indent=4))

