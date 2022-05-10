### Compare les listes en utilisant le module fuzzy

from fuzzywuzzy import fuzz
from fuzzywuzzy import process

import json

with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/AAS/AAS.json') as f:
    data_aas = json.load(f)

with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/WIKIDATA/list_observatories_spacecrafts.json') as f:
    wikidata = json.load(f)

results = []
tres_certain = []
tres_probable = []
probable = []
non_trouves = []

data_aas = data_aas[0:1000]
for i,e in enumerate(data_aas):
    print("[" + str(i+1) + "/" + str(len(data_aas))+"]" + str(e))
    #r = process.extract(e['Name']+' '+e['ID'], wikidata, scorer=fuzz.token_sort_ratio)

    def mon_scorer(q, c):
        r = fuzz.WRatio(q['ID'], c['itemLabel']) + fuzz.WRatio(q['ID'], c['aliases']) + fuzz.WRatio(q['ID'], c[
            'itemLabel']) + fuzz.WRatio(q['ID'], c['aliases'])


        if "ID" in q and 'itemLabel' in c:
            if q['ID'] == c['itemLabel']:
                r += 400
            else:
                r -= 50
        if "ID" in q and 'aliases' in c:
            if q['ID'] in c['aliases'].split("|"):
                r += 200
            else:
                r -= 50

        return r

    def dummy_proc(x):
        return x

    r = process.extract(e, wikidata, processor=dummy_proc, scorer=mon_scorer)


    results.append(r)
    trouve = False
    for r_elem in r:
        print("    " + str(r_elem[1]) + " : " + str(r_elem[0]))
        if r_elem[1] > 500:
            trouve = True
            tres_certain.append((e, r_elem[0]))
        elif r_elem[1] > 180:
            tres_probable.append((e, r_elem[0]))
        elif r_elem[1] > 150:
            probable.append((e, r_elem[0]))
    if not trouve : non_trouves.append(e)

print("tres_certain : " + str(len(tres_certain)))
print("tres_probable : " + str(len(tres_probable)))
print("probable : " + str(len(probable)))
print("non_trouves : " + str(len(non_trouves)))

with open("tres_certain.json",'w') as fout:
    fout.write(json.dumps(tres_certain, indent=4))
with open("tres_probable.json",'w') as fout:
    fout.write(json.dumps(tres_probable, indent=4))
with open("probable.json",'w') as fout:
    fout.write(json.dumps(probable, indent=4))
with open("non_trouves.json",'w') as fout:
    fout.write(json.dumps(non_trouves, indent=4))