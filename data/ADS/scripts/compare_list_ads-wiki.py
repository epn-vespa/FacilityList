### Compare les listes en utilisant le module fuzzy

from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import sys
import json

with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/ADS/harvard_old.json') as f:
    data_ads = json.load(f)

with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/WIKIDATA/list_observatories_spacecrafts.json') as f:
    wikidata = json.load(f)

results = []
tres_certain = []
tres_probable = []
probable = []
non_trouves = []

data_ads = data_ads[0:1000]
for i,e in enumerate(data_ads):
    print("[" + str(i+1) + "/" + str(len(data_ads))+"]" + str(e))
    #r = process.extract(e['Name']+' '+e['ID'], wikidata, scorer=fuzz.token_sort_ratio)

    def mon_scorer(q, c):
        r = fuzz.WRatio(q['Name'], c['itemLabel']) + fuzz.WRatio(q['Name'], c['aliases']) + fuzz.WRatio(q['ID'], c[
            'itemLabel']) + fuzz.WRatio(q['ID'], c['aliases'])

        if "Name" in q and 'itemLabel' in c:
            if q['Name'] == c['itemLabel']:
                r += 400
            else:
                r -= 50
        if "Name" in q and 'aliases' in c:
            if q['Name'] in c['aliases'].split("|"):
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

#if __name__ == "__main__":
 #   if len(sys.argv) > 1:
 #       results_count_output_file = open(sys.argv[1], 'a')
 #   else:
 #       results_count_output_file = sys.stdout
 #   compare_ads(data_ads, wikidata, results_count_output_file)