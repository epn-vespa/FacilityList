### Compare les listes en utilisant le module fuzzy

from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from multiprocessing import Pool
import cProfile
import json
import sys

with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/IRAF/data/iraf.json') as f:
    data_iraf = json.load(f)

with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/WIKIDATA/scripts/extract_wikidata.json') as f:
    wikidata = json.load(f)
    
def mon_scorer(q, c):
    r = fuzz.WRatio(q['Name'], c['itemLabel']) + fuzz.WRatio(q['Name'], c['aliases']) + fuzz.WRatio(q['ID'], c[
        'itemLabel']) + fuzz.WRatio(q['ID'], c['aliases'])
    if "Name" in q and 'itemLabel' in c:
        if q['Name'] == c['itemLabel']:
            r += 500
        else:
            r -= 50
    if "Name" in q and 'aliases' in c:
        if q['ID'] in c['aliases'].split("|"):
            r += 500
        else:
            r -= 50

    return r
def dummy_proc(x):
    return x
def get_scores(t):
    i = t[0]
    e = t[1]
    r = process.extract(e, wikidata, processor=dummy_proc, scorer=mon_scorer)
    print("[" + str(i + 1) + "/" + str(len(data_iraf)) + "]" + str(e))
    print("  " + str(r[0][1]) + " : " + str(r[0][0]))
    return r

def compare_iraf(data_iraf, wikidata):
    results_iraf = []
    tres_certain_iraf = []
    tres_probable_iraf = []
    probable_iraf = []
    non_trouves_iraf = []

    with Pool(8) as p:
        results = p.map(get_scores, enumerate(data_iraf))

    for i, e in enumerate(data_iraf):
        r = results_iraf[i]
        trouve = False
        for r_elem in r:
            if r_elem[1] > 500:
                trouve = True
                tres_certain_iraf.append((e, r_elem[0]))
            # elif r_elem[1] > 180:
            # tres_probable_iraf.append((e, r_elem[0]))
            # elif r_elem[1] > 150:
            # probable_iraf.append((e, r_elem[0]))
        if not trouve: non_trouves_iraf.append(e)

    print("tres_certain_iraf : " + str(len(tres_certain_iraf)))
    #print("tres_probable_iraf : " + str(len(tres_probable_iraf)))
    #print("probable_iraf : " + str(len(probable_iraf)))
    print("non_trouves_iraf : " + str(len(non_trouves_iraf)))

    with open("tres_certain.json",'w') as fout:
        fout.write(json.dumps(tres_certain_iraf, indent=4))
    #with open("tres_probable.json",'w') as fout:
        #fout.write(json.dumps(tres_probable_iraf, indent=4))
    #with open("probable.json",'w') as fout:
        #fout.write(json.dumps(probable_iraf, indent=4))
    with open("non_trouves.json",'w') as fout:
        fout.write(json.dumps(non_trouves_iraf, indent=4))
        for i, e in enumerate(data_iraf):
            r = results[i]
            print({"[" + str(i + 1) + "/" + str(len(data_iraf)) + "]" + str(e): r}, file=fout)
        for t in r:
            print("  " + str(t[1]) + " : " + str(t[0]), file=fout)
if __name__ == "__main__":
    if len(sys.argv) > 1:
        results_count_output_file = open(sys.argv[1], 'a')
    else:
        results_count_output_file = sys.stdout
    compare_iraf(data_iraf, wikidata, results_count_output_file)
   

   