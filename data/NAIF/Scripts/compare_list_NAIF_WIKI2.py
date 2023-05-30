### Compare les listes en utilisant le module fuzzy

from fuzzywuzzy import fuzz
from fuzzywuzzy import process

import json

import cProfile

from multiprocessing import Pool


def mon_scorer(q, c):
    r = fuzz.WRatio(q['Name'], c['itemLabel']) + fuzz.WRatio(q['Name'], c['aliases'])
    if c['all_NAIF_ID'] != "":
        if q['ID'] == c['all_NAIF_ID']:
            r += 500
        else:
            r -= 100
    return r

def dummy_proc(x):
    return x

with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/NAIF/Scripts/naif_dsn.json') as f:
    data_naif = json.load(f)

with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/WIKIDATA/scripts/extract_wikidata.json') as f:
    wikidata = json.load(f)

def get_scores( t ):
    i=t[0]
    e=t[1]
    r = process.extract(e, wikidata, processor=dummy_proc, scorer=mon_scorer)
    print("[" + str(i + 1) + "/" + str(len(data_naif)) + "]" + str(e))
    print("  " + str(r[0][1]) + " : " + str(r[0][0]))
    return r


def compare_naif(data_naif, wikidata) :
    results = []
    tres_certain = []
    tres_probable = []
    probable = []
    non_trouves = []

    with Pool(8) as p:
        results = p.map(get_scores, enumerate(data_naif) )

    for i, e in enumerate(data_naif):
        r = results[i]
        trouve = False
        for r_elem in r:
            if r_elem[1] > 400:
                trouve = True
                tres_certain.append((e, r_elem[0]))
            #elif r_elem[1] > 160:
                #tres_probable.append((e, r_elem[0]))
            #elif r_elem[1] > 120:
                #probable.append((e, r_elem[0]))
        if not trouve: non_trouves.append(e)

    print("tres_certain : " + str(len(tres_certain)))
    print("tres_probable : " + str(len(tres_probable)))
    print("probable : " + str(len(probable)))
    print("non_trouves : " + str(len(non_trouves)))

    with open("tres_certain.json", 'w') as fout:
        fout.write(json.dumps(tres_certain, indent=4))
    with open("tres_probable.json", 'w') as fout:
        fout.write(json.dumps(tres_probable, indent=4))
    with open("probable.json", 'w') as fout:
        fout.write(json.dumps(probable, indent=4))
    with open("non_trouves.json", 'w') as fout:
        fout.write(json.dumps(non_trouves, indent=4))
    with open("results.json", 'w') as fout:
        for i, e in enumerate(data_naif):
            r = results[i]
            print({"[" + str(i + 1) + "/" + str(len(data_naif)) + "]" + str(e): r}, file=fout)
            for t in r:
                print("  " + str(t[1]) + " : " + str(t[0]), file=fout)

if __name__ == "__main__" :
    # chosse to either run with or without profiling
    compare_naif(data_naif, wikidata)
    # cProfile.run("compare_NSSDC(data_nssdc[0:10], wikidata)")
