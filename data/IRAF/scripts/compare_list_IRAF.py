### Compare les listes en utilisant le module fuzzy

from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from multiprocessing import Pool
import cProfile
import json

with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/IRAF/data/IRAF.json') as f:
    data_iraf = json.load(f)

with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/WIKIDATA/Scripts/extract_wikidata.json') as f:
    wikidata = json.load(f)


def mon_scorer(q, c):
    r = fuzz.WRatio(q['Name'], c['itemLabel']) + fuzz.WRatio(q['Name'], c['aliases']) + fuzz.WRatio(q['ID'], c[
        'itemLabel']) + fuzz.WRatio(q['ID'], c['aliases'])

    if "Name" in q and 'itemLabel' in c:
        if q['Name'] == c['itemLabel']:
            r += 500
        else:
            r -= 50
   # if "Name" in q and 'aliases' in c:
   #     if q['Name'] == c['aliases']:
   #          r += 500
   #     else:
   #          r -= 50
   # if "ID" in q and 'aliases' in c:
   #     if q['ID'] in c['aliases'].split("|"):
   #         r += 500
   #     else:
    #        r -= 50
   # if "ID" in q and 'itemLabel' in c:
    #    if q['ID'] in c['itemLabel'].split("|"):
    #        r += 500
    #    else:
    #        r -= 50
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


def compare_aas(data_iraf, wikidata):
    results = []
    tres_certain = []
    tres_probable = []
    probable = []
    non_trouves = []

    with Pool(8) as p:
        results = p.map(get_scores, enumerate(data_iraf))

    for i, e in enumerate(data_iraf):
        r = results[i]
        trouve = False
        for r_elem in r:
            if r_elem[1] > 180:
                trouve = True
                tres_certain.append((e, r_elem[0]))
            # elif r_elem[1] > 180:
            # tres_probable.append((e, r_elem[0]))
            # elif r_elem[1] > 150:
            # probable.append((e, r_elem[0]))
        if not trouve: non_trouves.append(e)

    print("tres_certain : " + str(len(tres_certain)))
    print("tres_probable : " + str(len(tres_probable)))
    print("probable : " + str(len(probable)))
    print("non_trouves : " + str(len(non_trouves)))

    with open("tres_certain.json", 'w') as fout:
        fout.write(json.dumps(tres_certain, indent=4))
    # with open("tres_probable.json",'w') as fout:
    # fout.write(json.dumps(tres_probable, indent=4))
    # with open("probable.json",'w') as fout:
    # fout.write(json.dumps(probable, indent=4))
    with open("non_trouves.json", 'w') as fout:
        fout.write(json.dumps(non_trouves, indent=4))
        for i, e in enumerate(data_iraf):
            r = results[i]
            print({"[" + str(i + 1) + "/" + str(len(data_iraf)) + "]" + str(e): r}, file=fout)
        for t in r:
            print("  " + str(t[1]) + " : " + str(t[0]), file=fout)


if __name__ == "__main__":
    # chosse to either run with or without profiling
    compare_aas(data_iraf, wikidata)
    # cProfile.run("compare_IRAF(data_iraf[0:10], wikidata)")



