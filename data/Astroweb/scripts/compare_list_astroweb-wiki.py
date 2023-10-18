### Compare les listes en utilisant le module fuzzy

from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from multiprocessing import Pool
import cProfile
import json

with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/Astroweb/data/astroweb.json') as f:
    data_astroweb = json.load(f)

with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/WIKIDATA/scripts/extract_wikidata.json') as f:
    wikidata = json.load(f)


def mon_scorer(q, c):
    r = fuzz.WRatio(q['Name'], c['itemLabel']) + fuzz.WRatio(q['Name'], c['aliases']) + fuzz.WRatio(q['ID'], c[
        'itemLabel']) + fuzz.WRatio(q['ID'], c['aliases'])
    if "Name" in q and 'itemLabel' in c:
        if q['Name'] in c['itemLabel']:
            r += 500
        else:
            r -= 50
    if "Name" in q and 'aliases' in c:
        if q['Name'] in c['aliases'].split("|"):
            r += 500
        else:
             r -= 50
    if "ID" in q and 'itemLabel' in c:
        if q['ID'].lower() in c['itemLabel']:
            r += 500
        else:
            r -= 50
    if "ID" in q and 'aliases' in c:
        if q['ID'].lower().strip() in c['aliases'].lower().split("|"):
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
    print("[" + str(i + 1) + "/" + str(len(data_atroweb)) + "]" + str(e))
    print("  " + str(r[0][1]) + " : " + str(r[0][0]))
    return r


def compare_astroweb(data_astroweb, wikidata):
    results = []
    tres_certain_data_astroweb = []
    tres_probable_data_astroweb = []
    probable_data_astroweb = []
    non_trouves_data_astroweb = []

    with Pool(8) as p:
        results = p.map(get_scores, enumerate(data_astroweb))

    for i, e in enumerate(data_astroweb):
        r = results[i]
        trouve = False
        for r_elem in r:
            if r_elem[1] > 180:
                trouve = True
                tres_certain_data_astroweb.append((e, r_elem[0]))
            elif r_elem[1] > 130:
                 tres_probable_data_astroweb.append((e, r_elem[0]))
            # elif r_elem[1] > 150:
            # probable_data_astroweb.append((e, r_elem[0]))
        if not trouve: non_trouves_data_astroweb.append(e)


    print("tres_certain_data-aas : " + str(len(tres_certain_data_astroweb)))
    print("tres_probable_data-aas : " + str(len(tres_probable_data_astroweb)))
    print("probable_data-aas : " + str(len(probable_data_astroweb)))
    print("non_trouves_data-aas : " + str(len(non_trouves_data_astroweb)))

    with open("tres_certain_data-aas.json", 'w') as fout:
        fout.write(json.dumps(tres_certain_data_astroweb, indent=4))
    with open("tres_probable_data-aas.json", 'w') as fout:
        fout.write(json.dumps(tres_probable_data_astroweb, indent=4))
    with open("probable_data-aas.json", 'w') as fout:
        fout.write(json.dumps(probable_data_astroweb, indent=4))
    with open("non_trouves_data-aas.json", 'w') as fout:
        fout.write(json.dumps(non_trouves_data_astroweb, indent=4))
        for i, e in enumerate(data_astroweb):
            r = results[i]
            print({"[" + str(i + 1) + "/" + str(len(data_astroweb)) + "]" + str(e): r}, file=fout)
        for t in r:
            print("  " + str(t[1]) + " : " + str(t[0]), file=fout)
if __name__ == "__main__":
    # chosse to either run with or without profiling
    compare_astroweb(data_astroweb, wikidata)
    # cProfile.run("compare_astroweb(data_astroweb[0:10], wikidata)")
