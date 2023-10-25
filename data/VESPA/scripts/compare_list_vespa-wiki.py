### Compare les listes en utilisant le module fuzzy

from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from multiprocessing import Pool
import cProfile
import json

with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/VESPA/data/instrument_host_name.json') as f:
    data_vespa = json.load(f)

with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/WIKIDATA/scripts/extract_wikidata.json') as f:
    wikidata = json.load(f)


def mon_scorer(q, c):
    r = 0
    if 'instrument_host_name_lower' in q:
        if 'itemLabel' in c:
            if q['instrument_host_name_lower'] in c['itemLabel']:
                r += 500
            else:
                r -= 50
        if 'aliases' in c:
            aliases_parts = c['aliases'].lower().split("|")
            if q['instrument_host_name_lower'] in aliases_parts:
                r += 500
            else:
                r -= 50
    r += fuzz.WRatio(q['instrument_host_name_lower'], c['itemLabel']) + fuzz.WRatio(q['instrument_host_name_lower'], c['aliases'])
    return r


def dummy_proc(x):
    return x


def get_scores(t):
    i = t[0]
    e = t[1]
    r = process.extract(e, wikidata, processor=dummy_proc, scorer=mon_scorer)
    print("[" + str(i + 1) + "/" + str(len(data_vespa)) + "]" + str(e))
    try :
        print("  " + str(r[0][1]) + " : " + str(r[0][0]))
    except IndexError :
        print(None)

    return r


def compare_vespa(data_vespa, wikidata):
    # results = [] # results is a list
    tres_certain_vespa = []
    tres_probable_vespa = []
    probable_vespa = []
    non_trouves_vespa = []

    with Pool(8) as p:
        results = p.map(get_scores, enumerate(data_vespa))

    for i, e in enumerate(data_vespa):
        r = results[i]
        trouve = False
        for r_elem in r:
            if r_elem[1] > 180:
                trouve = True
                tres_certain_vespa.append((e, r_elem[0]))
            #elif r_elem[1] > 130:
            #     tres_probable_vespa.append((e, r_elem[0]))
            # elif r_elem[1] > 150:
            # probable_vespa.append((e, r_elem[0]))
        if not trouve:
            non_trouves_vespa.append(e)


    print("tres_certain_vespa : " + str(len(tres_certain_vespa)))
    #print("tres_probable_vespa : " + str(len(tres_probable)))
    #print("probable_vespa : " + str(len(probable)))
    print("non_trouves_vespa : " + str(len(non_trouves_vespa)))

    with open("tres_certain_vespa.json", 'w') as fout:
        fout.write(json.dumps(tres_certain_vespa, indent=4))
    with open("tres_probable_vespa.json", 'w') as fout:
        fout.write(json.dumps(tres_probable_vespa, indent=4))
    with open("probable_vespa.json", 'w') as fout:
        fout.write(json.dumps(probable_vespa, indent=4))
    with open("non_trouves_vespa.json", 'w') as fout:
        fout.write(json.dumps(non_trouves_vespa, indent=4))
        for i, e in enumerate(data_vespa):
            r = results[i]
            print({"[" + str(i + 1) + "/" + str(len(data_vespa)) + "]" + str(e): r}, file=fout)
        for t in r:
            print("  " + str(t[1]) + " : " + str(t[0]), file=fout)


if __name__ == "__main__":
    # chosse to either run with or without profiling
    compare_vespa(data_vespa, wikidata)
