## Compare les listes en utilisant le module fuzzy

from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from multiprocessing import Pool
import cProfile
import json
import sys

with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/WiseREP/data/WiseREP.json') as f:
    data_wiserep = json.load(f)

with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/WIKIDATA/scripts/extract_wikidata.json') as f:
    wikidata = json.load(f)


def mon_scorer(q, c):
    r = 0
    if 'Name' in q:
        if 'itemLabel' in c:
            if q['Name'] in c['itemLabel']:
                r += 500
            else:
                r -= 50
        if 'aliases' in c:
            aliases_parts = c['aliases']
            if q['Name'] in aliases_parts:
                r += 500
            else:
                r -= 50
        if "ID" in q and 'itemLabel' in c:
            if q['ID'].lower() in c['itemLabel']:
                r += 500+
            else:
                r -= 50
        if "ID" in q and 'aliases' in c:
            if q['ID'].lower() in c['aliases']:
                r += 500
            else:
                r -= 50
    r += fuzz.WRatio(q['Name'], c['itemLabel']) + fuzz.WRatio(q['Name'], c['aliases']) + fuzz.WRatio(q['ID'], c[
        'itemLabel']) + fuzz.WRatio(q['ID'], c['aliases'])
    return r


def dummy_proc(x):
    return x


def get_scores(t):
    i = t[0]
    e = t[1]
    r = process.extract(e, wikidata, processor=dummy_proc, scorer=mon_scorer)
    print("[" + str(i + 1) + "/" + str(len(data_wiserep)) + "]" + str(e))
    try :
        print("  " + str(r[0][1]) + " : " + str(r[0][0]))
    except IndexError :
        print(None)

    return r


def compare_wiserep(data_wiserep, wikidata, results_count_output_file):
    # results = [] # results is a list
    tres_certain_wiserep = []
    #tres_probable_wiserep = []
    #probable_wiserep = []
    non_trouves_wiserep = []

    with Pool(8) as p:
        results = p.map(get_scores, enumerate(data_wiserep))

    for i, e in enumerate(data_wiserep):
        r = results[i]
        trouve = False
        for r_elem in r:
            if r_elem[1] > 180:
                trouve = True
                tres_certain_wiserep.append((e, r_elem[0]))
            #elif r_elem[1] > 130:
             #    tres_probable_wiserep.append((e, r_elem[0]))
            # elif r_elem[1] > 150:
            # probable_wiserep.append((e, r_elem[0]))
        if not trouve:
            non_trouves_wiserep.append(e)


    print("tres_certain_wiserep : " + str(len(tres_certain_wiserep)))
    #print("tres_probable_wiserep : " + str(len(tres_probable)))
    #print("probable_wiserep : " + str(len(probable)))
    print("non_trouves_wiserep: " + str(len(non_trouves_wiserep)))

    with open("tres_certain_wiserep.json", 'w') as fout:
        fout.write(json.dumps(tres_certain_wiserep, indent=4))
   # with open("tres_probable.json", 'w') as fout:
        #fout.write(json.dumps(tres_probable, indent=4))
    #with open("probable.json", 'w') as fout:
        #fout.write(json.dumps(probable, indent=4))
    with open("non_trouves_wiserep.json", 'w') as fout:
        fout.write(json.dumps(non_trouves_wiserep, indent=4))
        for i, e in enumerate(data_wiserep):
            r = results[i]
            print({"[" + str(i + 1) + "/" + str(len(data_wiserep)) + "]" + str(e): r}, file=fout)
        for t in r:
            print("  " + str(t[1]) + " : " + str(t[0]), file=fout)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        results_count_output_file = open(sys.argv[1], 'a')
    else:
        results_count_output_file = sys.stdout
    compare_wiserep(data_wiserep, wikidata, results_count_output_file)
