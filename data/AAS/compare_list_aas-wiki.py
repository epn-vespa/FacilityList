### Compare les listes en utilisant le module fuzzy
import sys
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from multiprocessing import Pool
import json
from pathlib import Path

data_dir = Path(__file__).parent / "input_data"
wikidata_dir = Path(__file__).parents[1] / "WIKIDATA"

with open(data_dir / 'aas.json') as f:
    data_aas = json.load(f)

with open(wikidata_dir / 'Scripts' / 'extract_wikidata_V_1.1.json') as f:
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
    print("[" + str(i + 1) + "/" + str(len(data_aas)) + "]" + str(e))
    print("  " + str(r[0][1]) + " : " + str(r[0][0]))
    return r


def compare_aas(data_aas, wikidata, results_count_output_file):
    results = []
    tres_certain_data_aas = []
    tres_probable_data_aas = []
    probable_data_aas = []
    non_trouves_data_aas = []

    with Pool(8) as p:
        results = p.map(get_scores, enumerate(data_aas))

    for i, e in enumerate(data_aas):
        r = results[i]
        trouve = False
        for r_elem in r:
            if r_elem[1] > 180:
                trouve = True
                tres_certain_data_aas.append((e, r_elem[0]))
            #elif r_elem[1] > 130:
                 #tres_probable_data_aas.append((e, r_elem[0]))
            # elif r_elem[1] > 150:
            # probable_data_aas.append((e, r_elem[0]))
        if not trouve: non_trouves_data_aas.append(e)


    print("tres_certain_data-aas : " + str(len(tres_certain_data_aas)))
    print("tres_probable_data-aas : " + str(len(tres_probable_data_aas)))
    print("probable_data-aas : " + str(len(probable_data_aas)))
    print("non_trouves_data-aas : " + str(len(non_trouves_data_aas)))

    with open("tres_certain_data-aas.json", 'w') as fout:
        fout.write(json.dumps(tres_certain_data_aas, indent=4))
    with open("tres_probable_data-aas.json", 'w') as fout:
        fout.write(json.dumps(tres_probable_data_aas, indent=4))
    with open("probable_data-aas.json", 'w') as fout:
        fout.write(json.dumps(probable_data_aas, indent=4))
    with open("non_trouves_data-aas.json", 'w') as fout:
        fout.write(json.dumps(non_trouves_data_aas, indent=4))
        for i, e in enumerate(data_aas):
            r = results[i]
            print({"[" + str(i + 1) + "/" + str(len(data_aas)) + "]" + str(e): r}, file=fout)
        for t in r:
            print("  " + str(t[1]) + " : " + str(t[0]), file=fout)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        results_count_output_file = open(sys.argv[1], 'a')
    else:
        results_count_output_file = sys.stdout
    compare_aas(data_aas, wikidata, results_count_output_file)
