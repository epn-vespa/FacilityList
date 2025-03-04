### Compare les listes en utilisant le module fuzzy

from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import json
import cProfile
from multiprocessing import Pool
import sys

with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/NSSDC/datas/NSSDC_list1.json') as f:
    data_nssdc = json.load(f)

with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/WIKIDATA/scripts/extract_wikidata.json') as f:
    wikidata = json.load(f)

def mon_scorer(q, c):
    r = fuzz.WRatio(q['Name'], c['itemLabel']) + fuzz.WRatio(q['Name'], c['aliases'])
    if c['all_COSPAR_ID'] != "":
        if q['ID'] == c['all_COSPAR_ID']:
            r += 500
        else:
            r -= 100
    if c['all_NSSDCA_ID'] != "":
        if q['ID'] == c['all_NSSDCA_ID']:
            r += 500
        else:
            r -= 100
    return r

def dummy_proc(x):
    return x



def get_scores( t ):
    i = t[0]
    e = t[1]
    r = process.extract(e, wikidata, processor=dummy_proc, scorer=mon_scorer)
    print("[" + str(i + 1) + "/" + str(len(data_nssdc)) + "]" + str(e))
    print("  " + str(r[0][1]) + " : " + str(r[0][0]))
    try:
        print("  " + str(r[0][1]) + " : " + str(r[0][0]))
    except IndexError:
        print(None)
    return r


def compare_NSSDC(data_nssdc, wikidata, results_count_output_file):
    results = []
    tres_certain = []
    tres_probable = []
    probable = []
    non_trouves = []

    with Pool(8) as p:
        results = p.map(get_scores, enumerate(data_nssdc) )

    for i, e in enumerate(data_nssdc):
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
        for i, e in enumerate(data_nssdc):
            r = results[i]
            print({"[" + str(i + 1) + "/" + str(len(data_nssdc)) + "]" + str(e): r}, file=fout)
            for t in r:
                print("  " + str(t[1]) + " : " + str(t[0]), file=fout)

if __name__ == "__main__" :
    # chosse to either run with or without profiling
    compare_NSSDC(data_nssdc, wikidata)
    # cProfile.run("compare_NSSDC(data_nssdc[0:10], wikidata)")
    if len(sys.argv) > 1:
        results_count_output_file = open(sys.argv[1], 'a')
    else:
        results_count_output_file = sys.stdout
    compare_NSSDC(data_nssdc, wikidata, results_count_output_file)