#############################################################################################################################
# Compare strings of characters between the list IAU_MPC and a list of items extract of wikidata with fuzzywuzzy library ####
#############################################################################################################################

from FuzzyWuzzy import fuzz
from FuzzyWuzzy import process
from multiprocessing import Pool
import json
import sys
import cProfile

#Takes as input a query object and a candidate object and returns a score indicating the similarity between their names and IAU identifiers
def mon_scorer(q, c):
    r = fuzz.WRatio(q['Name'], c['itemLabel']) + fuzz.WRatio(q['Name'], c['aliases']) + fuzz.WRatio(q['ID'], c['all_Minor_Planet_Center_observatory_ID'])
    if c['all_Minor_Planet_Center_observatory_ID'] != "":
        if q['ID'] == c['all_Minor_Planet_Center_observatory_ID']:
            r += 500
        else:
            r -= 100
    return r

def dummy_proc(x):
    return x

with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/IAU-MPC/data/iau-mpc.json') as f:
    data_iau = json.load(f)

with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/WIKIDATA/scripts/extract_wikidata.json') as f:
    wikidata = json.load(f)

# Search for the best match based on the custom scoring function
def get_scores( t ):
    i=t[0]
    e=t[1]
    r = process.extract(e, wikidata, processor=dummy_proc, scorer=mon_scorer)
    print("[" + str(i + 1) + "/" + str(len(data_iau)) + "]" + str(e))
    print("  " + str(r[0][1]) + " : " + str(r[0][0]))
    return r

#Initializes several empty lists to store the results of the comparison.
#Use the "multiprocessing" libray to run the "get_scores"  fucntion in parallel for each element of "data_iau"
#The results are stored in the "results" list
def compare_iau(data_iau, wikidata, results_count_output_file):
    results = []
    tres_certain_iau = []
    #tres_probable_iau = []
    #probable_iau = []
    non_trouves_iau = []

    with Pool(8) as p:
        results = p.map(get_scores, enumerate(data_iau) )

# if the score of the best match is higher than certain threshold, the pair is adde to the " tres_certain" list.
# if no matches are found, the element is ades to the "non_trouves" list.
    for i, e in enumerate(data_iau):
        r = results[i]
        trouve = False
        for r_elem in r:
            if r_elem[1] > 400:
                trouve = True
                tres_certain_iau.append((e, r_elem[0]))
            #elif r_elem[1] > 160:
                #tres_probable_iau.append((e, r_elem[0]))
            #elif r_elem[1] > 120:
                #probable_iau.append((e, r_elem[0]))
        if not trouve: non_trouves_iau.append(e)

    print("tres_certain_iau : " + str(len(tres_certain_iau)))
    #print("tres_probable_iau : " + str(len(tres_probable_iau)))
    #print("probable_iau : " + str(len(probable_iau)))
    print("non_trouves_iau : " + str(len(non_trouves_iau)))

#the fuction writes the results to seprarate JSON files and pritns the number of elements in each list.
    with open("tres_certain_iau.json", 'w') as fout:
        fout.write(json.dumps(tres_certain_iau, indent=4))
    #with open("tres_probable_iau.json", 'w') as fout:
    #    fout.write(json.dumps(tres_probable_iau, indent=4))
    #with open("probable_iau.json", 'w') as fout:
    #    fout.write(json.dumps(probable_iau, indent=4))
    with open("non_trouves_iau.json", 'w') as fout:
        fout.write(json.dumps(non_trouves_iau, indent=4))
    with open("results.json", 'w') as fout:
        for i, e in enumerate(data_iau):
            r = results[i]
            print({"[" + str(i + 1) + "/" + str(len(data_iau)) + "]" + str(e): r}, file=fout)
            for t in r:
                print("  " + str(t[1]) + " : " + str(t[0]), file=fout)

if __name__ == "__main__" :
    if len(sys.argv) > 1:
        results_count_output_file = open(sys.argv[1], 'a')
    else:
        results_count_output_file = sys.stdout
    compare_iau(data_iau, wikidata, results_count_output_file)