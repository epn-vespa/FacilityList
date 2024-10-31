import json
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import itertools

def compare_names(resource_name, labels):
    resource_name_lower = resource_name.lower()
    return any(resource_name_lower in label.lower() for label in labels)

def compare_item(item1, item2):
    labels_comparaison = [item2["@id"], item2["rdfs:label"]] + item2.get("skos:altLabel", [])

    correspondances = []
    if compare_names(item1["title"], labels_comparaison):
        correspondances.append({
            "ID": item2["@id"],
            "Label": item2["rdfs:label"],
            "Comment": item2.get("rdfs:comment", ""),
            "altLabel": item2.get("skos:altLabel", []),
            "ExactMatch": item2.get("skos:exactMatch", []),
        })

    return correspondances

def compare_all(items):
    item1, liste2 = items
    res1 = {
        "title": item1["title"],
        "logical_identifier": item1[" logical_identifier"],
        "Matches": [],
    }

    for item2 in liste2:
        correspondances = compare_item(item1, item2)
        if correspondances:
            res1["Matches"].extend(correspondances)

    return res1 if res1["Matches"] else None

if __name__ == '__main__':
    with open('/Users/ldebisschop/PycharmProjects/FacilityList/data/PDS/data/pds_facility.json', 'r') as f:
        liste1 = json.load(f)

    with open('/Users/ldebisschop/PycharmProjects/FacilityList/data/obs-facilities_vocabulary/obsfacility-vocabulary_v_1.1.json', 'r') as f:
        liste2 = json.load(f)

    pool = Pool(cpu_count())
    items = list(itertools.product(liste1, [liste2]))
    resultats = []

    for resultat in tqdm(pool.imap(compare_all, items), total=len(liste1)):
        if resultat is not None:
            resultats.append(resultat)

    pool.close()
    pool.join()
    with open('results_comparison_pds_obsfacility.json', 'w') as f:
        json.dump(resultats, f, ensure_ascii=False, indent=4)


    with open('added-spase-id_manually.json', 'w') as f:
        json.dump(liste2, f, ensure_ascii=False, indent=4)

    print("Comparaison terminée. Résultats sauvegardés dans resultats_comparaison.json")
