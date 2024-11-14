import json
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import itertools
import re


def format_resource_name(name):
    # Convertir en minuscules, supprimer les accents et remplacer les espaces par des tirets
    name = name.lower().strip()
    name = re.sub(r'\s+', '-', name)  # Remplacer les espaces par des tirets
    return name


def compare_names(resource_name, labels):
    resource_name_formatted = format_resource_name(resource_name)
    return any(resource_name_formatted in format_resource_name(label) for label in labels)


def compare_item(item1, item2):
    # Utiliser ResourceName et AlternateName pour la comparaison
    labels_comparaison = [item2["@id"], item2["rdfs:label"]] + item2.get("skos:altLabel", [])

    correspondances = []
    if compare_names(item1["ResourceName"], labels_comparaison) or \
            any(compare_names(alt_name, labels_comparaison) for alt_name in item1.get("AlternateName", [])):
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
    if "ResourceName" not in item1:
        # Skip this item if "ResourceName" is missing
        return None
    # Formater ResourceName
    res1 = {
        "ResourceName": format_resource_name(item1["ResourceName"]),
        "ResourceID": item1.get("ResourceID", "Unknown"),
        "Matches": [],
    }

    for item2 in liste2:
        correspondances = compare_item(item1, item2)
        if correspondances:
            res1["Matches"].extend(correspondances)

    return res1 if res1["Matches"] else None


if __name__ == '__main__':
    with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/WIKIDATA/scripts/extract_wikidata-11082024.json', 'r') as f:
        liste1 = json.load(f)

    with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/obs-facilities_vocabulary/obsfacilities_vocabulary_step1.json', 'r') as f:
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
