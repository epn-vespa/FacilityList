import json
from difflib import SequenceMatcher
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import itertools


def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()


def compare_names(resource_name, labels, threshold=0.8):
    for label in labels:
        if similar(resource_name, label) >= threshold:
            return True
    return False


def compare_item(item1, item2):
    comparison_labels = [item2["rdfs:label"], item2["rdfs:comment"]] + item2.get("skos:sameAs", []) + item2.get(
        "skos:exactMatch", [])

    # Check if ResourceID is in skos:exactMatch
    if item1["ResourceID"] in item2.get("skos:exactMatch", []):
        return [{
            "ID": item2["@id"],
            "Label": item2["rdfs:label"],
            "Comment": item2["rdfs:comment"],
            "SameAs": item2.get("skos:sameAs", []),
            "ExactMatch": item2.get("skos:exactMatch", []),
        }], True

    matches = []
    if compare_names(item1["ResourceName"], comparison_labels, threshold=0.8):
        matches.append({
            "ID": item2["@id"],
            "Label": item2["rdfs:label"],
            "Comment": item2["rdfs:comment"],
            "SameAs": item2.get("skos:sameAs", []),
            "ExactMatch": item2.get("skos:exactMatch", []),
        })

    for alt_name in item1.get("AlternateName", []):
        if compare_names(alt_name, comparison_labels, threshold=0.8):
            matches.append({
                "ID": item2["@id"],
                "Label": item2["rdfs:label"],
                "Comment": item2["rdfs:comment"],
                "SameAs": item2.get("skos:sameAs", []),
                "ExactMatch": item2.get("skos:exactMatch", []),
            })

    return matches, False


def compare_all(items):
    item1, list2 = items
    res1 = {
        "ResourceName": item1["ResourceName"],
        "ResourceID": item1["ResourceID"],
        "Matches": [],
    }

    for item2 in list2:
        matches, exact_match_found = compare_item(item1, item2)
        if matches:
            if exact_match_found:
                res1["Matches"] = matches
                return res1  # Return immediately if exact match is found
            res1["Matches"].extend(matches)

    return res1


if __name__ == '__main__':
    with open('/users/ldebisschop/Documents/GitHub/FacilityList/data/SPASE/data/spase.json', 'r') as f:
        list1 = json.load(f)

    with open(
            '/users/ldebisschop/Documents/GitHub/FacilityList/data/obs-facilities_vocabulary/cleaned_obsfacility.json',
            'r') as f:
        list2 = json.load(f)

    pool = Pool(cpu_count())
    items = list(itertools.product(list1, [list2]))
    results = []

    for result in tqdm(pool.imap(compare_all, items), total=len(list1)):
        results.append(result)

    pool.close()
    pool.join()

    # Save the results to a file
    with open('comparison_results.json', 'w') as f:
        json.dump(results, f, indent=4)

    print("Comparison completed. Results saved to comparison_results.json")
