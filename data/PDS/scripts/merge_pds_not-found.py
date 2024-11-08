import json
import re

with open('/Users/ldebisschop/PycharmProjects/FacilityList/data/PDS/scripts/added-pds-id_manually.json') as f2:
    data2 = json.load(f2)
with open('/Users/ldebisschop/PycharmProjects/FacilityList/data/PDS/data/pds_facility.json') as f1:
    data1 = json.load(f1)

# transforme le titre en identifiant
def title_to_id(title):
    return re.sub(r'\s+', '-', title.strip().lower())

# enleve les caractères non-ASCII
def remove_non_ascii(text):
    return re.sub(r'[^\x00-\x7F]+', '', text)

# Nettoie la liste skos:altLabel
def clean_alt_labels(alt_labels):
    return [remove_non_ascii(label) for label in alt_labels]

# Compare et extraire les données
def compare_and_extract(data1, data2):
    # Extraire les identifiants du fichier 2
    data2_ids = {match for item in data2 for match in item.get("skos:exactMatch", [])}

    # Liste pour stocker les éléments non trouvés
    not_found = []

    for item in data1:
        logical_id = item[" logical_identifier"].strip()
        if logical_id not in data2_ids:
            new_item = {
                "@id": title_to_id(item["title"]),
                "rdfs:label": item["title"],
                "rdfs:comment": "",
                "skos:altLabel": [],
                "skos:exactMatch": [logical_id],
                "dcterms:isPartOf": [],
                "dcterms:hasPart": []
            }
            not_found.append(new_item)

    return not_found

# Extraire les éléments non trouvés dans le fichier 2
not_found = compare_and_extract(data1, data2)

# Fusionner les données
merged_data = data2 + not_found

# Sauvegarder les données fusionnées dans un nouveau fichier JSON
output_path = '/Users/ldebisschop/PycharmProjects/FacilityList/data/obs-facilities_vocabulary/obsfacility-vocabulary.json'
with open(output_path, 'w') as f:
    json.dump(merged_data, f, ensure_ascii=False, indent=4)

