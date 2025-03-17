import json
import re

with open('/Users/ldebisschop/PycharmProjects/FacilityList/data/obs-facilities_vocabulary/added-spase-id.json', 'r') as f2, open('/Users/ldebisschop/PycharmProjects/FacilityList/data/SPASE/scripts/spase1.json', 'r') as f1:
    data1 = json.load(f1)
    data2 = json.load(f2)

# Fonction pour transformer ResourceName en @id
def resource_name_to_id(resource_name):
    return re.sub(r'\s+', ',', resource_name.lower())


# Fonction pour comparer les deux fichiers et ressortir les items non trouvés
def compare_and_extract(data1, data2):
    # Créer un ensemble des IDs du fichier 2 pour un accès rapide
    data2_ids = {item["@id"] for item in data2}

    # Liste pour stocker les items non trouvés
    not_found = []

    for item in data1:
        resource_id = resource_name_to_id(item["ResourceName"])

        if resource_id not in data2_ids:
            new_item = {
                "@id": resource_id,
                "rdfs:label": item["ResourceName"],
                "rdfs:comment": item.get("Description", ""),
                "skos:sameAs": item.get("AlternateName", []),
                "skos:exactMatch": [item["ResourceID"]]
            }
            not_found.append(new_item)

    return not_found


# Charger les deux fichiers JSON


# Comparer les fichiers et extraire les items non trouvés
result = compare_and_extract(data1, data2)

# Sauvegarder le résultat dans un nouveau fichier JSON
with open('not_found.json', 'w') as f:
    json.dump(result, f, indent=4)

print("Comparison completed. Results saved to not_found.json")
