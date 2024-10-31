import json
import re


with open('/Users/ldebisschop/PycharmProjects/FacilityList/data/SPASE/scripts/added-spase-id_manually1.json', 'r') as f2:
    data2 = json.load(f2)
with open('/Users/ldebisschop/PycharmProjects/FacilityList/data/SPASE/scripts/spase_data_V_1.0.json', 'r') as f1:
    data1 = json.load(f1)

# Function to transform ResourceName into @id
def resource_name_to_id(resource_name):
    return re.sub(r'\s+', '-', resource_name.lower())

# Function to remove non-ASCII characters
def remove_non_ascii(text):
    return re.sub(r'[^\x00-\x7F]+', '', text)

# Function to clean skos:altLabel list
def clean_alt_labels(alt_labels):
    return [remove_non_ascii(label) for label in alt_labels]

# Function to compare and extract data
def compare_and_extract(data1, data2):
    data2_ids = {item["@id"] for item in data2}

    # List to store items not found
    not_found = []

    for item in data1:
        resource_id = resource_name_to_id(item["ResourceName"])

        if resource_id not in data2_ids:
            new_item = {
                "@id": resource_id,
                "rdfs:label": item["ResourceName"],
                "rdfs:comment": item.get("Description", ""),
                "skos:altLabel": clean_alt_labels(item.get("AlternateName", [])),
                "skos:exactMatch": [item["ResourceID"]],
                "dcterms:isPartOf": [],
                "dcterms:hasPart": []
            }
            not_found.append(new_item)

    return not_found

# Process the data1 items to handle skos:exactMatch and dcterms:isPartOf fields
for item in data1:
    if "skos:exactMatch" in item:
        # Collect terms containing "Observatory"
        observatory_terms = [match for match in item["skos:exactMatch"] if "spase://SMWG/Observatory/" in match]

        # If more than one term contains "Observatory", process each one
        if len(observatory_terms) > 1:
            for match in observatory_terms:
                parts = match.split("/Observatory/")[1].split("/")
                term = parts[0]

                if "dcterms:isPartOf" in item:
                    if term not in item["dcterms:isPartOf"]:
                        item["dcterms:isPartOf"].append(term)
                else:
                    item["dcterms:isPartOf"] = [term]
        elif len(observatory_terms) == 1:
            match = observatory_terms[0]
            parts = match.split("/Observatory/")[1].split("/")
            term = parts[0]
            if "dcterms:isPartOf" in item:
                if term not in item["dcterms:isPartOf"]:
                    item["dcterms:isPartOf"].append(term)
            else:
                item["dcterms:isPartOf"] = [term]

# Extract items not found in data2
not_found = compare_and_extract(data1, data2)

# Merge the data
merged_data = data2 + not_found

# Save the merged data
with open('/Users/ldebisschop/PycharmProjects/FacilityList/data/obs-facilities_vocabulary/obsfacility-vocabulary_v_1.1.json', 'w') as f:
    json.dump(merged_data, f, ensure_ascii=False, indent=4)

print("Merging completed. Results saved to obsfacility-vocabulary_v_1.1.json")
