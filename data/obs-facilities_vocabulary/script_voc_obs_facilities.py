import json
import unicodedata

with open('/Users/ldebisschop/PycharmProjects/FacilityList/data/WIKIDATA/scripts/extract_wikidata_V_1.1.json') as f:
    wikidata_list = json.load(f)

# Function to remove accents from strings

def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return nfkd_form.encode('ASCII', 'ignore').decode('ASCII')

# Function to transform the initial JSON data
def transform_list(wikidata_list):
    output_list = []
    for item in wikidata_list:
        transformed_item = {
            "@id":(item["itemLabel_lower"]),
            "rdfs:label": item["itemLabel"],
            "rdfs:comment": (item.get("itemDescription", "")),
            "skos:altLabel": [(alias) for alias in item["aliases"].split('|')] if item.get("aliases") else [],
            "skos:exactMatch": [
                item["item"]
            ],
            "dcterms:isPartOf": [(partof) for partof in item["all_part_of"].split('|')] if item.get("all_part_of") else [],
            "dcterms:hasPart": [(haspart) for haspart in item["all_has_part"].split('|')] if item.get("all_has_part") else [],
        }
        # Conditionally add URLs to skos:exactMatch
        if item.get("all_Minor_Planet_Center_observatory_ID"):
            transformed_item["skos:exactMatch"].append(
                f"https://minorplanetcenter.net/iau/lists/ObsCodesF.html#{remove_accents(item['all_Minor_Planet_Center_observatory_ID']).replace(' ', '_')}"
            )
        if item.get("all_NSSDCA_ID"):
            transformed_item["skos:exactMatch"].append(
                f"https://nssdc.gsfc.nasa.gov/nmc/spacecraft/display.action?id={remove_accents(item['all_NSSDCA_ID']).replace(' ', '_')}"
            )
        if item.get("all_NAIF_ID"):
            transformed_item["skos:exactMatch"].append(
                f"https://naif.jpl.nasa.gov/pub/naif/toolkit_docs/C/req/naif_ids.html#Spacecraft{remove_accents(item['all_NAIF_ID']).replace(' ', '_')}"
            )

        # Append the transformed item to the output list
        output_list.append(transformed_item)

    return output_list

# Function to clean the lists by removing duplicates and specific unwanted entries
def clean_list(lst):
    # Specific entries to remove
    entries_to_remove = {"--", "    ()", "    ",".    ","  ", " ","", "  \"\"", " ( )","-","----",".----","--"}
    # Use a set to remove duplicates
    cleaned = list(set(lst) - entries_to_remove)
    return cleaned


# Transform the initial JSON data
result_list = transform_list(wikidata_list)

# Write the transformed data to a JSON file
with open('obsfacilities_voc_step_1_ontology.json', 'w', encoding='utf-8') as fout:
    json.dump(result_list, fout, ensure_ascii=False, indent=4)

# Load the JSON file to be cleaned
with open('obsfacilities_voc_step_1_ontology.json', 'r') as f:
    data = json.load(f)

# Clean the 'skos:altLabel' lists in each item
for item in data:
    if "skos:altLabel" in item:
        item["skos:altLabel"] = clean_list(item["skos:altLabel"])

# Save the cleaned data to a new JSON file
with open('obsfacilities_voc_step_2_clean.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=4)

print("Duplication removal and cleaning completed. Results saved to cleaned_obsfacility-final1.json")
