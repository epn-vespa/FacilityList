import json

# Load the JSON data
with open('/Users/ldebisschop/PycharmProjects/FacilityList/data/obs-facilities_vocabulary/obsfacility-vocabulary_v_1.2.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Function to convert strings to lowercase and replace spaces with hyphens
def transform_string(input_str):
    return input_str.strip().lower().replace(" ", "-")

# Apply transformation to 'dcterms:isPartOf' and 'dcterms:hasPart' fields
for item in data:
    if "dcterms:isPartOf" in item:
        item["dcterms:isPartOf"] = [transform_string(partof) for partof in item["dcterms:isPartOf"]]
    if "dcterms:hasPart" in item:
        item["dcterms:hasPart"] = [transform_string(haspart) for haspart in item["dcterms:hasPart"]]

# Save the transformed data back to a JSON file
with open('obsfacilities_vocabulary.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=4)

print("Transformation completed. Results saved to obsfacilities_voc_step_2_clean_transformed.json")
