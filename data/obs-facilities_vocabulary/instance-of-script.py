import json

# Load your data
with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/WIKIDATA/scripts/extract_wikidata.json') as f:
    wikidata_list = json.load(f)

# Function to extract unique 'instance_of' values
def extract_unique_instance_of(wikidata_list):
    unique_instance_of = set()
    for e in wikidata_list:
        if 'all_instance_of' in e:
            unique_instance_of.add(e['all_instance_of'])
    return list(unique_instance_of)  # Convert set to list

# Extract unique 'instance_of' values
unique_instance_of_values = extract_unique_instance_of(wikidata_list)

# Write the unique values to a JSON file
with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/obs-facilities_vocabulary/all_instance_of.json', 'w', encoding='utf-8') as fout:
    json.dump(unique_instance_of_values, fout, ensure_ascii=False, indent=4)
