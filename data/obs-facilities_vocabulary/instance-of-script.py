import json

# Load your data
with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/WIKIDATA/scripts/extract_wikidata.json') as f:
    wikidata_list = json.load(f)

# Function to reformat 'instance_of' values by removing pipes, adding quotes, and removing duplicates
def reformat_and_deduplicate_instance_of(wikidata_list):
    unique_instance_of = set()  # Use a set to avoid duplicates
    for e in wikidata_list:
        if 'all_instance_of' in e:
            # Split the string by '|', strip spaces, add quotes, and add to the set
            instances = e['all_instance_of'].split('|')
            for instance in instances:
                formatted_instance = '"{}"'.format(instance.strip())
                unique_instance_of.add(formatted_instance)
    return list(unique_instance_of)  # Convert set to list to maintain JSON serialization compatibility

# Reformat 'instance_of' values and remove duplicates
unique_reformatted_instance_of_values = reformat_and_deduplicate_instance_of(wikidata_list)


# Write the unique values to a JSON file
with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/obs-facilities_vocabulary/all_instance_of.json', 'w', encoding='utf-8')  as fout:
    json.dump(unique_reformatted_instance_of_values, fout, ensure_ascii=False, indent=4)
