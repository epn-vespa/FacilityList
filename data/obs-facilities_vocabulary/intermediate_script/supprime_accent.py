import json
import unidecode
import os
import re

input_file = '/Users/ldebisschop/PycharmProjects/FacilityList/data/obs-facilities_vocabulary/obsfacilities_vocabulary.json'
output_file = 'obsfacilities_vocabulary-clean-accent.json'

try:
    # Load the JSON input file
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Process each item in the JSON
    for item in data:
        # For each specified key, process the value if it exists
        for key in ["@id", "skos:altLabel", "dcterms:isPartOf", "dcterms:hasPart"]:
            if key in item:
                value = item[key]

                # If the value is a string, clean accents, parentheses, &, :, ., and []
                if isinstance(value, str):
                    cleaned_value = unidecode.unidecode(value)
                    cleaned_value = re.sub(r"[()]", "", cleaned_value)  # Remove parentheses
                    cleaned_value = re.sub(r"&", "-", cleaned_value)  # Replace & with -
                    cleaned_value = re.sub(r":", "", cleaned_value)  # Remove :
                    cleaned_value = re.sub(r"\.", "", cleaned_value)  # Remove .
                    cleaned_value = re.sub(r"[\[\]]", "", cleaned_value)  # Remove []
                    item[key] = cleaned_value
                # If the value is a list, process each item
                elif isinstance(value, list):
                    cleaned_list = [
                        re.sub(r"[\[\]]", "", re.sub(r"\.", "", re.sub(r":", "", re.sub(r"&", "-", re.sub(r"[()]", "",
                                                                                                          unidecode.unidecode(
                                                                                                              val))))))
                        if isinstance(val, str) else val
                        for val in value
                    ]
                    item[key] = cleaned_list

    # Write the cleaned data to a new JSON file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    print(f"The cleaned file has been saved as: {os.path.abspath(output_file)}")

except FileNotFoundError:
    print(f"Error: The input file '{input_file}' was not found.")
except json.JSONDecodeError:
    print("Error: The input file is not a valid JSON.")
except Exception as e:
    print(f"An error occurred: {e}")
