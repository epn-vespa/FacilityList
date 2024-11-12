import json
import re
import unidecode
import os

<<<<<<< Updated upstream
# Fichiers d'entrée et de sortie
input_file = "/Users/ldebisschop/PycharmProjects/FacilityList/data/WIKIDATA/scripts/extract_wikidata-11082024.json"
output_file = "/Users/ldebisschop/PycharmProjects/FacilityList/data/obs-facilities_vocabulary/obsfacilities_vocabulary_step1.json"

# Fonction pour enlever les accents
def remove_accents(input_str):
    return unidecode.unidecode(input_str)

# Fonction pour formater les chaînes
=======
# Input and output files
input_file = "/Users/ldebisschop/PycharmProjects/FacilityList/data/WIKIDATA/scripts/extract_wikidata-11082024.json"
output_file = "/Users/ldebisschop/PycharmProjects/FacilityList/data/obs-facilities_vocabulary/obsfacilities_vocabulary_step1.json"

# Function to remove accents
def remove_accents(input_str):
    return unidecode.unidecode(input_str)

# Function to format strings
>>>>>>> Stashed changes
def format_string(value):
    cleaned_value = unidecode.unidecode(value)
    cleaned_value = re.sub(r"[()]", "", cleaned_value)
    cleaned_value = re.sub(r"&", "-", cleaned_value)
    cleaned_value = re.sub(r":", "", cleaned_value)
    cleaned_value = re.sub(r"\.", "", cleaned_value)
    cleaned_value = re.sub(r"[\[\]]", "", cleaned_value)
    return cleaned_value.strip().lower().replace(" ", "-")

<<<<<<< Updated upstream
# Fonction de transformation et de nettoyage
=======
# Function for transformation and cleaning
>>>>>>> Stashed changes
def transform_and_clean_data(data_list):
    output_list = []
    unwanted_entries = {"--", "    ()", "    ", ".    ", "  ", " ", "", "  \"\"", " ( )", "-", "----", ".----", "--"}

    for item in data_list:
        is_part_of_transformed = [
            part.strip().lower().replace(" ", "-") for part in item.get("all_part_of", "").split('|') if part
        ]
        has_part_transformed = [
            part.strip().lower().replace(" ", "-") for part in item.get("all_has_part", "").split('|') if part
        ]

<<<<<<< Updated upstream
        # Utiliser format_string pour créer l'ID en minuscule, sans accents et avec des tirets
=======
        # Use format_string to create the ID in lowercase, without accents, and with hyphens
>>>>>>> Stashed changes
        transformed_item = {
            "@id": format_string(item.get("itemLabel", "")),
            "rdfs:label": item.get("itemLabel", ""),
            "rdfs:comment": item.get("itemDescription", ""),
            "skos:altLabel": clean_list([alias.strip() for alias in item.get("aliases_with_lang", "").split('|')]),
            "skos:exactMatch": build_skos_exact_match(item),
            "dcterms:isPartOf": clean_list(is_part_of_transformed),
            "dcterms:hasPart": clean_list(has_part_transformed),
        }
        output_list.append(transformed_item)
    return output_list

<<<<<<< Updated upstream
# Fonction pour nettoyer les listes
=======
# Function to clean lists
>>>>>>> Stashed changes
def clean_list(data_list):
    unwanted_entries = {"--", "    ()", "    ", ".    ", "  ", " ", "", "  \"\"", " ( )", "-", "----", ".----", "--"}
    return list(set(data_list) - unwanted_entries)

<<<<<<< Updated upstream
# Fonction pour construire les liens skos:exactMatch
def build_skos_exact_match(item):
    matches = [item["item"]] if "item" in item else []
    if "all_Minor_Planet_Center_observatory_ID" in item:
        matches.append(f"https://minorplanetcenter.net/iau/lists/ObsCodesF.html#{remove_accents(item['all_Minor_Planet_Center_observatory_ID']).replace(' ', '_')}")
    if "all_NSSDCA_ID" in item:
        matches.append(f"https://nssdc.gsfc.nasa.gov/nmc/spacecraft/display.action?id={remove_accents(item['all_NSSDCA_ID']).replace(' ', '_')}")
    if "all_NAIF_ID" in item:
        matches.append(f"https://naif.jpl.nasa.gov/pub/naif/toolkit_docs/C/req/naif_ids.html#Spacecraft{remove_accents(item['all_NAIF_ID']).replace(' ', '_')}")
    return matches

# Lecture, transformation et sauvegarde des données
=======
# Function to build skos:exactMatch links only if they exist
def build_skos_exact_match(item):
    matches = []
    if "item" in item:
        matches.append(item["item"])
    if "all_Minor_Planet_Center_observatory_ID" in item and item["all_Minor_Planet_Center_observatory_ID"].strip():
        matches.append(f"https://minorplanetcenter.net/iau/lists/ObsCodesF.html#{remove_accents(item['all_Minor_Planet_Center_observatory_ID']).replace(' ', '_')}")
    if "all_NSSDCA_ID" in item and item["all_NSSDCA_ID"].strip():
        matches.append(f"https://nssdc.gsfc.nasa.gov/nmc/spacecraft/display.action?id={remove_accents(item['all_NSSDCA_ID']).replace(' ', '_')}")
    if "all_NAIF_ID" in item and item["all_NAIF_ID"].strip():
        matches.append(f"https://naif.jpl.nasa.gov/pub/naif/toolkit_docs/C/req/naif_ids.html#Spacecraft{remove_accents(item['all_NAIF_ID']).replace(' ', '_')}")
    return matches

# Reading, transforming, and saving data
>>>>>>> Stashed changes
try:
    with open(input_file, 'r', encoding='utf-8') as f:
        filtered_data = json.load(f)

    cleaned_data = transform_and_clean_data(filtered_data)

    with open(output_file, 'w', encoding='utf-8') as fout:
        json.dump(cleaned_data, fout, ensure_ascii=False, indent=4)

<<<<<<< Updated upstream
    print(f"Transformation et nettoyage terminés. Résultats sauvegardés dans '{output_file}'")

except FileNotFoundError:
    print(f"Erreur : le fichier '{input_file}' est introuvable.")
except json.JSONDecodeError:
    print("Erreur : le fichier d'entrée n'est pas un JSON valide.")
except Exception as e:
    print(f"Une erreur est survenue : {e}")
=======
    print(f"Transformation and cleaning completed. Results saved in '{output_file}'")

except FileNotFoundError:
    print(f"Error: The file '{input_file}' is not found.")
except json.JSONDecodeError:
    print("Error: The input file is not valid JSON.")
except Exception as e:
    print(f"An error occurred: {e}")
>>>>>>> Stashed changes
