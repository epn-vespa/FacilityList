import json
import unicodedata
with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/WIKIDATA/scripts/extract_wikidata.json') as f:
    wikidata_list = json.load(f)




# Function to remove accents from strings
def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return nfkd_form.encode('ASCII', 'ignore').decode('ASCII')


def transform_list(wikidata_list):
    output_list = []
    for item in wikidata_list:
        transformed_item = {
             "@id": remove_accents(item["itemLabel-lower"]),
            "rdfs:label": remove_accents(item["itemLabel"]),
            "rdfs:comment": remove_accents(item.get("itemDescription", "")),
            "skos:sameAs": [remove_accents(alias) for alias in item["aliases"].split('|')] if item.get("aliases") else [],
            "skos:exactMatch": [
                item["item"]
             ]
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


result_list = transform_list(wikidata_list)

# Write the result to a JSON file
with open('obs-facilities_vocabulary.json', 'w',encoding='utf-8') as fout:
    fout.write(json.dumps(result_list,ensure_ascii=False, indent=4))
