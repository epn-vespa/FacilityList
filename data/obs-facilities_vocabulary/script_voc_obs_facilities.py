import json

with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/WIKIDATA/scripts/extract_wikidata.json') as f:
    wikidata_list = json.load(f)

output_list = []

def transform_list(wikidata_list):
    for item in wikidata_list:
        transformed_item = {
            "@id": item["itemLabel-lower"],
            "rdfs:label": item["itemLabel"],
            "rdfs:comment": item.get("itemDescription", ""),
            "skos:sameAs": [item["aliases"].split('|')] if item["aliases"] else [],
            "skos:exactMatch": [
            item["item"]
             ]
        }
        # Conditionally add URLs to skos:exactMatch
        if item.get("all_Minor_Planet_Center_observatory_ID"):
            transformed_item["skos:exactMatch"].append(
                f"https://minorplanetcenter.net/iau/lists/ObsCodesF.html#{item['all_Minor_Planet_Center_observatory_ID'].replace(' ', '_')}"
            )
        if item.get("all_NSSDCA_ID"):
            transformed_item["skos:exactMatch"].append(
                f"https://nssdc.gsfc.nasa.gov/nmc/spacecraft/display.action?id={item['all_NSSDCA_ID'].replace(' ', '_')}"
            )
        if item.get("all_NAIF_ID"):
            transformed_item["skos:exactMatch"].append(
                f"https://naif.jpl.nasa.gov/pub/naif/toolkit_docs/C/req/naif_ids.html#Spacecraft{item['all_NAIF_ID'].replace(' ', '_')}"
            )

        # Append the transformed item to the output list
        output_list.append(transformed_item)

    return output_list


                #item["all_COSPAR_ID"],
                #item["all_NAIF_ID"],
                #item["all_NSSDCA_ID"],
                #item["all_Minor_Planet_Center_observatory_ID"],
                #item["all_instance_of"],
                #item["all_part_of"]



# Call the function to transform the list
result_list = transform_list(wikidata_list)

# Write the result to a JSON file
with open('obs-facilities_vocabulary.json', 'w',encoding='utf-8') as fout:
    fout.write(json.dumps(result_list,ensure_ascii=False, indent=4))
