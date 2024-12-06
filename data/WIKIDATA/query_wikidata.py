#!/usr/bin/env python
"""
Initial Processing
==================

Run `initial_query()` to initialize control data file. This file is used to keep track of
the records available on Wikidata and fitting with our query parameters. This file is a
JSON-formatted file that contains:
- the processing date (for subsequent processing, checking for updates in Wikidata)
- the previous processing date (set to null at first run)
- the result count
- the result data as a dictionary {itemURI: {"label": itemLabel, "modified_date": modifiedDate}}

This control file is synced with the exclusion file, so that we do not process entries
that shall be further ignored.

Run `collect_metadata()` to collect the metadata from wikidata records using the
special entity query, which returns the wikidata entity properties as a dictionary (JSON).
We select a limited set of properties and include them into the raw extraction file.

Update Content
==============

Run `update_query()` to retrieve the lastest entities from Wikidata. This creates a new
control file (with current date). This file is also synced with the exclusion list.

Run `collect_metadata()` to collect the metadata from wikidata records using the
special entity query, which returns the wikidata entity properties as a dictionary (JSON).
We select a limited set of properties and include them into the raw extraction file.

"""
import json
import os
import sys
from SPARQLWrapper import SPARQLWrapper, JSON
import ssl
import urllib.request
import certifi
import datetime
import subprocess

endpoint_url = "https://query.wikidata.org/sparql"

query_prefix = """
PREFIX schema: <http://schema.org/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
"""

select_count = """
SELECT  
     ( COUNT ( DISTINCT ?itemURI ) as ?count )
"""

select_main_simple = """
SELECT DISTINCT
  ?itemURI     
  ?itemLabel
  ?modifiedDate
"""

where_simple = """
WHERE
{
  ?itemURI rdfs:label ?itemLabel filter (lang(?itemLabel) = "en") . # get itemLabel only for lang = @en
  ?itemURI schema:dateModified ?modifiedDate . # get last modification date
  
  # Filter on classes:
  {?itemURI wdt:P31/wdt:P279* wd:Q40218 .} # spacecraft
  UNION {?itemURI wdt:P31/wdt:P279* wd:Q5916 .} # spaceflight
  UNION {?itemURI  wdt:P31/wdt:P279* wd:Q62832 .} # observatory
}
"""


def page(page, page_size):
    return """
    ORDER BY ?itemURI
    OFFSET {}
    LIMIT {}
    """.format(page * page_size, page_size)


def get_results(endpoint_url, query):
    # We should update this per user:
    user_agent = "semantics@ivoa.net - PADC/Observatoire de Paris - Python/%s.%s" % (
        sys.version_info[0], sys.version_info[1])
    sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    # Use certifi certificates
    context = ssl.create_default_context(cafile=certifi.where())
    sparql.urlopener = lambda request: urllib.request.urlopen(request, context=context)
    return sparql.query().convert()


def count():
    # Assemble a query for knowing the number of results
    query_count = query_prefix + select_count + where_simple
    query_count_results = get_results(endpoint_url, query_count)
    results_count = query_count_results["results"]["bindings"][0]["count"]["value"]

    print("Response contains " + results_count + " results")
    return results_count


def apply_exclusions(control_data):
    exclusion_file = "list-exclusion_extract-wikidata.json"
    with open("list-exclusion_extract-wikidata.json", 'r', encoding='utf-8') as file:
        exclusion_data = json.load(file)

    excluded_entities = [item['item'] for item in exclusion_data]
    for entity in control_data["results"].keys():
        control_data["results"][entity]["excluded"] = entity in excluded_entities

    write_data("wikidata_entities_control_file", control_data)
    print("Successfully applied " + exclusion_file)


def write_data(file_stem, content):
    today = datetime.datetime.now().strftime("%Y%m%d")
    filename = f"{file_stem}_{today}.json"
    filename_latest = f"{file_stem}_latest.json"

    # write JSON file with control data content
    print(f"Writing control data in: {filename}")
    with open(filename, 'w', encoding='utf-8') as fout:
        fout.write(json.dumps(content, ensure_ascii=False, indent=4))

    # replace latest file with new content
    print(f"Updating latest control data ({filename_latest})")
    subprocess.run(["cp", "-f", filename, filename_latest])


def initial_query(test=False, page_size=1000):

    print("Using page_size = " + str(page_size))
    results_count = count()

    # control_data contains:
    # - the processing date (for subsequent processing, checking for updates in Wikidata)
    # - the result count
    # - the result data as a dictionary {itemURI: {"label": itemLabel, "modified_date": modifiedDate}}
    control_data = dict([
        ("processing_date", datetime.datetime.now(tz=datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")),
        ("previous_date", None),
        ("results_count", results_count),
        ("results", {})
    ])

    for i in range(1 if test else (int(results_count) // page_size) + 1):
        print("Requesting page " + str(i))
        query_page = query_prefix + select_main_simple + where_simple + page(i, page_size)
        try:
            query_page_result = get_results(endpoint_url, query_page)
        except Exception as e:
            print(e)
            print(f"Erreur lors de la récupération de la page {i}, skipping...")
            continue

        bindings = query_page_result["results"]["bindings"]
        print("Found "+ str(len(bindings)))
        for binding in bindings:
            item_uri, item_label, modified_date = [binding[k]["value"] for k in ['itemURI', 'itemLabel', 'modifiedDate']]
            # if there is a duplicate itemURI (should not happen), print out the extra data:
            if item_uri in control_data["results"].keys():
                print(item_uri, control_data["results"][item_uri], item_label, modified_date)
            control_data["results"][item_uri] = dict([
                ("label", item_label),
                ("modified_date", modified_date)
                ])

    print("Successfully retrieved " + str(len(control_data["results"])) + " results")
    apply_exclusions(control_data)

    control_filename_latest = "wikidata_entities_control_file_latest.json"
    if os.path.exists(control_filename_latest):
        # if there is a previous "latest" file, then store the processing_date in new content
        with open(control_filename_latest, 'r') as fl:
            previous_data = json.load(fl)
        control_data["previous_date"] = previous_data["processing_date"]

    write_data("wikidata_entities_control_file", control_data)


def load_wikidata_json(wikidata_item:str):
    """Connect to the https://www.wikidata.org/wiki/Special:EntityData endpoint
    to get the JSON response from the Wikidata item.

    :param wikidata_item: Wikidata item (Qxxxxxxx) to retrieve
    :type wikidata_item: str
    :return: select metadata
    :rtype: dict
    """
    import requests

    wikidata_url_json = f"https://www.wikidata.org/wiki/Special:EntityData/{wikidata_item}.json"
    print(f"Processing {wikidata_item}")
    response = requests.get(wikidata_url_json)
    result = {}
    if response.status_code == 200:
        wikidata_dict = response.json()

        # select the `labels`, `aliases` and `descriptions`
        for item in ["labels", "aliases", "descriptions"]:
            result[item] = wikidata_dict["entities"][wikidata_item][item]

        # select the wikidata properties:
        # P31   instance_of
        # P247  COSPAR_ID
        # P8913 NAIF_ID
        # P2956 ObsCode_MPC_ID
        # P527  has_part
        # P361  part_of
        property_ids = {
            "P31": "instance_of",
            "P247": "COSPAR_ID",
            "P8913": "NAIF_ID",
            "P2956": "ObsCode_MPC_ID",
            "P527": "has_part",
            "P361": "part_of",
        }
        # list the properties actually listed in the entity
        item_properties = wikidata_dict["entities"][wikidata_item]["claims"]

        for property_id in item_properties.keys():
            if property_id in property_ids.keys():
                property_name = property_ids[property_id]
                property_data = item_properties[property_id]
                result[property_name] = []
                for prop in property_data:
                    datatype = prop["mainsnak"]["datatype"]
                    if datatype == "external-id":
                        property_value = prop["mainsnak"]["datavalue"]["value"]
                    elif datatype == "wikibase-item":
                        property_value = prop["mainsnak"]["datavalue"]["value"]["id"]
                    else:
                        print(f"Warning {property_id}({property_name}) not parsed. datatype: {datatype}")
                        property_value = None
                    result[property_name].append(property_value)
    else:
        print(wikidata_item, response.status_code)
    return result


def collect_metadata(control_filename="wikidata_entities_control_file_latest.json", init:bool=False):

    wikidata_content = {}
    # open the latest control file:
    with open(control_filename) as file:
        control_data = json.load(file)
        for itemURI, itemData in control_data["results"].items():
            # for each itemURI, check the modified date with the current date
            modified_date = datetime.datetime.strptime(itemData["modified_date"], "%Y-%m-%dT%H:%M:%SZ")
            if (
                    not control_data["results"][itemURI]["excluded"] or  # skip exluded records
                    init or  # keep all records if init phase
                    (control_data['previous_date'] is None) or  # keep all records if init phase
                    (control_data["results"][itemURI]["modified_date"] > control_data['previous_date'])
            ):
                wikidata_item = itemURI.split("/")[-1]
                wikidata_content[itemURI] = load_wikidata_json(wikidata_item)
    # load previous raw_extract_wikidata
    today = datetime.datetime.now().strftime("%Y%m%d")
    wikidata_content_file = f"raw_extract_wikidata_{today}.json"
    with open(wikidata_content_file, "w") as file:
        json.dump(wikidata_content, file, indent=4)


def update_query(test=False, page_size=100):

    print("Using page_size = " + str(page_size))
    results_count = count()

    control_filename_latest = "wikidata_entities_control_file_latest.json"
    with open(control_filename_latest, 'r', encoding='utf-8') as file:
        control_data = json.load(file)

    previous_itemURI = [item for item in control_data["results"]]


if __name__ == "__main__":
    args = sys.argv[1:]

    init = "-i" in args
    test = "-T" in args
    if init:
        initial_query(test)
        collect_metadata(init=True)
    else:
        update_query()
