import json
import sys
from SPARQLWrapper import SPARQLWrapper, JSON
import ssl
import urllib.request
import certifi
import time
import datetime

endpoint_url = "https://query.wikidata.org/sparql"

query_prefix = """
PREFIX schema: <http://schema.org/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#> 
PREFIX wikibase: <http://wikiba.se/ontology#>
PREFIX bd: <http://www.bigdata.com/rdf#>
"""

select_count = """
SELECT  
     ( COUNT ( DISTINCT ?item ) as ?count )
"""
select_main = """
SELECT
  ?item     
  ?itemLabel
  ?itemDescription
  
  (GROUP_CONCAT(DISTINCT ?COSPAR_ID; SEPARATOR="|") AS ?all_COSPAR_ID)
  (GROUP_CONCAT(DISTINCT ?NAIF_ID; SEPARATOR="|") AS ?all_NAIF_ID)
  (GROUP_CONCAT(DISTINCT ?NSSDCA_ID; SEPARATOR="|") AS ?all_NSSDCA_ID)
  (GROUP_CONCAT(DISTINCT ?Minor_Planet_Center_observatory_ID; SEPARATOR="|") AS ?all_Minor_Planet_Center_observatory_ID)
  (GROUP_CONCAT(DISTINCT CONCAT(?alias, " @", LANG(?alias)); SEPARATOR="|") AS ?aliases_with_lang)
  (GROUP_CONCAT(DISTINCT ?instance_ofName; SEPARATOR="|") AS ?all_instance_of)
  (GROUP_CONCAT(DISTINCT CONCAT(?has_partName," @", LANG(?has_partName)); SEPARATOR="|") AS ?all_has_part)
  (GROUP_CONCAT(DISTINCT CONCAT(?part_ofName," @",LANG(?part_ofName)); SEPARATOR="|") AS ?all_part_of)
"""

where = """
WHERE 
{      
  {?item wdt:P31/wdt:P279*  wd:Q40218 .} # spacecraft
  UNION {?item wdt:P31/wdt:P279* wd:Q5916 .} # spaceflight
  UNION {?item  wdt:P31/wdt:P279* wd:Q62832 .} # observatory
  # ... autres unions ...

  OPTIONAL {?item wdt:P247 ?COSPAR_ID .}    
  OPTIONAL {?item wdt:P8913 ?NSSDCA_ID .}
  OPTIONAL {?item wdt:P2956 ?NAIF_ID .}
  OPTIONAL {?item wdt:P717 ?Minor_Planet_Center_observatory_ID .}
  OPTIONAL {?item skos:altLabel ?alias .}
  OPTIONAL {?item wdt:P31 ?instance_of .}

  OPTIONAL {
    ?item wdt:P31 ?instance_of .
    ?instance_of rdfs:label ?instance_ofName .
    FILTER((LANG(?instance_ofName)) = "en")
  }
  OPTIONAL {
    ?item wdt:P527 ?has_part .
    ?has_part rdfs:label ?has_partName .
    FILTER((LANG(?has_partName)) = "en")
  }
  OPTIONAL {
    ?item wdt:P361 ?part_of .
    ?part_of rdfs:label ?part_ofName .
    FILTER((LANG(?part_ofName)) = "en")
  }

  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
"""


def page(page, page_size):
    return """
    GROUP BY ?item ?itemLabel ?itemDescription ?has_partLabel ?part_ofLabel
    ORDER BY ?item
    OFFSET {}
    LIMIT {}
    """.format(page * page_size, page_size)


def get_results(endpoint_url, query):
    # We should update this per user:
    user_agent = "Laura.debisschop@obspm.fr - Observatoire de Paris - Python/%s.%s" % (
        sys.version_info[0], sys.version_info[1])
    sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    # Use certifi certificates
    context = ssl.create_default_context(cafile=certifi.where())
    sparql.urlopener = lambda request: urllib.request.urlopen(request, context=context)
    return sparql.query().convert()


# Assemble a query for knowing the number of results
query_count = query_prefix + select_count + where
query_count_results = get_results(endpoint_url, query_count)
results_count = query_count_results["results"]["bindings"][0]["count"]["value"]

print("Response contains " + results_count + " results")

test = False
page_size = 1000  # Réduire la taille de la page pour alléger la requête

print("Using page_size = " + str(page_size))
r = []
for i in range(1 if test else (int(results_count) // page_size) + 1):
    print("Requesting page " + str(i))
    query_page = query_prefix + select_main + where + page(i, page_size)
    try:
        query_page_result = get_results(endpoint_url, query_page)
    except Exception as e:
        print(e)
        print(f"Erreur lors de la récupération de la page {i}, nouvelle tentative...")  # Ce n'est pas une nouvelle tentative, c'est juste passer à la page suivante...
        time.sleep(5)
        continue  # Essayer la page suivante
    time.sleep(10)

    bindings = query_page_result["results"]["bindings"]
    new_elements = [{k: b[k]["value"] for k in b} for b in bindings]

    for e in new_elements:
        # Get item label with language if it exists, otherwise set as empty string
        item_label = e.get('itemLabel', "")

        # Check if item_label is not empty before transforming it
        e['itemLabel_lower'] = item_label.lower().replace(' ', '-') if item_label else ""

        # Convert aliases with language to lowercase if available
        e['aliases_with_lang'] = e.get('aliases_with_lang', "").lower()

    r.extend(new_elements)

print("Successfully retrieved " + str(len(r)) + " results")

today = datetime.now().strftime("%Y%m%d")

with open(f"raw-extract-wikidata-{today}.json", 'w', encoding='utf-8') as fout:
    fout.write(json.dumps(r, ensure_ascii=False, indent=4))

data1 = r
with open("list-exclusion_extract-wikidata.json", 'r', encoding='utf-8') as file:
    data2 = json.load(file)

elements_a_exclure = [element['item'] for element in data2]
nouvelle_liste = [element for element in data1 if element['item'] not in elements_a_exclure]

with open(f"extract_wikidata-{today}.json", 'w', encoding='utf-8') as file:
    json.dump(nouvelle_liste, file, ensure_ascii=False, indent=4)

print(f"Nombre d'items dans le fichier extract_wikidata.json : {len(nouvelle_liste)}")
