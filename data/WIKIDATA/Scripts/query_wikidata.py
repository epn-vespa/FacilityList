# pip install sparqlwrapper
# https://rdflib.github.io/sparqlwrapper/
import json
import sys
from SPARQLWrapper import SPARQLWrapper, JSON

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
  (GROUP_CONCAT(DISTINCT ?alias; SEPARATOR="|") AS ?aliases)
  (GROUP_CONCAT(DISTINCT ?instance_ofName; SEPARATOR="|") AS ?all_instance_of)
  #(GROUP_CONCAT(DISTINCT ?country; SEPARATOR="|") AS ?countries)
  #(GROUP_CONCAT(DISTINCT ?located; SEPARATOR="|") AS ?all_located)
  #(GROUP_CONCAT(DISTINCT ?coordinate_location; SEPARATOR="|") AS ?all_coordinate_location)
  (GROUP_CONCAT(DISTINCT ?has_partName; SEPARATOR="|") AS ?all_has_part)
  (GROUP_CONCAT(DISTINCT ?part_ofName; SEPARATOR="|") AS ?all_part_of)
"""

where = """
 WHERE 
 {      
  {?item wdt:P31/wdt:P279*  wd:Q40218 .} # spacecraft
  UNION {?item wdt:P31/wdt:P279* wd:Q5916 .} # spaceflight
  UNION {?item  wdt:P31/wdt:P279* wd:Q62832 .} # observatory
  UNION {?item  wdt:P31/wdt:P279*  wd:Q697175 .} # Launch vehicle
  UNION {?item  wdt:P31  wd:Q18812508 .} # space station module 
  UNION {?item  wdt:P31  wd:Q797476 .} # rocket launch
  UNION {?item  wdt:P31  wd:Q35273 .} # optical telescope
  UNION {?item  wdt:P31  wd:Q35221 .} # reflecting telescope
  UNION {?item  wdt:P31  wd:Q3370723 .} # infrared telescope
  UNION {?item  wdt:P31  wd:Q184356 .} # radiotelescope
  UNION {?item  wdt:P31  wd:Q1369318 .} # X-ray telescope
  UNION {?item  wdt:P31  wd:Q148578 .} # Space telescope
  UNION {?item  wdt:P31  wd:Q26529 .} # space probe
  UNION {?item  wdt:P31  wd:Q1062138 .} # Ritchey–Chrétien telescope
  #{?item wdt:P31/ wdt:P279*  wd:Q117273481.} # observation facility
  

  
  #instruments
  #UNION {?item  wdt:P31/wdt:P279*  wd:Q751997 .} # astronomical instrument
  #UNION {?item  wdt:P31  wd:Q550089 .} # astronomical survey
  #UNION {?item  wdt:P31  wd:Q100349043 .} # space instrument 
  
  OPTIONAL {?item wdt:P247 ?COSPAR_ID .}    
  OPTIONAL {?item wdt:P8913 ?NSSDCA_ID .}
  OPTIONAL {?item wdt:P2956 ?NAIF_ID .}
  OPTIONAL {?item wdt:P717 ?Minor_Planet_Center_observatory_ID .}
  OPTIONAL {?item skos:altLabel ?alias .}
  OPTIONAL {?item wdt:P31 ?instance_of .}
  #OPTIONAL {?item wdt:P17 ?country .}
  #OPTIONAL {?item wdt:P131 ?located .}
  #OPTIONAL {?item wdt:P625 ?coordinate_location .}
  
  OPTIONAL {
  ?item wdt:P31 ?instance_of .
  ?instance_of rdfs:label ?instance_ofName .
  Filter((LANG(?instance_ofName)) = "en")
  }
  OPTIONAL {
  ?item wdt:P527 ?has_part .
  ?has_part rdfs:label ?has_partName .
  Filter((LANG(?has_partName)) = "en")
  }
  OPTIONAL {
  ?item wdt:P361 ?part_of .
  ?part_of rdfs:label ?part_ofName .
  Filter((LANG(?part_ofName)) = "en")
  }

SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
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
    user_agent = "Laura.debisschop@obspm.fr - Observatoire de Paris - Python/%s.%s" % (
        sys.version_info[0], sys.version_info[1])
    # TODO adjust user agent; see https://w.wiki/CX6
    sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    return sparql.query().convert()


# assemble a query for knowing the number of results
query_count = query_prefix + select_count + where
query_count_results = get_results(endpoint_url, query_count)
results_count = query_count_results["results"]["bindings"][0]["count"]["value"]

print("response contains " + results_count + " results")

# test
# test = True
# page_size=1

# or not test
test = False
page_size = 2000

print("using page_size = " + str(page_size))
r = []
# for each page that we need to query
for i in range(1 if test == True else (int(results_count) // page_size) + 1):
    print("requesting page " + str(i))
    # assemble a query for knowing the results of the i-th page
    query_page = query_prefix + select_main + where + page(i, page_size)
    # print(query_page)

    query_page_result = get_results(endpoint_url, query_page)
    bindings = query_page_result["results"]["bindings"]
    new_elements = [{k: b[k]["value"] for k in b} for b in bindings]

    # remplacer les espaces par des tirets dans itemLabel
    for e in new_elements:
        e['itemLabel'] = e['itemLabel'].lower().replace(' ', '-')
        #e['itemLabel-lower'] = e['itemLabel'].lower().replace(' ', '-')
        e['aliases'] = e['aliases'].lower().replace(' ', '-')
        #e['aliases-lower'] = e['aliases'].lower().replace(' ', '-')
        # e['all_has_part'] = e['all_has_part'].lower().replace(' ', '-')
        # e['all_part_of'] = e['all_part_of'].lower().replace(' ', '-')

    r.extend(new_elements)

print("successfully retrieved " + str(len(r)) + " results")

with open("raw_extract_wikidata.json", 'w', encoding='utf-8') as fout:
    fout.write(json.dumps(r, ensure_ascii=False, indent=4))

# Filtre exclusion item
data1 = r
#with open("raw_extract_wikidata.json", 'r', encoding='utf-8') as file:
#  data1 = json.load(file)

with open("list-exclusion_extract-wikidata.json", 'r', encoding='utf-8') as file:
    data2 = json.load(file)

elements_a_exclure = [data2]
# Nouvelle liste sans les éléments à exclure
elements_a_exclure = [element['item'] for element in data2]

# Nouvelle liste sans les éléments à exclure
nouvelle_liste = [element for element in data1 if element['item'] not in elements_a_exclure]

# Écrire la nouvelle liste dans un nouveau fichier
with open("extract_wikidata.json", 'w', encoding='utf-8') as file:
    json.dump(nouvelle_liste, file, ensure_ascii=False, indent=4)

print(f"Nombre d'items dans le fichier extract_wikidata.json : {len(nouvelle_liste)}")