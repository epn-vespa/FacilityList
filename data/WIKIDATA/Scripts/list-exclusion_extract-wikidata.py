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
"""

where = """
 WHERE 
 {      
  {?item wdt:P31/wdt:P279*  wd:Q752783 .} # human spaceflight
  UNION {?item  wdt:P31/wdt:P279* wd:Q149918 .} # communications satellite
  UNION {?item  wdt:P31/wdt:P279*  wd:Q113255208 .} # spacecraft series
  UNION {?item  wdt:P31/wdt:P279* wd:Q209363 .} # weather satellite 
  UNION {?item  wdt:P31/wdt:P279*  wd:Q466421 .} # reconnaissance satellite
  UNION {?item  wdt:P31/wdt:P279*  wd:Q2741214 .} # KH-7 Gambit
  UNION {?item  wdt:P31/wdt:P279*  wd:Q973887 .} # military satellite 
  UNION {?item  wdt:P31/wdt:P279*  wd:Q854845 .} # Earth observation satellite

SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
"""
def page(page, page_size):
    return """
    GROUP BY ?item ?itemLabel 
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
page_size = 1000

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
        e['itemLabel'] = e['itemLabel'].replace(' ', '-')
        e['itemLabel'] = e['itemLabel'].lower()

    r.extend(new_elements)

print("successfully retrieved " + str(len(r)) + " results")

with open("list-exclusion_extract-wikidata.json", 'w', encoding='utf-8') as fout:
    fout.write(json.dumps(r, ensure_ascii=False, indent=4))
