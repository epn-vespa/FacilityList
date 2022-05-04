# pip install sparqlwrapper
# https://rdflib.github.io/sparqlwrapper/
import json
import pywikibot
#!/usr/bin/python
# -*- coding: utf-8 -*-

# Pywikibot will automatically set the user-agent to include your username.
# To customise the user-agent see
# https://www.mediawiki.org/wiki/Manual:Pywikibot/User-agent

import pywikibot
from pywikibot.pagegenerators import WikidataSPARQLPageGenerator
from pywikibot.bot import SingleSiteBot


class WikidataQueryBot(SingleSiteBot):
    """
    Basic bot to show wikidata queries.

    See https://www.mediawiki.org/wiki/Special:MyLanguage/Manual:Pywikibot
    for more information.
    """

    def __init__(self, generator, **kwargs):
        """
        Initializer.

        @param generator: the page generator that determines on which pages
            to print
        @type generator: generator
        """
        super(WikidataQueryBot, self).__init__(**kwargs)
        self.generator = generator

    def treat(self, page):
        print(page)


if __name__ == '__main__':
    query = """PREFIX schema: <http://schema.org/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#> 
PREFIX wikibase: <http://wikiba.se/ontology#>
PREFIX bd: <http://www.bigdata.com/rdf#>

SELECT  
   ?item     
   ?itemLabel
  (GROUP_CONCAT(DISTINCT ?Unified_Astro_Thesaurus_ID; SEPARATOR="|") AS ?all_Unified_Astro_Thesaurus_ID)
  (GROUP_CONCAT(DISTINCT ?COSPAR_ID; SEPARATOR="|") AS ?all_COSPAR_ID)
  (GROUP_CONCAT(DISTINCT ?NAIF_ID; SEPARATOR="|") AS ?all_NAIF_ID)
  (GROUP_CONCAT(DISTINCT ?NSSDCA_ID; SEPARATOR="|") AS ?all_NSSDCA_ID)
  (GROUP_CONCAT(DISTINCT ?Minor_Planet_Center_observatory_ID; SEPARATOR="|") AS ?all_Minor_Planet_Center_observatory_ID)
  (GROUP_CONCAT(DISTINCT ?alias; SEPARATOR="|") AS ?aliases)
  
 WHERE 
 {       
   ?item p:P31 ?stat . 
  #item instance of
  {?stat ps:P31 wd:Q148578 .}  # space observatory
  UNION {?stat ps:P31 wd:Q40218 .} # spacecraft
  UNION {?stat ps:P31 wd:Q1254933 .} # astronomical observatory
  UNION {?stat ps:P31 wd:Q26540 .} # artificial satellite 
  UNION {?stat ps:P31 wd:Q697175 .} # Launch vehicle
  UNION {?stat ps:P31 wd:Q349772 .} # radio interferometer
  UNION {?stat ps:P31 wd:Q2098169 .} # planetary probe
  UNION {?stat ps:P31 wd:Q928667 .} # orbiter 
  UNION {?stat ps:P31 wd:Q26529 .} # space probe
  UNION {?stat ps:P31 wd:Q752783 .} # human spacefligh
  UNION {?stat ps:P31 wd:Q2133344 .} # space mission
  UNION {?stat ps:P31 wd:Q5916 .} # spaceflight
  UNION {?stat ps:P31 wd:Q62832 .} # observatory
  UNION {?stat ps:P31 wd:Q35273 .} # optical telescope
  UNION {?stat ps:P31 wd:Q854845 .} # Earth observation satellite
  UNION {?stat ps:P31 wd:Q763288 .} # lander  
  UNION {?stat ps:P31 wd:Q15078724 .} # expendable launch vehicle
  UNION {?stat ps:P31 wd:Q389459 .} # Mars rover
  UNION {?stat ps:P31 wd:Q1580082 .} # small satellite
  UNION {?stat ps:P31 wd:Q209363 .} # weather satellite
  UNION {?stat ps:P31 wd:Q466421 .} # reconnaissance satellite
  #item has part(s) of the class
  UNION {?item wdt:P2670 wd:Q148578 .} # space observatory
  UNION {?item wdt:P2670 wd:Q40218 .} # spacecraft
  UNION {?item wdt:P2670 wd:Q1254933 .} # astronomical observatory
  UNION {?item wdt:P2670 wd:Q26540 .} # artificial satellite 
  UNION {?item wdt:P2670 wd:Q697175 .} # Launch vehicle
  UNION {?item wdt:P2670 wd:Q349772 .} # radio interferometer
  UNION {?item wdt:P2670 wd:Q2098169 .} # planetary probe
  UNION {?item wdt:P2670 wd:Q928667 .} # orbiter 
  UNION {?item wdt:P2670 wd:Q26529 .} # space probe
  UNION {?item wdt:P2670 wd:Q752783 .} # human spacefligh
  UNION {?item wdt:P2670 wd:Q2133344 .} # space mission
  UNION {?item wdt:P2670 wd:Q5916 .} # spaceflight
  UNION {?item wdt:P2670 wd:Q62832 .} # observatory
  UNION {?item wdt:P2670 wd:Q35273 .} # optical telescope
  UNION {?item wdt:P2670 wd:Q854845 .} # Earth observation satellite
  UNION {?item wdt:P2670 wd:Q763288 .} # lander
  UNION {?item wdt:P2670 wd:Q15078724 .} # expendable launch vehicle
  UNION {?item wdt:P2670 wd:Q389459 .} # Mars rover
  UNION {?item wdt:P2670 wd:Q1580082 .} # small satellite
  UNION {?item wdt:P2670 wd:Q209363 .} # weather satellite
  UNION {?stat wdt:P2670 wd:Q466421 .} # reconnaissance satellite
   
  OPTIONAL {?item wdt:P4466 ?Unified_Astro_Thesaurus_ID .}
  OPTIONAL {?item wdt:P247 ?COSPAR_ID .}    
  OPTIONAL {?item wdt:P8913 ?NSSDCA_ID .}
  OPTIONAL {?item wdt:P2956 ?NAIF_ID .}
  OPTIONAL {?item wdt:P717 ?Minor_Planet_Center_observatory_ID .}
  OPTIONAL {?item skos:altLabel ?alias .}
   
   
 
   SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
 } 
 GROUP BY ?item ?itemLabel"""
    site = pywikibot.Site()
    gen = WikidataSPARQLPageGenerator(query, site=site.data_repository(),
                                      endpoint='https://query.wikidata.org/sparql')
    bot = WikidataQueryBot(gen, site=site)
    bot.run()
#with open("list_observatories_spacecrafts1.json", 'w') as fout:
#    fout.write(json.dumps(results, indent=4))

 