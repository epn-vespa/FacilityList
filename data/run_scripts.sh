#!/bin/bash

cd $(dirname $(realpath $0))

# 1 : lancer la récupération des données wikidata
(
cd WIKIDATA
# python3 query_wikidata.py
)

# 2 : lancer le script de comparaison NSSDC
(
cd NSSDC
python3 compare_NSSDC_wiki.py
)


