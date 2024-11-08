#!/bin/bash

workdir=$(dirname $(realpath $0))
cd $workdir


#### update wikidata list & filter item which are not an observation facilitity
(
    cd WIKIDATA/scripts
    python3 query_wikidata.py
)
#### Build intermediate Obsfacility vocabulary, step 1
(
    cd obs-facilities-vocabulary
      python3 script_voc_obs_facilities_step1.py
)
#### Add SPASE Vocabulary
(
    cd SPASE/scripts
      python3 compare-spase_obsfacility.py
(
    cd SPASE/scripts
    python3 merge_spase_not_found.py
)

#### Add PDS Vocabulary

(
    cd PDS/scripts
      python3 compare_obsfacilityvoc-pds.py
)

(
    cd PDS/scripts
      python3 merge_pds_not-found.py
)