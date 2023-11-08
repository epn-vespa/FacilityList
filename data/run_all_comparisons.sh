#!/bin/bash

workdir=$(dirname $(realpath $0))
cd $workdir
results_count_output_file=results.out

#### AAS
(
    cd AAS/scripts
    python3 compare_list_aas-wiki.py $results_count_output_file
)
#### ADS
(
    cd ADS/scripts
    python3 compare_list_ads-wiki.py $results_count_output_file
)

#### Astroweb
(
   cd Astroweb/scripts
  python3 compare_list_astroweb-wiki.py $results_count_output_file
)

#### IAU-MPC
(
    cd IAU-MPC/scripts
    python3 compare_list_iau-wiki.py $results_count_output_file
)

#### IRAF
(
    cd IRAF/scripts
    python3 compare_list_iraf-wiki.py $results_count_output_file
)

#### NAIF
(
    cd NAIF/scripts
    python3 compare_list_naif-wiki.py $results_count_output_file
)

#### NSSDC
(
    cd NSSDC/scripts
    python3 compare_nssdc-wiki.py $results_count_output_file
)


#### observatories
(
    cd observatories/scripts
    python3 compare_list_observatories-wiki.py $results_count_output_file
)

#### PDS
#(
#    cd PDS/scripts
#    python3 compare_list_pds-wiki.py $results_count_output_file
#)

#### SANA
#(
#    cd SANA/scripts
#    python3 compare_list_sana-wiki.py $results_count_output_file
#)

#### SPASE
#(
#    cd SPASE/scripts
#    python3 compare_list_spase-wiki.py $results_count_output_file
#)

#### VESPA
(
    cd VESPA/scripts
    python3 compare_list_vespa-wiki.py $results_count_output_file
)

#### WISEREP
(
    cd WISEREP/scripts
    python3 compare_wiserep-wiki.py $results_count_output_file
)

#### WMO
#(
#    cd WMO/scripts
#    python3 compare_list_wmo-wiki.py $results_count_output_file
#)


#### XEPHEM-SITES

#(
#    cd XEPHEM-SITES/scripts
#    #python3 compare_xephem_wiki.py $results_count_output_file
#)


