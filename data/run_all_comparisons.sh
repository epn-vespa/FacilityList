#!/bin/bash

cd $(dirname $(realpath $0))

#### AAS
(
    cd AAS/scripts
    python3 compare_list_aas-wiki.py
)
#### ADS
(
    cd ADS/scripts
    python3 compare_list_ads-wiki.py
)

#### Astroweb
(
   cd Astroweb/scripts
  python3 compare_list_astroweb-wiki.py
)

#### IAU-MPC
(
    cd IAU-MPC/scripts
    python3 compare_list_iau-wiki.py
)

#### IRAF
(
    cd IRAF/scripts
    python3 compare_list_iraf-wiki.py
)

#### NAIF
(
    cd NAIF/scripts
    python3 compare_list_naif-wiki.py
)

#### NSSDC
(
    cd NSSDC/scripts
    python3 compare_nssdc-wiki.py
)


#### observatories
(
    cd observatories/scripts
    python3 compare_list_observatories-wiki.py
)

#### PDS
#(
#    cd PDS/scripts
#    python3 compare_list_pds-wiki.py
#)

#### SANA
#(
#    cd SANA/scripts
#    python3 compare_list_sana-wiki.py
#)

#### SPASE
#(
#    cd SPASE/scripts
#    python3 compare_list_spase-wiki.py
#)

#### VESPA
(
    cd VESPA/scripts
    python3 compare_list_vespa-wiki.py
)

#### WISEREP
(
    cd WISEREP/scripts
    python3 compare_wiserep-wiki.py
)

#### WMO
#(
#    cd WMO/scripts
#    python3 compare_list_wmo-wiki.py
#)


#### XEPHEM-SITES

#(
#    cd XEPHEM-SITES/scripts
#    #python3 compare_xephem_wiki.py
#)


