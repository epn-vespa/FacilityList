#!/bin/bash

cd $(dirname $(realpath $0))

#### update wikidata list
(
    cd WIKIDATA/scripts
     python3 query_wikidata.py
)

#### AAS
(
    cd AAS/scripts
    python3 extract_data_aas.py
)

#### ADS
#(
#    cd ADS/scripts
#    python3 extract_data_ads.py
#)

#### Astroweb
(
   cd Astroweb/scripts
   python3 extract_data_astroweb.py
)

#### IAU-MPC
(
    cd IAU-MPC/scripts
    python3 extract_data_iau-mpc.py
)

#### IRAF
(
    cd IRAF/scripts
    python3 extract_data_iraf.py
)

#### NAIF
#(
#    cd NAIF/scripts
#    python3 extract_data_naif.py
#)

#### NSSDC
#(
#    cd NSSDC/scripts
#    python3 extract_data_nssdc.py
#)


#### PDS
#(
#    cd PDS/scripts
#    python3 extract_data_pds.py
#)

#### SANA
#(
#    cd SANA/scripts
#    python3 extract_data_sana.py
#)

#### SPASE
#(
#    cd SPASE/scripts
#    python3 Extract_data_spase.py
#)

#### VESPA
#(
#    cd VESPA/scripts
#    python3 .py
#)

#### WISEREP
#(
#    cd WISEREP/scripts
#    python3 extract_data_wiserep.py
#)

#### WMO
#(
#    cd WMO/scripts
#    python3 extarct_data_wmo.py
#)


#### XEPHEM-SITES

#(
#    cd XEPHEM-SITES/scripts
#    #python3 extract_data_xephem.py
#)