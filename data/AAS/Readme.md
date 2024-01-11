# AAS (American Astronomical Society)

The American Astronomical Society (AAS) is dedicated to advancing and disseminating humanity's scientific comprehension of the universe within a diverse and inclusive astronomical community.

More info:
(https://aas.org/about/mission-and-vision-statement)

## Identifiers
 AAS identifiers are available from [AAS web portal, AAS journals section](https://journals.aas.org/facility-keywords/)


## Extract data

To facilitate data extraction, there's a Python script available in the scripts folder named **extract_data_aas.py** 
This script employs web scraping techniques directly from the AAS facility keywords page.

The extracted data is then stored in the **aas.json** file within the data folder (available in xml format too). 
(last updated on 14/12/23) 

## Compare lists

Python scrypt is available **scripts** folder, called **compare_list_aas_wiki.py** file
This script compares terms in the aas.json file with terms in the extract_wikidata.json file, using the fuzzy wuzzy library to establish a correspondence score.
Results are available in the files : 
- **non_trouves_data-aas.json** 
- **tres_certain_data-aas.json**