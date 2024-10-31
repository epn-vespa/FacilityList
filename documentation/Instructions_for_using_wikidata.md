# <span style="color:#2D5EAA">Instructions for using Wikidata</span>
## <span style="color:#2D5EAA">Contents</span>
[TOC]


 ## <span style="color:#2D5EAA">1. Overview of Wikidata: A Collaborative Hub for Structured Knowledge</span>

Wikidata is a collaborative knowledge base that serves as a central hub for structured data on diverse topics. It allows users to contribute and edit information, creating a comprehensive and interconnected database. Wikidata is particularly valuable for its role in supporting projects across the Wikimedia Foundation, providing a shared resource for information retrieval and enrichment.

## <span style="color:#2D5EAA">2. Presentation of a Wikidata item</span>

The Wikidata repository is mainly composed of elements, each of which has a label, a description, and a number of aliases. The elements are uniquely identified by `Q` followed by a number.

`Statements` describe the detailed characteristics of an item and include a property and a value. Properties in Wikidata are defined by a`P` followed by a number (e.g., `P31` corresponds to`instance of`).

Example for Meudon Observatory : 

![](https://mdbook.obspm.fr/uploads/2fa4070a-ac0e-499d-8b34-168a30823d1e.png)
![](https://mdbook.obspm.fr/uploads/3f42cda8-1f1e-4f5b-8123-ad8e07b5d083.png)

We can add a property, for example, to specify the geographical coordinate with values for longitude and latitude.

![](https://mdbook.obspm.fr/uploads/ba132ba1-d7b0-45e1-b622-204351b6e707.png)

Properties can also refer to databases used by libraries and archives. These are `Identifiers`.

![](https://mdbook.obspm.fr/uploads/21a2ab6e-7c5a-49e5-bdf4-e593385212b6.png)



## <span style="color:#2D5EAA"> 3. Extraction datas from Wikidata</span>
### <span style="color:#2D5EAA">3.1 Query SPARQL on Wikidata Query Services</span>
To generate lits on Wikidata, we use SPARQL queries on [Wikidata Query Service](https://query.wikidata.org)
SPARQL is a language for querying knowledge bases.
A triplet can be read as a sentence(ending with a period), with a subject, a predicate and an object.

`SELECT` lists the variables that we want to retrieve.
`WHERE` contain restrictions on these variables, mostly in the form of triplets. 
In the example below: we search to determine all "items" whose `instance of` (P31) is a spacecraft.

### <span style="color:#2D5EAA">3.2 First query</span>
```SQL
SELECT ?item ?itemLabel 
 WHERE 
 {  
  ?item wdt:P31 wd:Q40218 . #spacecraft
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
} 
GROUP BY ?item ?itemLabel
```
For more information: 
https://www.wikidata.org/wiki/Wikidata:SPARQL_tutorial/fr

![](https://mdbook.obspm.fr/uploads/e0eb9cf5-65fd-4a4f-83d2-a05916b5730d.png)

The results are available in the form of table, with as the input which Wikidata identifier and label which corresponds to the name given in Wikidata. 

By Exploring all the elements of each list in Wikidata, we notice that the relationships (predicate) between the subject and object are variable. A non-exhaustive list of predicates used in wikidata for space mission has been established : 

- Space observatory
- Spacecraft
- Astronomical observatory
- Artificial satellite
- Launch vehicle
- Radio interferometer
- Planetary probe
- Orbiter 
- Space probe
- Human spaceflight
- Space mission
- Spaceflight
- Observatory
- Optical telescope
- Earth observation satellite
- Lander
- Cubesat
- ...


 

Each predicate has a parent and/or a children. 
Here an exemple of relationship that we can find between predicates in Wikidata


![](https://mdbook.obspm.fr/uploads/7103069f-5934-4314-ada6-bc224f4f5d4e.png)


![](https://mdbook.obspm.fr/uploads/e90d2e57-7f1b-4993-8820-6f67b0a72739.png)



Here  we observe that a *Space Observatory* is both a *Telescope*, an *Artificial satellite*, an *Astronomical observatory* and a *Space instrument*.
Each of them also has a child. 

Most of the predicates are often related to a Spacecraft.


After streamlining the list of all predicates, allowing us to cover a wider range to obtain the most important number of elements, we obtain a much smaller list of predicates.
Here is a graph showing the percentage of predicates that are either a spacecraft or an observatory ...

![](https://mdbook.obspm.fr/uploads/1d303ead-59e4-414f-85c6-68c46681b70a.png)

This list of predicates allows us to simplify the query. We add to the initial query:

```SPARQL
?item wdt:P31/wdt:P279* wd:Q40218 . #spacecraft
``` 

Which means that we will search for all items whose nature of the element and its subclasses are a spacecraft.



### <span style="color:#2D5EAA">3.3 Exemple de requête complète </span>
Here is the current working query after streamlining it: 
 ```SQL
PREFIX schema: <http://schema.org/>
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
  {?item wdt:P31/wdt:P279*  wd:Q40218 .} # spacecraft
  UNION {?item wdt:P31/wdt:P279* wd:Q62832 .} # observatory
  UNION {?item wdt:P31/wdt:P279* wd:Q5916 .} # spaceflight
  UNION {?item  wdt:P31  wd:Q35273 .} # optical telescope
  UNION {?item  wdt:P31/wdt:P279*  wd:Q697175 .} # Launch vehicle
  UNION {?item  wdt:P31  wd:Q17004698 .} # astronomical interferometer
  UNION {?item  wdt:P31  wd:Q18812508 .} # space station module 
  UNION {?item  wdt:P31  wd:Q100349043 .} # space instrument 
  UNION {?item  wdt:P31  wd:Q797476 .} # rocket launch

  OPTIONAL {?item wdt:P4466 ?Unified_Astro_Thesaurus_ID .}
  OPTIONAL {?item wdt:P247 ?COSPAR_ID .}    
  OPTIONAL {?item wdt:P8913 ?NSSDCA_ID .}
  OPTIONAL {?item wdt:P2956 ?NAIF_ID .}
  OPTIONAL {?item wdt:P717 ?Minor_Planet_Center_observatory_ID .}
  OPTIONAL {?item skos:altLabel ?alias .}



   SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
 } 
 GROUP BY ?item ?itemLabel
 ORDER BY ?item
 ```
 We Download the results in `.json` format. The file is renames `extract_wikidata.json` in the WIKIDATA folder.
 
### <span style="color:#2D5EAA"> 3.4 Time of execution and pagination of results </span>

It is necessary to improve the process of retrieving results.
we can find our query under different languages by clicking on "code". Here we will use Python.


![](https://mdbook.obspm.fr/uploads/99ed1e8a-616e-452c-945f-f6476b33822c.png)

 A time limit prevents the display of the query results. To avoid this constraint, we will "paginate" to display fewer elements. To do this, we will use `OFFSET{}`and`LIMIT{}` in the query, which will define the number of elements per page.
 
In the `query_wikidata.py` Python file, we find our query to retrieve all the results in the output file `extract_wikidata`.

>[Verification of duplicates. In cases where there are duplicates, make sure that it is a duplicate and merge the two elements on Wikidata ![](https://mdbook.obspm.fr/uploads/c6730bd7-0af4-4ab8-8b39-1df7c51d14a1.png)
]

## <span style="color:#2D5EAA">4. Comparison of lists  </span>
We will compare all the elements that we find in the lists (NSSDC, NAIF, IAU, etc.) with the elements that we have in the Wikidata list. The elements that we find in each list do not have the same identifiers and require us to build a script that is adaptable to the data (NAIF_ID, NSSDC_ID, pds_id).

The compare_nomdelaliste_wiki.py scripts are created for list comparison. To compare the lists, we will use the fuzzywuzzy library, which searches for string matches. A score is established, which determines if the match is satisfactory. A score of 100 defines a perfect match.


Les scripts `compare_nomdelaliste_wiki.py` sont créés pour la comparaison des listes.
Pour comparer les listes nous nous aiderons de la librairie `fuzzywuzzy` qui procéde à la recherche de correspondances de chaines de caractères. Un score est établi et permet de déterminer si la correspondance est satisfaisante. Un score de 100 défini une correspondance parfaitement égale.

The result of the Wikidata query includes more than 13,000 elements, and the comparison process is very slow (about 120 minutes). To improve the program's execution time, we will use the multiprocessing module. It involves parallelizing processes on 4 cores, significantly reducing the script's execution time (30 minutes).

 
 Here is an example of a script for the NSSDC list:
```Python
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import json
import cProfile
from multiprocessing import Pool


def mon_scorer(q, c):
    r = fuzz.WRatio(q['Name'], c['itemLabel']) + fuzz.WRatio(q['Name'], c['aliases'])
    if c['all_COSPAR_ID'] != "":
        if q['ID'] == c['all_COSPAR_ID']:
            r += 500
        else:
            r -= 100
    if c['all_NSSDCA_ID'] != "":
        if q['ID'] == c['all_NSSDCA_ID']:
            r += 500
        else:
            r -= 100
    return r

def dummy_proc(x):
    return x
 ``` 
 

Here, we will compare all the "Name" in the NSSDC list with the "itemLabel" and "alias" in the Wikidata list. We will also search for a match between the identifiers in the NSSDC list and those in Wikidata, if they exist. The COSPAR_ID and NSSDCA_ID are mostly identical (except for spacecraft that were not launched or whose mission failed, in which case only the NSSDCA_ID is present). The results will appear in the file results.json.




### <span style="color:#2D5EAA">4.1 Results of comparison</span>
The results are in the file `results.json`
An example of results : 

```python
{"[1/1183]{'Name': '1962 Lambda 1', 'ID': '1962-011A'}": [({'item': 'http://www.wikidata.org/entity/Q9207773', 'itemLabel': 'corona-39', 'all_Unified_Astro_Thesaurus_ID': '', 'all_COSPAR_ID': '1962-011A', 'all_NAIF_ID': '', 'all_NSSDCA_ID': '1962-011A', 'all_Minor_Planet_Center_observatory_ID': '', 'aliases': '1962 Lambda 1|Discoverer 39|Grape Juice 2|Mission 9032'}, 1116), ({'item': 'http://www.wikidata.org/entity/Q1681656', 'itemLabel': 'science-power-module-1', 'itemDescription': 'proposed Russian module of the International Space Station', 'all_Unified_Astro_Thesaurus_ID': '', 'all_COSPAR_ID': '', 'all_NAIF_ID': '', 'all_NSSDCA_ID': '', 'all_Minor_Planet_Center_observatory_ID': '', 'aliases': 'Wissenschafts- und Energiemodule|NEM-1|SPM-1|科学電力モジュール1|Science and Power Module'}, 172), ({'item': 'http://www.wikidata.org/entity/Q3200441', 'itemLabel': 'venta-1', 'itemDescription': '42791', 'all_Unified_Astro_Thesaurus_ID': '', 'all_COSPAR_ID': '', 'all_NAIF_ID': '', 'all_NSSDCA_ID': '', 'all_Minor_Planet_Center_observatory_ID': '', 'aliases': 'VENTA 1'}, 172), ({'item': 'http://www.wikidata.org/entity/Q64691193', 'itemLabel': 'simulation-to-flight-1', 'itemDescription': 'CubeSat sattelite', 'all_Unified_Astro_Thesaurus_ID': '', 'all_COSPAR_ID': '', 'all_NAIF_ID': '', 'all_NSSDCA_ID': '', 'all_Minor_Planet_Center_observatory_ID': '', 'aliases': 'STF-1'}, 172), ({'item': 'http://www.wikidata.org/entity/Q847714', 'itemLabel': 'cosmos-1', 'itemDescription': 'solar sail project', 'all_Unified_Astro_Thesaurus_ID': '', 'all_COSPAR_ID': '', 'all_NAIF_ID': '', 'all_NSSDCA_ID': '', 'all_Minor_Planet_Center_observatory_ID': '', 'aliases': 'Cosmos-1|Космос 1|Космос-1 (солнечный парус)|宇宙一号'}, 172)]}
  1116 : {'item': 'http://www.wikidata.org/entity/Q9207773', 'itemLabel': 'corona-39', 'all_Unified_Astro_Thesaurus_ID': '', 'all_COSPAR_ID': '1962-011A', 'all_NAIF_ID': '', 'all_NSSDCA_ID': '1962-011A', 'all_Minor_Planet_Center_observatory_ID': '', 'aliases': '1962 Lambda 1|Discoverer 39|Grape Juice 2|Mission 9032'}
  172 : {'item': 'http://www.wikidata.org/entity/Q1681656', 'itemLabel': 'science-power-module-1', 'itemDescription': 'proposed Russian module of the International Space Station', 'all_Unified_Astro_Thesaurus_ID': '', 'all_COSPAR_ID': '', 'all_NAIF_ID': '', 'all_NSSDCA_ID': '', 'all_Minor_Planet_Center_observatory_ID': '', 'aliases': 'Wissenschafts- und Energiemodule|NEM-1|SPM-1|科学電力モジュール1|Science and Power Module'}
  172 : {'item': 'http://www.wikidata.org/entity/Q3200441', 'itemLabel': 'venta-1', 'itemDescription': '42791', 'all_Unified_Astro_Thesaurus_ID': '', 'all_COSPAR_ID': '', 'all_NAIF_ID': '', 'all_NSSDCA_ID': '', 'all_Minor_Planet_Center_observatory_ID': '', 'aliases': 'VENTA 1'}
  172 : {'item': 'http://www.wikidata.org/entity/Q64691193', 'itemLabel': 'simulation-to-flight-1', 'itemDescription': 'CubeSat sattelite', 'all_Unified_Astro_Thesaurus_ID': '', 'all_COSPAR_ID': '', 'all_NAIF_ID': '', 'all_NSSDCA_ID': '', 'all_Minor_Planet_Center_observatory_ID': '', 'aliases': 'STF-1'}
  172 : {'item': 'http://www.wikidata.org/entity/Q847714', 'itemLabel': 'cosmos-1', 'itemDescription': 'solar sail project', 'all_Unified_Astro_Thesaurus_ID': '', 'all_COSPAR_ID': '', 'all_NAIF_ID': '', 'all_NSSDCA_ID': '', 'all_Minor_Planet_Center_observatory_ID': '', 'aliases': 'Cosmos-1|Космос 1|Космос-1 (солнечный парус)|宇宙一号'}
{"[2/1183]{'Name': '1962 Phi 1', 'ID': '1962-021A'}": [({'item': 'http://www.wikidata.org/entity/Q9196224', 'itemLabel': 'corona-42', 'all_Unified_Astro_Thesaurus_ID': '', 'all_COSPAR_ID': '1962-021A', 'all_NAIF_ID': '', 'all_NSSDCA_ID': '1962-021A', 'all_Minor_Planet_Center_observatory_ID': '', 'aliases': 'Discoverer 42|1962 Phi 1'}, 1110), ({'item': 'http://www.wikidata.org/entity/Q1681656', 'itemLabel': 'science-power-module-1', 'itemDescription': 'proposed Russian module of the International Space Station', 'all_Unified_Astro_Thesaurus_ID': '', 'all_COSPAR_ID': '', 'all_NAIF_ID': '', 'all_NSSDCA_ID': '', 'all_Minor_Planet_Center_observatory_ID': '', 'aliases': 'Wissenschafts- und Energiemodule|NEM-1|SPM-1|科学電力モジュール1|Science and Power Module'}, 172), ({'item': 'http://www.wikidata.org/entity/Q1921511', 'itemLabel': 'mercury-redstone-1', 'itemDescription': 'test flight of the Redstone rocket and Mercury spacecraft', 'all_Unified_Astro_Thesaurus_ID': '', 'all_COSPAR_ID': '', 'all_NAIF_ID': '', 'all_NSSDCA_ID': '', 'all_Minor_Planet_Center_observatory_ID': '', 'aliases': 'MR-1|MR-1|ميركوري-ريدستون ١|Mercury 1|MR-1'}, 172), ({'item': 'http://www.wikidata.org/entity/Q56042842', 'itemLabel': 'boeing-starliner-1', 'itemDescription': 'first operational mission for the Boeing Starliner', 'all_Unified_Astro_Thesaurus_ID': '', 'all_COSPAR_ID': '', 'all_NAIF_ID': '', 'all_NSSDCA_ID': '', 'all_Minor_Planet_Center_observatory_ID': '', 'aliases': 'Crew-2|USCV-3|Starliner-1'}, 172), ({'item': 'http://www.wikidata.org/entity/Q605607', 'itemLabel': 'mercury-scout-1', 'itemDescription': 'test flight of a Scout rocket during Project Mercury', 'all_Unified_Astro_Thesaurus_ID': '', 'all_COSPAR_ID': '', 'all_NAIF_ID': '', 'all_NSSDCA_ID': '', 'all_Minor_Planet_Center_observatory_ID': '', 'aliases': 'MS-1|MS-1|Меркурий-Скаут|MS-1'}, 172)]}
  1110 : {'item': 'http://www.wikidata.org/entity/Q9196224', 'itemLabel': 'corona-42', 'all_Unified_Astro_Thesaurus_ID': '', 'all_COSPAR_ID': '1962-021A', 'all_NAIF_ID': '', 'all_NSSDCA_ID': '1962-021A', 'all_Minor_Planet_Center_observatory_ID': '', 'aliases': 'Discoverer 42|1962 Phi 1'}
  172 : {'item': 'http://www.wikidata.org/entity/Q1681656', 'itemLabel': 'science-power-module-1', 'itemDescription': 'proposed Russian module of the International Space Station', 'all_Unified_Astro_Thesaurus_ID': '', 'all_COSPAR_ID': '', 'all_NAIF_ID': '', 'all_NSSDCA_ID': '', 'all_Minor_Planet_Center_observatory_ID': '', 'aliases': 'Wissenschafts- und Energiemodule|NEM-1|SPM-1|科学電力モジュール1|Science and Power Module'}
  172 : {'item': 'http://www.wikidata.org/entity/Q1921511', 'itemLabel': 'mercury-redstone-1', 'itemDescription': 'test flight of the Redstone rocket and Mercury spacecraft', 'all_Unified_Astro_Thesaurus_ID': '', 'all_COSPAR_ID': '', 'all_NAIF_ID': '', 'all_NSSDCA_ID': '', 'all_Minor_Planet_Center_observatory_ID': '', 'aliases': 'MR-1|MR-1|ميركوري-ريدستون ١|Mercury 1|MR-1'}
  172 : {'item': 'http://www.wikidata.org/entity/Q56042842', 'itemLabel': 'boeing-starliner-1', 'itemDescription': 'first operational mission for the Boeing Starliner', 'all_Unified_Astro_Thesaurus_ID': '', 'all_COSPAR_ID': '', 'all_NAIF_ID': '', 'all_NSSDCA_ID': '', 'all_Minor_Planet_Center_observatory_ID': '', 'aliases': 'Crew-2|USCV-3|Starliner-1'}
  172 : {'item': 'http://www.wikidata.org/entity/Q605607', 'itemLabel': 'mercury-scout-1', 'itemDescription': 'test flight of a Scout rocket during Project Mercury', 'all_Unified_Astro_Thesaurus_ID': '', 'all_COSPAR_ID': '', 'all_NAIF_ID': '', 'all_NSSDCA_ID': '', 'all_Minor_Planet_Center_observatory_ID': '', 'aliases': 'MS-1|MS-1|Меркурий-Скаут|MS-1'}

  ```
The results are sorted according to the score. In the case of the NSSDC list, if the score is above 400, the comparison is a perfect match, and all results will appear in the `tres_certain` file. If the score is below 400, then the results will appear in the`non_trouve `file.

### <span style="color:#2D5EAA">4.2 Results analysis
</span>

#### <span style="color:#2D5EAA">4.2.1 Verification of the found elements</span>

Firstly, we verify if the results in the file `tres_certain` are coherent.

#### <span style="color:#2D5EAA">4.2.2 Verification of the unfound elements</span>

We perform a quick search to determine the reasons why the elements are not found in the wikidata list.
Several cases may occur:

-The entry does not exist, so we need to create a new item in wikidata.
-The element exists on wikidata but does not appear in the query result. The ID or name is missing (add it to wikidata).

Before creating a new entry on wikidata, it is imperative to check if the element does not already exist or if it has another name.

We will start by querying the Wikidata knowledge base. For example, we will write a query that lists all the elements that have an NSSDCA identifier.

```SQL
SELECT ?item ?itemLabel ?NSSDCA_ID 
WHERE {
  ?item wdt:P8913 ?NSSDCA_ID
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
} 
GROUP BY ?item ?NSSDCA_ID ?itemLabel
```
 This way, we can check if the identifier already exists. Several websites where missions are listed can also be used to find all space missions.
 
Here are some examples:
https://nssdc.gsfc.nasa.gov/planetary/chronology.html
https://ofrohn.github.io/seh-doc/list-missions.html
https://en.wikipedia.org/wiki/List_of_observatory_codes

### <span style="color:#2D5EAA">4.3 Adding on Wikidata base</span>

#### <span style="color:#2D5EAA">4.3.1 Adding manually elements to wikidata</span>
To add elements to Wikidata, it is necessary to create an account. This facilitates exchanges with wikidata administrators and contributors. Several means can be used to contact wikidata administrators on the Wikimedia France website:
https://www.wikimedia.fr/contact-public/

In particular, on the Wikimedia France community Discord server, a wikidata discussion thread allows discussion of project creation, SPARQL query construction, error reporting, and more.

##### <span style="color:#2D5EAA">4.3.1.1 Creating a new wikidata entry</span>

![](https://mdbook.obspm.fr/uploads/11013852-e175-4a7d-8416-0eb4ce63bd9f.png)

When creating a new item, English is preferred. When searching on wikidata, the result found will be what is listed in the English label.


![](https://mdbook.obspm.fr/uploads/d2f126df-7c15-4079-897d-2c1b1a9884d2.png)
![](https://mdbook.obspm.fr/uploads/056f6e86-16f9-4949-8c3b-8218192a4432.png)
Once created, we can edit the item using the "edit" button to add aliases or other labels in other languages. 


We can also add "Statements". For example, Driesen Observatory is an instance of an astronomical observatory.

![](https://mdbook.obspm.fr/uploads/9ae2d899-c634-40cf-8294-40eeb8bbf834.png)

We can add geographic coordinates, images, identifiers, and any other known information about the object.

It is advisable to add references for the source of the collected information.



##### <span style="color:#2D5EAA">4.3.1.2 Adding an ID </span>
![](https://mdbook.obspm.fr/uploads/201c5114-0fb9-426c-8939-dc523cefc798.png)

#### <span style="color:#2D5EAA">4.3.2  Bulk add/remove tool </span>

##### <span style="color:#2D5EAA">4.3.2.1 Quickstatements</span> 

QuickStatements is a tool that allows modification of Wikidata items through a set of text commands. The tool can add and remove statements, labels, descriptions, and aliases; as well as adding statements with optional qualifiers and sources. The command sequence can be entered in the import window or created in a spreadsheet or text editor, then pasted. To add a label in a specific language to an item, use 
#### Adding a Label
To add a label in a specific language to an item, use <span style="color:red">`Lxx`</span> instead of a property, with <span style="color:red">`xx`</span> as the language code.

**Example** : <span style="color:blue">`Q2513`</span> <span style="color:green">`TAB`</span> <span style="color:red">`Len`</span> <span style="color:green">`TAB`</span> **`"Hubble Space Telescope"`**
Meaning: add the label "Hubble Space Telescope" in English(Q2513)

#### Adding an Alias
To add an alias in a specific language to an item, use <span style="color:red">`Axx`</span>

**Example** : <span style="color:blue">`Q2513`</span> <span style="color:green">`TAB`</span><span style="color:red"> `Afr`</span> <span style="color:green">`TAB`</span> **`"télescope spatial Hubble"`**
Meaning: Add the French alias "télescope spatial Hubble" to Hubble Space Telescope (Q2513).

Several aliases can be added separated by the "|" character.
**Example** : <span style="color:blue">`Q2513`</span> <span style="color:green">`TAB`</span><span style="color:red"> `Afr`</span> <span style="color:green">`TAB`</span> **`"télescope spatial Hubble|HST|Hubble"`**

#### Adding a description
To add a description in a specific language to an item, use <span style="color:red">`Dxx`</span> 

**Example** : <span style="color:blue">`Q2513`</span> <span style="color:green">`TAB`</span><span style="color:red">` Den` </span> <span style="color:green">`TAB`</span> **`"NASA/ESA space telescope (launched 1990)"`**

Meaning: Add the English description "NASA/ESA space telescope (launched 1990)" to Hubble Space Telescope (Q2513).

To erase a label, description or site link, the value must be an empty string. The rest of the command works the same way.

#### Creating a new item
We can create a new item by inserting the word <span style="color:orange">`CREATE`</span>.  To add a statement to a newly created item, use the word <span style="color: violet">`LAST`</span> 

Let's take an example:

<span style="color:orange">`CREATE`</span>
<span style="color: violet"> `LAST`</span> <span style="color:green">`TAB`</span> <span style="color:red">`Len`</span> <span style="color:green">`TAB`</span> **`"Hubble Space Telescope"`** 
<span style="color: violet"> `LAST`</span> <span style="color:green">`TAB`</span> <span style="color:red">`Lfr`</span> <span style="color:green">`TAB`</span> **`"télescope spatial Hubble"`** 
<span style="color: violet"> `LAST`</span> <span style="color:green">`TAB`</span> <span style="color:red">`P31`</span> <span style="color:green">`TAB`</span> <span style="color:blue">`Q148578`</span> 
<span style="color: violet"> `LAST`</span> <span style="color:green">`TAB`</span> <span style="color:red">`P8913`</span> <span style="color:green">`TAB`</span>  **`"1990-037B"`**
`...`

Meaning: We create an item whose label in English is "Hubble Space Telescope", the label in French is "télescope spatial Hubble", the nature is a "space observatory" (Q148578) and whose NSSDCA identifier is "1990-037B".

For more information on the QuickStatements tool: https://www.wikidata.org/wiki/Help:QuickStatements
 
![](https://mdbook.obspm.fr/uploads/4f95f544-809a-44e4-94dd-3937675b4942.png)

##### <span style="color:#2D5EAA">4.3.2.2 OpenRefine</span>
OpenRefine is a tool allowing load, clean, compare, merge or reconcile unstructured datasets. Has a Wikidata plugin allowing for mass contribution

**To download OpenRefine:**
https://openrefine.org/download

**Creating a new project**
You can load your data file in several formats from local storage, using an URL, or from a server. Click on `NEXT` button. 

![](https://mdbook.obspm.fr/uploads/1ffb72a3-1256-4e0f-824f-556a85eabbb3.png)

If the format is incorrect when loading data, you can change it and select options that allow you to structure your data. Click on `Create project` button.

![](https://mdbook.obspm.fr/uploads/69211517-009a-4888-86fb-67908ff00260.png)

Once your data is loaded, you can reconcile it, meaning you can compare it to the Wikidata database.


![](https://mdbook.obspm.fr/uploads/c534473c-1042-4b74-9d84-0427b8be4b1e.png)

Choose English to reconcile the datas

![](https://mdbook.obspm.fr/uploads/064e3a44-84a9-4fe3-92f5-eca0b7f97b65.png)


Select an object for the reconciliation and click on ` start reconciling` button.

![](https://mdbook.obspm.fr/uploads/45bcc2a7-74a0-47b8-89ad-bcc5c5d28871.png)


Reconciliation can take time depending on the amount of data.

![](https://mdbook.obspm.fr/uploads/a80f3f45-9bfa-4626-b64d-ad48bd7b1cd7.png)


Once the reconciliation is complete, we notice that some elements have been reconciled while others have not.

![](https://mdbook.obspm.fr/uploads/b5dae963-8ba0-48b6-a506-41553b71649b.png)

We can use facets to select the unreconciled elements and associate them with an item on Wikidata or not.

![](https://mdbook.obspm.fr/uploads/e1a34dad-a635-4799-b70f-c46aba045d63.png)
![](https://mdbook.obspm.fr/uploads/99155ef3-8954-491b-a3fb-0a7191d75beb.png)

We can match the missing elements by selecting the item from the list of Wikidata reconciliation proposals.


![](https://mdbook.obspm.fr/uploads/142ab620-42ae-41eb-85c5-cd178024f340.png)

We can add columns based on reconciled values. This allows us to enrich our database by adding data recorded in Wikidata.

![](https://mdbook.obspm.fr/uploads/0435362b-17e9-4d62-80ee-88f6245269d8.png)
![](https://mdbook.obspm.fr/uploads/73ffddca-a2fa-41f6-b989-b6c2dd82f15b.png)
![](https://mdbook.obspm.fr/uploads/a6864f24-d55f-446d-b119-f89e2542c59b.png)

![](https://mdbook.obspm.fr/uploads/0710ec60-c5a0-4cef-821d-cdb8b1585e5f.png)
![](https://mdbook.obspm.fr/uploads/a8376b2b-b213-4497-8034-6523fbef1a9f.png)
![](https://mdbook.obspm.fr/uploads/9e1e5031-9ec1-4209-833a-df37206ee838.png)


#### <span style="color:#2D5EAA">4.3.2 Wikidata Gadgets</span>
Gadgets are programs that can also help perform various tasks on Wikidata more easily and efficiently. They can be activated in the Preferences menu under the "Gadgets" section.

Some examples include:

- slurpInterwiki: Imports interwiki links from a Wikipedia project.
- Merge: This script adds a tool for merging entries.
- SitelinkCheck: Displays a form to check if a particular link is already used and gives the item identification number if it is.
- autoEdit: Automatically adds labels through existing interwiki links and descriptions through a customizable list.

#### <span style="color:#2D5EAA">4.3.4 Merging similar items on Wikidata </span>

Multiple items that concern the same subject, concept, or object are merged. Merges can be done manually or automatically by moving interlanguage links and statements into one item and then redirecting the obsolete item(s).
We prefer automatic merges, despite the risk of errors during data transfer or modifying an item that is not exactly the same. For automatic merges, we will use the Merge gadget on Wikidata.

![](https://mdbook.obspm.fr/uploads/0d44e1b6-6d16-4b39-9896-ccb244705c8c.png)

We access the Merge tool in the More tab. Simply enter the Q identifier. We will preferably keep the best-referenced Q, often the oldest.

![](https://mdbook.obspm.fr/uploads/48053cd5-01f3-47bd-a47c-94f456fefff3.png)

The merged Q identifiers will not be assigned again, they will be redirected to the chosen Q identifier.

#### <span style="color:#2D5EAA">4.3.5 Adding erroneous information to Wikidata</span>
It can happen inadvertently to introduce errors when enriching Wikidata's database (property, ID, duplication with another item...). 
Wikidata has set up bots to verify if there are aberrations in the addition of a new item to their

If the error is isolated, it will be deleted. However, if it is recurring, it will be flagged to the contributor.
In this case, an administrator will contact the contributor by email. The latter will have to correct the erroneous information.

#### <span style="color:#2D5EAA">4.3.6 Wikidata robots </span>
Contributors can have a robot to simplify and save time when adding items. To do this, a separate account must be created for it.
In general, it takes the contributor's name followed by `bot`.

Wikidata robots allow modifications to be made without human assistance. They can add interwiki links, labels, descriptions, and even create items.

Warning! Robots are extremely fast and can disrupt Wikidata's operation if poorly designed or used. The contributor is therefore responsible for their robot's contributions. In case of malfunction caused by a bot, it must be stopped by the contributor, otherwise it will be blocked by an administrator.
A request must be made to the administrators for approval and the robot status, detailing the tasks performed.




