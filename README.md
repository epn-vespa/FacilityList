# FacilityList: Astronomy Observation Facilities Matcher


## [data](data)

Observation facility lists from various origins and in various formats.

Supported lists:
| List          | Format |
| ------------- | ------ |
| AAS           | HTML   |
| IAU-MPC       | HTML   |
| IMCCE/Quaero  | JSON   |
| NAIF          | HTML   |
| NASA/PDS      | XML    |
| NSSDC         | HTML   |
| SPASE         | JSON   |
| WikiData      | RDF    |

Types of facilities:
_Spacecraft_, _Observatories_, _Telescopes_, _Investigations_, _Airborne platforms_.


## update.py
Download data for facility lists and save them in an unified output ontology.
It will perform entity typing by LLM and try to retrieve geographical information for every entity.
type_confidence and location_confidence will be added to every entity, depending on how those information were retrieved.
This might take some time during the first run, but will save all data in cache for next runs.

### Remark
All data are publicly available but the URLs' availability or structures might change over years.
We will publish the result ontology on OntoPortal-Astro or another Ontology sharing tool. This ontology will be the output of this script, that serves as a basis for map_ontologies.pỳ.


### Usage
```python update.py [options]```

| Option                    | Description                                                                                                                                                                             |
| ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `-l`, `--lists`           | Name(s) of the lists to extract data from. Default is `all`. Available options: `all` or specific list names from `ExtractorLists.EXTRACTORS_BY_NAMES`. Multiple lists can be provided. |
| `-i`, `--input-ontology`  | Optional input ontology file (`.ttl`). Data from this ontology will be merged with newly extracted data. Useful for running the script in multiple steps.                               |
| `-o`, `--output-ontology` | Output ontology file name. Default is `output.ttl`.                                                                                                                                     |
| `-c`, `--no-cache`        | If set, disables caching and forces re-download and version comparison.                                                                                                                 |

### Example
```python update.py -l aas pds -i wikidata.ttl -o all_entities.ttl```

## map_ontologies.py
Entity matching tool. Will perform external ID linking, then follow a merging strategy configuration file (default: conf/merging_strategy.conf).
Then, generate a full mapping, compute discriminant criteria, compute other scores on the remaining candidate pairs.

LLM validation uses an LLM to accept/reject candidate pairs. Save the mapped data (data with skos:exactMatch for matched objects) with the synonym sets objects. Save its SSSOM ontology next to it.
The execution time depends on the scores used in the merging strategy (sentence-cosine-similarity and llm-embedding take longer to encode entities), and on the validation LLM's size.
The quality of the mapping mostly depends on the LLM used for validation and the instructions given in the prompt, as well as the representation of entities.

### Usage
```python map_ontologies.py -i input_ontology.ttl [options]```

| Option                      | Description                                                                                                                            |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `-i`, `--input-ontologies`  | **(Required)** One or more input ontologies (`.ttl`) to process.        |
| `-o`, `--output-dir`        | Output directory to save the final merged ontology and the SSSOM mapping ontology. Default is a timestamped folder.                    |
| `-l`, `--limit`             | (Optional) Limit the number of entities per source to speed up testing. Only the top N entities from each list will be compared (NxN). |
| `-s`, `--merging-strategy`  | Path to the merging strategy config file. Default is `conf/merging_strategy.conf`.                                                     |
| `-d`, `--direct-validation` | Skip manual review. Candidate matches will be validated automatically based on scores.                                                 |
| `--human-validation`        | Enable human-in-the-loop disambiguation after scoring. This disables LLM-based validation.                                             |

Input ontologies can be already processed ontologies with validated pairs. In this case, it will try to map only unmapped entities, ignoring entities that are already paired with an entity from the target list.

## evaluate_sssom.py
Evaluation tool. Evaluates a mapping (SSSOM ontology) using a gold TSV file with annotations ('o': same, 'x': distinct) that contains annotated candidate pairs.

Annotation files can be found in the data/evaluation folder, while the SSSOM ontology is the output of map_ontologies.py.

### Usage
```ipython evaluate_sssom.py -t annotations.tsv -s SSSOM_ontology.ttl```


## Acknowledgments

This activity is a joint effort of the EPN-VESPA, IVOA and IPDA projects.

This work has also been supported by: the Europlanet 2020 Research Infrastructure project, which received funding from the European Union's Horizon 2020 research and innovation programme under grant agreement No 654208; the Europlanet 2024 Research Infrastructure project, which received funding from the European Union's Horizon 2020 research and innovation programme under grant agreement No 871149; the FAIR-IMPACT project, which received funding from the European Commission's Horizon Europe Research and Innovation programme under grant agreement no 101057344; and OPAL cascading grant from the the OSCARS project, which received funding from the European Commission's Horizon Europe Research and Innovation programme under grant agreement no 101129751.
