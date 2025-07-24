# FacilityList: Observation Facilities Matcher

This activity is a joint effort of the EPN-VESPA, IVOA and IPDA projects.

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
Download data for facility lists and save it in an unified output ontology.
It will perform entity typing by LLM and try to retrieve geographical information for every entity.
type_confidence and location_confidence will be added to every entity, depending on how those information were retrieved.
This might take some time during the first run, but will save all data in cache for next runs.


### Usage
python update.py [options]

| Option                    | Description                                                                                                                                                                             |
| ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `-l`, `--lists`           | Name(s) of the lists to extract data from. Default is `all`. Available options: `all` or specific list names from `ExtractorLists.EXTRACTORS_BY_NAMES`. Multiple lists can be provided. |
| `-i`, `--input-ontology`  | Optional input ontology file (`.ttl`). Data from this ontology will be merged with newly extracted data. Useful for running the script in multiple steps.                               |
| `-o`, `--output-ontology` | Output ontology file name. Default is `output.ttl`.                                                                                                                                     |
| `-c`, `--no-cache`        | If set, disables caching and forces re-download and version comparison.                                                                                                                 |

### Example
python update.py -l aas pds -i wikidata.ttl -o all_entities.ttl

## merge.py
Entity matching tool. Will perform external ID linking, then follow a merging strategy configuration file (default: conf/merging_strategy.conf).
Then, generate a full mapping, compute discriminant criteria, compute other scores on the remaining candidate pairs.

LLM validation uses an LLM to accept/reject candidate pairs. Save the mapped data with synonym sets objects with its SSSOM ontology.
The execution time depends on the scores used in the merging strategy (sentence-cosine-similarity and llm-embedding take longer to encode entities), and on the validation LLM.

### Usage
python merge.py -i input_ontology.ttl [options]

| Option                      | Description                                                                                                                            |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `-i`, `--input-ontologies`  | **(Required)** One or more input ontologies (`.ttl`) to process.        |
| `-o`, `--output-dir`        | Output directory to save the final merged ontology and the SSSOM mapping ontology. Default is a timestamped folder.                    |
| `-l`, `--limit`             | (Optional) Limit the number of entities per source to speed up testing. Only the top N entities from each list will be compared (NxN). |
| `-s`, `--merging-strategy`  | Path to the merging strategy config file. Default is `conf/merging_strategy.conf`.                                                     |
| `-d`, `--direct-validation` | Skip manual review. Candidate matches will be validated automatically based on scores and logic.                                       |
| `--human-validation`        | Enable human-in-the-loop disambiguation after scoring. This disables LLM-based validation.                                             |