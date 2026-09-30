#!/bin/env python
"""
Calls all the views generators in order to get:
- 1 intermediate ontology (with merged synonym sets)
- 2 output ontologies (Obs Facilities & Instruments)
- 2 csv
- 2 json

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import argparse
from views import merge_uris, split_instruments, generate_csv_json
from pathlib import Path
from rdflib import Graph, OWL


def main(input_ontology: str,
         output_merged: str,
         community_views: list[str] = [],
         previous_output_facilities: Path = None,
         previous_output_instruments: Path = None):
    output_merged_folder = Path(input_ontology)
    output_merged = output_merged_folder.parent / output_merged
    if output_merged.exists():
        raise FileExistsError(f"{output_merged} already exists. Please use another output filename.")
    output_merged = str(output_merged)
    merge_uris.main(input_ontology,
                    output_merged,
                    community_views)
    facility_version, instrument_version = "0.0.0", "0.0.0"
    if previous_output_facilities and previous_output_facilities.exists():
        graph = Graph()
        graph.parse(previous_output_facilities)
        for _, _, version in graph.triples(None, OWL.versionInfo, None):
            facility_version = str(version)
    facility_version = facility_version.rsplit('.')
    facility_version[-1] = str(int(facility_version[-1]) + 1)
    facility_version = '.'.join(facility_version)
    if previous_output_instruments and previous_output_instruments.exists():
        graph = Graph()
        graph.parse(previous_output_instruments)
        for _, _, version in graph.triples(None, OWL.versionInfo, None):
            instrument_version = str(version)
    instrument_version = instrument_version.rsplit('.')
    instrument_version[-1] = str(int(instrument_version[-1]) + 1)
    instrument_version = '.'.join(instrument_version)

    output_obsf, output_obsi = split_instruments.split_instruments(output_merged, facility_version, instrument_version)
    generate_csv_json.main(output_obsf)
    generate_csv_json.main(output_obsi)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog = "generate_views.py",
                                     description = "Generate all views (Instruments, Obs facilities)*(ttl, csv, json).")
    parser.add_argument("-i",
                        "--input-ontology",
                        dest = "input_ontology",
                        type = str,
                        required = True,
                        help = "Input ontology (that has been mapped and contains exactMatch relations).")

    parser.add_argument("-o",
                        "--outout-ontology",
                        dest = "output_ontology",
                        required = False,
                        default = "merged.ttl",
                        help = "Base output ontology filename that will contain all merged entities (intermediate step to merge synonym sets before generating views). Note that the views will be generated next to the original ontologies.")

    from update import Updater
    parser.add_argument("-c",
                        "--community-views",
                        dest = "community_views",
                        type = str,
                        nargs = "*",
                        default = [],
                        choices = set(Updater.SOURCE_BY_PRIMARY_COMMUNITY),
                        help = "Consider lists for which the primary community (or alliance) is amongst this list as authoritative, and ignore synonym sets (or lonely entities) that did not match with any entity in these lists.")
    args = parser.parse_args()
    main(args.input_ontology,
         args.output_ontology,
         args.community_views)
