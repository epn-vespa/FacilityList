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
from graph.graph import Graph
from pathlib import Path


def main(input_ontology: str,
         output_merged: str):
    output_merged_folder = Path(input_ontology)
    output_merged = output_merged_folder.parent / output_merged
    if output_merged.exists():
        raise FileExistsError(f"{output_merged} already exists. Please use another output filename.")
    output_merged = str(output_merged)
    merge_uris.main(input_ontology,
                    output_merged)
    output_obsf, output_obsi = split_instruments.split_instruments(output_merged)
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
    args = parser.parse_args()
    main(args.input_ontology,
         args.output_ontology)
