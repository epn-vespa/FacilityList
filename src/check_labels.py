"""
Script that checks labels' format conformity to the standards described in post_process.
Call after generate_views on the main ontology.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
from argparse import ArgumentParser
from views.post_process import PostProcess
from graph.graph import Graph


def main(input_ontology: str,
         output_file: str):
    graph = Graph(input_ontology)
    pp = PostProcess(graph)
    for uri, _, label in graph.triples((None, graph.PROPERTIES.label, None)):
        pp._check_llm_label(uri, label)

    pp._save_label_warnings(output_file)


if __name__ == "__main__":

    parser = ArgumentParser(
        prog = "check_labels.py",
        description = "Check labels' format conformity to standards described in post_process.")

    parser.add_argument("-i",
                        "--input-ontology",
                        dest = "input_ontology",
                        type = str,
                        required = True,
                        help = "Input ontology.")
    parser.add_argument("-o",
                        "--output-file",
                        dest = "output_file",
                        default = "label_warnings.json",
                        type = str,
                        required = False,
                        help = "Output file to save the warnings (JSON).")

    args = parser.parse_args()
    main(args.input_ontology,
         args.output_file)