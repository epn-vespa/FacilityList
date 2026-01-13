"""
Split the instruments from an ontology.

    Everything that has a type under observation_facility type
    will be in the output obsf (facilities) ontology,
    and everything that has the instrument type
    will be in the output obsi (instruments) ontology.
    Thus, there will be common entities between both ontologies.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import setup_path
import argparse

from rdflib import Graph as G, RDFS, RDF
from graph.graph import Graph
from graph.properties import Properties

properties = Properties()

def split_instruments(input_file: str):
    """
    Set instruments and observation facilities apart in distinct ontologies.
    """
    graph = Graph()
    facilities_file = input_file.removesuffix(".ttl") + "_facilities.ttl"
    instruments_file = input_file.removesuffix(".ttl") + "_instruments.ttl"
    output_facilities = G()
    output_instruments = G()
    graph.parse(input_file)
    output_facilities.parse(input_file)

    for instrument_uri, _, _ in graph.triples((None, RDF.type, graph.PROPERTIES.OBS["instrument"])):
        for subj, pred, obj in graph.triples((instrument_uri, None, None)):
            output_instruments.add((subj, pred, obj))
            # Get other types
            res = list(graph.triples((instrument_uri, pred, None)))
            if len(res) == 1: # Only instrument. Split it from the observation facilities ontology.
                output_facilities.remove((subj, pred, obj))
    # Add classes hierarchy to ouput_instruments
    for s, p, o in graph.triples((None, RDFS.subClassOf, None)):
        output_instruments.add((s, p, o))
    # source lists
    for s, _, _ in graph.triples((None, None, graph.PROPERTIES.OBS["facility-list"])):
        for _, p, o in graph.triples((s, None, None)):
            output_instruments.add((s, p, o))

    # Bind namespaces to instruments output ontology
    output_instruments.bind("obsf", properties.OBS)
    output_instruments.bind("geo1", properties.GEO)
    output_instruments.bind("wb", properties.WB)
    output_instruments.bind("ivoasem", properties.IVOASEM)


    with open(facilities_file, 'w') as file:
        file.write(output_facilities.serialize())
        print(f"Facilities ontology saved in {facilities_file}")

    with open(instruments_file, 'w') as file:
        file.write(output_instruments.serialize())
        print(f"Instruments ontology saved in {instruments_file}")

    return facilities_file, instruments_file


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog = "split_instruments",
                                     description = "Generate two ontologies: one for instruments, one for observation facilities from a merged ontology for OntoPortal.")
    parser.add_argument("-i",
                        "--input-ontology",
                        dest = "input_ontology",
                        required = True,
                        help = "A merged ontology.")
    args = parser.parse_args()
    split_instruments(args.input_ontology)