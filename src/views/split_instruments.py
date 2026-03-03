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
import argparse

from rdflib import Graph as G, RDFS, RDF, URIRef, Namespace
from graph.graph import Graph
from graph.properties import Properties

properties = Properties()

def split_instruments(input_file: str):
    """
    Set instruments and observation facilities apart in distinct ontologies.
    """
    graph = Graph(input_file, replace = True)
    facilities_file = input_file.removesuffix(".ttl") + "_facilities.ttl"
    instruments_file = input_file.removesuffix(".ttl") + "_instruments.ttl"
    output_facilities = G()
    output_instruments = G()
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
    output_instruments.bind("obs", properties.OBS)
    output_instruments.bind("obsi", properties.OBSI)
    output_instruments.bind("obsf", properties.OBSF)
    output_instruments.bind("geo1", properties.GEO)
    output_instruments.bind("wb", properties.WB)
    output_instruments.bind("ivoasem", properties.IVOASEM)

    output_facilities.bind("obs", properties.OBS)
    output_facilities.bind("obsf", properties.OBSF, replace = True)
    output_facilities.bind("obsi", properties.OBSI)

    changes = change_namespace(output_facilities,
                               properties.OBS,
                               properties.OBSF)
    changes = change_namespace(output_instruments,
                               properties.OBS,
                               properties.OBSI,
                               changes = changes)
    update_object_namespaces(output_facilities,
                             changes = changes)
    update_object_namespaces(output_instruments,
                             changes = changes)


    with open(facilities_file, 'w') as file:
        file.write(output_facilities.serialize())
        print(f"Facilities ontology saved in {facilities_file}")

    with open(instruments_file, 'w') as file:
        file.write(output_instruments.serialize())
        print(f"Instruments ontology saved in {instruments_file}")

    return facilities_file, instruments_file


def change_namespace(graph: Graph,
                     old_ns: Namespace = properties.OBS,
                     new_ns: Namespace = properties.OBSF,
                     changes: dict = dict()):
    """
    Replace a Namespace in a graph.

    Args:
        graph: the Graph to update with new namespaces
        new_ns: the main Namespace of subjects
        changes: dictionary to save changes (old->new) of URIs for
                 updating cross-links between more than one graphs
    """
    old_ns = properties.OBS
    for s, p, o in graph.triples((None, None, None)):
        s2 = s
        o2 = o
        p2 = p
        if isinstance(s, URIRef) and str(s).startswith(str(old_ns)):
            s2 = URIRef(str(s).replace(str(old_ns), str(new_ns), 1))
            changes[s] = s2
        if isinstance(p, URIRef) and str(p).startswith(str(old_ns)):
            p2 = URIRef(str(p).replace(str(old_ns), str(new_ns), 1))
            changes[p] = p2
        if isinstance(o, URIRef) and str(o).startswith(str(old_ns)):
            o2 = URIRef(str(o).replace(str(old_ns), str(new_ns), 1))
            changes[o] = o2
        graph.remove((s, p, o))
        graph.add((s2, p2, o2))
    return changes


def update_object_namespaces(graph: Graph,
                             changes: dict):
    """
    Take the changes that were made in URIs of subjects
    to update the objects as well.

    Args:
        graph: graph to update
        changes: dict of changes (new terms by previous terms)
    """
    for s, p, o in graph.triples((None, None, None)):
        if o in changes:
            o2 = changes[o]
            graph.remove((s, p, o))
            graph.add((s, p, o2))


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
