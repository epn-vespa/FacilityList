"""
Script that applies links (exactMatch, broadMatch, narrowMatch)
from a SSSOM ontology to generate a linked ontology.
Use this script to apply previously created mappings after an
update.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import pathlib
import os
import shutil
from argparse import ArgumentParser
from collections import defaultdict
from rdflib import Graph, Namespace, RDF, SKOS, URIRef
from utils.dict_utilities import UnionFind
from utils.string_utilities import standardize_uri

SSSOM = Namespace("https://w3id.org/sssom/")


def main(from_folder: str,
         to_folder: str):
    """
    Copy mappings from the from_folder to the to_folder
    Args:
        from_folder: folder containing the previous version's files (linked.ttl, mapping.ttl) to apply mappings from
        to_folder: folder containing the latest version's files (updated.ttl) to apply mappings to
    """
    from_folder = pathlib.Path(from_folder)
    to_folder = pathlib.Path(to_folder)
    assert from_folder.is_dir()
    assert to_folder.is_dir()
    from_linked = from_folder / "linked.ttl"
    from_mapping = from_folder / "mapping.ttl"
    to_mapping = to_folder / "mapping.ttl" # update to_mapping if exists, else copy it from from_mapping
    to_updated = to_folder / "updated.ttl"
    if not to_updated.exists():
        # No update performed
        shutil.copy(from_linked, to_updated)
        # raise FileNotFoundError(f"The file to apply mappings to ({to_updated}) does not exist. Perhaps it was named differently?")

    to_updated_graph = Graph()
    to_updated_graph.parse(to_updated)
    if from_linked.exists():
        apply_from_linked(from_linked, to_updated_graph)
    elif from_mapping.exists():
        apply_from_mapping(from_mapping, to_mapping, to_updated_graph)
    else:
        raise FileNotFoundError(f"Neither {from_linked} and {from_mapping} exist, can not apply mappings from the folder {from_folder}.")

    # Transfer mappings from from_folder/mapping.ttl to to_folder/mapping.ttl and save the new to_folder/mapping.ttl
    if to_mapping.exists():
        from_mapping_graph = Graph()
        from_mapping_graph.parse(from_mapping)
        to_mapping_graph = Graph()
        to_mapping_graph.parse(to_mapping)
        for s, p, o in from_mapping_graph.triples((None, None, None)):
            to_mapping_graph.add((s, p, o))
        to_mapping_graph.serialize(to_mapping)
        print("New mapping saved in:", to_mapping)
    else:
        shutil.copy2(from_mapping, to_mapping)

    # Serialization
    to_linked = to_folder / "linked.ttl"
    to_updated_graph.serialize(to_linked)
    print("New linked ontology saved in:", to_linked)
    return to_linked



def apply_from_linked(from_linked: pathlib.Path,
                      to_graph: Graph):
    """
    Copy exactMatch, narrowMatch and broadMatch to the to_updated ontology.
    """
    from_graph = Graph()
    from_graph.parse(from_linked)

    all_uris = set()
    for s, _, _, in to_graph.triples((None, None, None)):
        all_uris.add(s)

    rels = [SKOS.exactMatch, SKOS.broadMatch, SKOS.narrowMatch]

    for rel in rels:
        for s, p, o in from_graph.triples((None, rel, None)):
            if o not in all_uris:
            # Spase URI is different from the source ontology one
                if "spase#" in str(s):
                    s = find_spase_entity(s, from_graph)
                if "spase#" in str(o):
                    o = find_spase_entity(o, from_graph)
                if o not in all_uris:
                    continue
            # Reset relation
            to_graph.remove((s, None, o))
            to_graph.remove((o, None, s))
            to_graph.add((s, p, o))
            if p == SKOS.exactMatch:
                to_graph.add((o, p, s))
            elif p == SKOS.broadMatch:
                to_graph.add((o, SKOS.narrowMatch, s))
            elif p == SKOS.narrowMatch:
                to_graph.add((o, SKOS.broadMatch, s))


def apply_from_mapping(from_mapping: pathlib.Path,
                       to_graph: Graph):
    """
    Copy exactMatch, narrowMatch and broadMatch to the to_updated ontology.
    """
    from_graph = Graph()
    from_graph.parse(from_mapping)
    all_uris = set()
    for s, _, _, in to_graph:
        all_uris.add(s)

    query = f"""
    SELECT ?s ?p ?o WHERE {{
        ?mapping a sssom:Mapping .
        ?mapping sssom:subject_id ?s .
        ?mapping sssom:predicate_id ?p .
        ?mapping sssom:object_id ?o .
        FILTER NOT EXISTS {{
            ?mapping owl:deprecated true .
        }}
    }}
    """
    rels = [SKOS.exactMatch, SKOS.broadMatch, SKOS.narrowMatch]
    for s, p, o in from_graph.query(query):
        if s not in all_uris or o not in all_uris:
            if "spase#" in str(s):
                s = find_spase_entity(s)
            if "spase#" in str(o):
                o = find_spase_entity(o)
            if s not in all_uris or o not in all_uris:
                continue
        # Reset relation
        to_graph.remove((s, None, o))
        to_graph.remove((o, None, s))
        to_graph.add((s, p, o))
        if p == SKOS.exactMatch:
            to_graph.add((o, p, s))
        elif p == SKOS.broadMatch:
            to_graph.add((o, SKOS.narrowMatch, s))
        elif p == SKOS.narrowMatch:
            to_graph.add((o, SKOS.broadMatch, s))


def main_old(input_ontology_path: str,
         # mapping_ontology: str,
         input_sssom_ontology_path: str,
         linked_ontology_path: str,
         output_ontology_path: str,
         llm_manual_only: bool):
    """
    Args:
        input_ontology_path: path of an ontology containing the entities' information
        input_sssom_ontology_path: path of the SSSOM ontology containing the mappings to apply
        linked_ontology_path: path of the previous version's linked ontology
        output_ontology_path: path to the output ontology
    """

    # _SSSOM = Namespace("https://w3id.org/sssom/")
    input_graph = Graph()
    sssom_graph = Graph()
    mapping_graph = Graph()
    linked_graph = Graph()
    input_ontology_path = pathlib.Path(input_ontology_path)
    if input_ontology_path.is_dir():
        input_folder = input_ontology_path
        output_folder = str(input_ontology_path) + "-v2"
        output_folder = pathlib.Path(output_folder)
        os.makedirs(output_folder, exist_ok = True)
        output_ontology_path = output_folder / "linked.ttl"
        input_ontology_path = input_folder / "updated.ttl"
        mapping_ontology = input_folder / "mapping.ttl"
        mapping_graph.parse(mapping_ontology)
        linked_graph.parse(input_folder / "linked.ttl")

    else:
        if not output_ontology_path:
            output_ontology_path = input_ontology_path.removesuffix(".ttl") + "_linked.ttl"

    input_graph.parse(input_ontology_path)
    sssom_graph.parse(input_sssom_ontology_path)
    # Namespaces
    for prefix, ns in input_graph.namespaces():
        mapping_graph.bind(prefix, ns)

    manual_only_str = ""
    if llm_manual_only:
        manual_only_str = f"FILTER NOT EXISTS {{ ?mapping sssom:similarity_measure semapv:StringEquality . }}"
    query = f"""
    SELECT ?mapping ?s ?p ?o WHERE {{
        ?mapping a sssom:Mapping .
        ?mapping sssom:subject_id ?s .
        ?mapping sssom:predicate_id ?p .
        ?mapping sssom:object_id ?o .
        FILTER NOT EXISTS {{
            ?mapping owl:deprecated true .
        }}
        {manual_only_str}
    }}
    """
    #for mapping, _, _ in sssom_graph.triples((None, RDF.type, _SSSOM.Mapping)):
    uf = UnionFind()
    all_uris = set()
    all_mappings = set()
    groups = defaultdict(set)
    for s, _, _ in input_graph.triples((None, None, None)):
        all_uris.add(s)
    for m, s, p, o in sssom_graph.query(query):
        all_mappings.add(m)
        if s in all_uris:
            pass
        elif "spase#" in str(s):
            s = find_spase_entity(s, input_graph)
            if not s:
                # print(f"Warning: URI not found in {input_ontology_path}:", s)
                continue
        else:
            # print(f"Warning: URI not found in {input_ontology_path}:", s)
            continue

        if o in all_uris:
            pass
        elif "spase#" in str(o):
            o = find_spase_entity(o, input_graph)
            if not o:
                # print(f"Warning: URI not found in {input_ontology_path}:", o)
                continue
        else:
            # print(f"Warning: URI not found in {input_ontology_path}:", o)
            continue
        input_graph.add((s, p, o))
        if p == SKOS.exactMatch:
            uf.union(s, o)
        elif p == SKOS.broadMatch:
            input_graph.remove((o, None, s))
            input_graph.remove((s, None, o))
            input_graph.add((o, SKOS.narrowMatch, s))
            input_graph.add((s, SKOS.broadMatch, o))
        elif p == SKOS.narrowMatch:
            input_graph.remove((o, None, s))
            input_graph.remove((s, None, o))
            input_graph.add((o, SKOS.broadMatch, s))
            input_graph.add((s, SKOS.narrowMatch, o))

    for uri in all_uris:
        groups[uf.find(uri)].add(uri)
    for _, group in groups.items():
        for e1 in group:
            for e2 in group:
                if e1 < e2:
                    # Remove previous links
                    input_graph.remove((e1, None, e2))
                    input_graph.remove((e2, None, e1))
                    input_graph.add((e1, SKOS.exactMatch, e2))
                    input_graph.add((e2, SKOS.exactMatch, e1))
    output_mapping_path = str(output_ontology_path).removesuffix("linked.ttl") + "mapping.ttl"
    output_mapping_path = pathlib.Path(output_mapping_path)

    for m in all_mappings:
        for s, p, o in sssom_graph.triples((m, None, None)):
            o = new_by_old_spase.get(o, o)
            mapping_graph.add((s, p, o))
    added = set()
    for m in all_mappings:
        # find the mapping set
        for _, _, mapping_set in sssom_graph.triples((m, SSSOM["mapping_set_id"], None)):
            if mapping_set in added:
                continue
            added.add(mapping_set)
            for s, p, o in sssom_graph.triples((mapping_set, None, None)):
                mapping_graph.add((s, p, o))

    mapping_graph.serialize(output_mapping_path)
    input_graph.serialize(output_ontology_path)
    print("New mapping saved in:", output_mapping_path)
    print("New linked ontology saved in:", output_ontology_path)


all_entities_spase_by_label = defaultdict(set)
new_by_old_spase = dict()


def find_spase_entity(uri: URIRef,
                      graph: Graph) -> URIRef:
    """
    Old SPASE uris => new SPASE uris (to use a mapping
    done with the old version of SPASE extractor)

    Args:
        uri: the old uri
        graph: the new graph to search new uri from
    """
    if not all_entities_spase_by_label:
        for entity, _, label in graph.triples((None, SKOS.prefLabel, None)):
            label = standardize_uri(label)
            if "spase#" in entity:
                all_entities_spase_by_label[label].add(entity)
        for entity, _, alt_label in graph.triples((None, SKOS.altLabel, None)):
            alt_label = standardize_uri(alt_label)
            if "spase#" in entity:
                all_entities_spase_by_label[alt_label].add(entity)
    uri_str = str(uri).split('#')[-1]
    new_uri = all_entities_spase_by_label.get(uri_str)
    if not new_uri:
        return None
    new_by_old_spase[uri] = list(new_uri)[0]
    return list(new_uri)[0]


if __name__ == "__main__":
    parser = ArgumentParser(prog = "apply_links.py",
                            description = "Apply links from an SSSOM ontology" \
                                          "or a linked ontology" \
                                          "to map an updated ontology.")

    """
    parser.add_argument("-i",
                        "--input-ontology",
                        dest = "input_ontology_path",
                        required = True,
                        type = str,
                        help = "The input ontology (updated).")
    parser.add_argument("-m",
                        "--input-mapping",
                        dest = "input_mapping",
                        required = False,
                        type = str,
                        help = "The SSSOM ontology of the input ontology (if existing)" \
                        "to which mappings will be transfered."
                        )
    parser.add_argument("-s",
                        "--sssom-ontology",
                        dest = "input_sssom_ontology_path",
                        required = True,
                        type = str,
                        help = "The SSSOM ontology to apply mappings from.")
    parser.add_argument("-l",
                        "--linked-ontology",
                        dest = "linked_ontology_path",
                        required = False,
                        type = str,
                        help = "The linked ontology to apply mappings from.")
    parser.add_argument("-o",
                        "--output-ontology",
                        dest = "output_ontology_path",
                        required = False,
                        type = str,
                        default = "",
                        help = "Output ontology path.")
    parser.add_argument("-m",
                        "--llm-manual-only",
                        dest = "llm_manual_only",
                        required = False,
                        action = "store_true",
                        help = "If True, only mappings made by an LLM or" \
                        "an human will be applied, ignoring exact matches and" \
                        "threshold mappings.")
    args = parser.parse_args()
    main(args.input_ontology_path,
         # args.mapping_ontology, # TODO
         args.input_sssom_ontology_path,
         args.output_ontology_path,
         args.linked_ontology_path,
         args.llm_manual_only)
    """

    parser.add_argument("-f",
                        "--from-folder",
                        dest = "from_folder",
                        required = True,
                        help = "Folder to apply mappings from. Will use linked.ttl by default, mapping.ttl elsewise."
                       )


    parser.add_argument("-t",
                        "--to-folder",
                        dest = "to_folder",
                        required = True,
                        help = "Folder containing an updated.ttl ontology, on which links will be applied."
                       )

    args = parser.parse_args()

    main(args.from_folder,
         args.to_folder)
