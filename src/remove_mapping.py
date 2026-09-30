"""
Script to remove mappings and everything associated to it (definitions, labels, terms) from the cache.
"""
import json
from argparse import ArgumentParser, ArgumentError

from graph.properties import Properties
from graph.mapping_graph import MappingGraph
from rdflib import SKOS, URIRef, Graph
properties = Properties()
from apply_links import apply_from_mapping
from config import TERM_LABEL_DEF_FILE, CACHE_DIR, OLLAMA_MODEL_NAME

with open(TERM_LABEL_DEF_FILE, "r") as file:
    term_label_def_dict = json.load(file)

def remove_mapping(uri: URIRef,
                   linked_graph: Graph,
                   mapping_graph: MappingGraph):
    """
    Remove a mapping from its URI
    """
    # Get the two entities' original uri to remove them from cache
    for _, _, uri1 in mapping_graph.triples((uri, properties.SSSOM.subject_id, None)):
        pass
    for _, _, uri2 in mapping_graph.triples((uri, properties.SSSOM.object_id, None)):
        pass
    mapping_graph.remove((uri, None, None))

    # Get all the linked entities (to remove def/label/term of every one of them)
    # Remove the exactMatch links from linked
    synset = set()
    for _, _, linked in linked_graph.triples((uri1, SKOS.exactMatch, None)):
        synset.add(linked)
    for _, _, linked in linked_graph.triples((uri2, SKOS.exactMatch, None)):
        synset.add(linked)

    # Remove the response string from the LLM (same + justification)
    remove_term_label_def(synset)

    # De-link uri1 & uri2 from other entities, then re-apply links
    for uri_1 in synset:
        for uri_2 in synset:
            if (uri_1 in [uri1, uri2] or uri_2 in [uri1, uri2]) and uri_1 != uri_2:
                linked_graph.remove((uri_1, SKOS.exactMatch, uri_2))



def save_term_label_def(term_label_def_dict):
    """
    Save the sorted dict on keys so that it is gitable.
    """
    s = sorted(term_label_def_dict)
    res = "{"
    for key in s:
        value = term_label_def_dict[key]
        value_str = ""
        if type(value) == dict:
            for key2 in sorted(value):
                value2 = str(value[key2]).replace('"', '\\"').replace("\n", "\\n")
                if type(value2) == list:
                    value2_str = sorted(value2)
                    value2_str = "[\"" + '",\n    "'.join(value2_str) + "]\""
                else:
                    value2_str = '"' + str(value2) + '"'
                value_str += f"\n  \"{key2}\": {value2_str},"
            value_str = value_str[:-1] + "\n"
            value_str = "{" + value_str + "},\n"
        else:
            value_str = '"' + value + '",\n'
        value_str = value_str[:-2] # remove final ','
        res += f"\"{key}\":{value_str},\n"
    if len(res) > 2: # prevent removing first '{' in case of empty dict
        res = res[:-2] # remove last space & ,
    res += "\n}"

    with open(TERM_LABEL_DEF_FILE, "w") as file:
        file.write(res)


def remove_term_label_def_from_synset_id(synset_id: str):
    """
    Gets the synset's entities' URIs from the synset identifier,
    then remove them from the LLM cache and the term-label-def file.
    """
    synset = set()
    for uri, synset_id2 in term_label_def_dict.items():
        if synset_id2 == synset_id:
            synset.add(uri)

    remove_term_label_def(synset)


def remove_term_label_def(synset: set):
    """
    Remove the term, label, def entry for an identifier
    (in the LLM's cache + in the term_label_def dict)
    """
    # Load the cache & data files
    cache_file = CACHE_DIR / f"{OLLAMA_MODEL_NAME }-generate.json"
    with open(cache_file, "r") as file:
        llm_cache = json.load(file)

    # Remove the generated term/label/def from the current LLMs' cache
    # Remove from TERM_LABEL_DEF_FILE everything associated with this mapping
    for linked in synset:
        key = str(linked)
        if key + ":term" in llm_cache:
            llm_cache.remove(key + ":term")
        if key + ":definition" in llm_cache:
            llm_cache.remove(key + ":definition")
        if key + ":label" in llm_cache:
            llm_cache.remove(key + ":label")
        synset_id = term_label_def_dict.get(key, None)
        if not synset_id:
            continue
        if synset_id in term_label_def_dict:
            term_label_def_dict.remove(synset_id)
        term_label_def_dict.remove(key)
    
    # Re-write cache & term_label_def files
    with open(cache_file, "w") as file:
        json.dump(llm_cache, file)
    save_term_label_def(term_label_def_dict)


def main(mapping_uri: str,
         synset_id: str,
         linked_ttl: str,
         mapping_ttl: str):
    """
    Remove a mapping and all of its associated values (if mapping_uri is set)
    or only remove the values (if synset_id is set instead).

    Args:
        mapping_uri: the URI in the mapping graph.
        synset_id: identifier in the term-label-def json file (in the data/ folder).
        linked_ttl: if 
    """
    if mapping_uri:
        linked_graph = None
        mapping_graph = None
        if linked_ttl:
            linked_graph = Graph()
            linked_graph.parse(linked_ttl)
        else:
            raise ArgumentError("With mapping_uri, you must provide a linked.ttl ontology.")
        if mapping_ttl:
            mapping_graph = MappingGraph(mapping_ttl)
        else:
            raise ArgumentError("With a mapping_uri, you must provide a mapping.ttl ontology.")
        remove_mapping(mapping_uri, linked_graph, mapping_graph)
    elif synset_id:
        remove_term_label_def_from_synset_id(synset_id)
    else:
        raise ArgumentError("You must provide at least one mapping_uri or one synset_id to remove.")


if __name__ == "__main__":
    parser = ArgumentParser(prog = "remove_mapping.py",
                            description = "Remove a mapping and all of its cached information." \
                            "Set this relation as distinct in the LLM's response.")

    parser.add_argument("-u",
                        "--mapping-uri",
                        dest = "mapping_uri",
                        help = "A mapping URI in mapping.ttl. If set, will remove this mapping.",
                        required = False)

    """
    parser.add_argument("-g",
                        "--rem-generated",
                        help = "If set, remove the generated term, label, def for this term.",
                        type = str,
                        required = False)
    """
    parser.add_argument("-l",
                        "--linked-graph",
                        dest = "linked_graph",
                        required = True,
                        type = str,
                        help = "The linked graph")

    parser.add_argument("-m",
                        "--mapping-graph",
                        dest = "mapping_graph",
                        required = True,
                        type = str,
                        help = "The mapping graph")

    args = parser.parse_args()
    main(args.mapping_uri,
         args.linked_graph,
         args.mapping_graph)