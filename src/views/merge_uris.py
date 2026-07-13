"""
Generate an intermediate ontology from the output of disambiguation.
This ontology contains both facilities & instruments.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import argparse
import json
import atexit

from graph.graph import Graph
from graph.properties import Properties
from graph.entity import Entity
from graph.value import Value, ValueSet
from graph.extractor.extractor_lists import ExtractorLists
from views import post_process
from utils.string_utilities import standardize_uri
from utils.dict_utilities import majority_voting_merge
from config import TERM_LABEL_DEF_FILE
from rdflib import Graph as G, URIRef, RDFS, XSD, Literal, SKOS, OWL, PROV
from datetime import timezone
from update import Updater
from llm.llm_connection import LLMConnection
from collections import defaultdict
from uuid import uuid1

properties = Properties()

class MergeURIs():


    def __init__(self,
                 input_ontologies: list[str],
                 output_ontology: str,
                 community_views: list[str] = []):

        self._graph = Graph(input_ontologies)
        self._output_ontology = output_ontology
        self._output_graph = G() # rdflib's Graph
        self._community_views = community_views

        # Bind namespaces
        for prefix, namespace in self._graph.namespaces():
            self._output_graph.bind(prefix, namespace)


        self.IGNORE_PROPERTIES = [# RDF.type, # Added manually
                                  SKOS.exactMatch,
                                  SKOS.narrowMatch,
                                  SKOS.broadMatch,
                                  OWL.differentFrom,
                                  #DCTERMS.modified,
                                  #SKOS.prefLabel,
                                  #SKOS.altLabel,
                                  properties.OBS["label"],
                                  properties.OBS["type_confidence"],
                                  # properties.OBS["location_confidence"], # Needed for the post processing
                                  properties.OBS["deprecated"],
                                 ]


    def get_synsets(self) -> set[frozenset[Entity]]:
        """
        Get all synsets (including entities that are not in a synset
        if they are from an authoritative list)
        """
        synsets = set() # Store synsets & entities

        # Get authoritative lists
        authoritative_extractors = []

        for community in self._community_views:
            authoritative_extractors.extend(Updater.SOURCE_BY_PRIMARY_COMMUNITY[community])

        if not authoritative_extractors:
            authoritative_extractors = ExtractorLists.AUTHORITATIVE_EXTRACTORS
        for extractor in authoritative_extractors:
            for entity in Entity.get_entities_from_list(extractor()):
                entities_uri = entity.get_synonyms()
                entities = {Entity(uri) for uri in entities_uri}
                entities.add(entity)
                synsets.add(frozenset(entities))
        return synsets


    def to_synonym_list(self):
        """
        From synonym sets to synonym list (merged)
        """
        properties = Properties()
        synsets = self.get_synsets()
        term_by_synonym_uri = dict()

        for synset in synsets:
            synset_dicts = []
            all_uris_in_synset = []
            labels_by_source = defaultdict(set)

            # Get synset's data into dicts for each member of the synset,
            # then merge them with majority voting
            for s in synset:
                all_uris_in_synset.append(s.uri)
                d = dict()
                s.add_source_to_attributes()

                for key in s._data:
                    is_label = key in [properties.convert_attr(p) for p in properties._KEEP_PROVENANCE]
                    d[key] = s.get_values_for(key,
                                              extend_to_synonyms = False,
                                              return_raw_value = not is_label)
                synset_dicts.append(d)
            # synset_dicts = [s._data for s in synset]
            data = majority_voting_merge(synset_dicts) # TODO verify that provenances are merged correctly

            # Post-processing of the synset: generate term (IVOA-friendly URI) & pref_label & definition
            # pref label
            old_pref_label = data.get(properties.label)
            old_definition = data.get(properties.definition)
            term, pref_label, definition  = self._get_term_label_def(synset_dicts)

            data[properties.alt_label].add(old_pref_label)
            data[properties.label] = pref_label
            data[properties.description].update(old_definition) # FIXME check that the type is correct (expected bug)

            # definition
            # TODO use the one from post_processing

            # term
            # term = standardize_uri(str(pref_label)) # TODO replace this by a better term generation strategy
            #while term in all_terms:
            #    term += "-bis"
            #all_terms.append(term)

            for uri in all_uris_in_synset:
                term_by_synonym_uri[uri] = term
                term_by_synonym_uri[pref_label] = term
            entity = properties.OBS[term]
            for property, values in data.items():
                if property in self.IGNORE_PROPERTIES:
                    continue
                # convert to str
                property = properties.get_attr_name(property)
                if not values:
                    continue
                if property in properties._MAPPING:
                    datatype = properties._MAPPING[property].get("objtype", None)
                else:
                    datatype = XSD.string
                if type(values) not in (set, tuple, list, ValueSet):
                    values = [values]
                for value in values:
                    if not value:
                        continue
                    if type(value) == URIRef:
                        pass
                    elif type(value) == Literal:
                        pass
                    elif type(value) != Value:
                        if datatype != XSD.string:
                            if datatype == XSD.dateTime:
                                #if type(value) == tuple:
                                #    value = value[0]
                                # dt = dateparser.isoparse(str(value)).astimezone(timezone.utc)
                                dt = value.astimezone(timezone.utc)
                                value = Literal(dt.isoformat(), datatype=XSD.dateTime)
                            elif datatype == URIRef:
                                value = properties.OBS[standardize_uri(value)]
                            else:
                                value = Literal(value, datatype = datatype)
                        else:
                            value = Literal(value, datatype = XSD.string)
                    else:
                        value = value.get_value_node(self._output_graph)
                    property = properties.convert_attr(property)
                    self._output_graph.add((entity, property, value))
        self._term_by_synonym_uri = term_by_synonym_uri
        self._fix_internal_link()


    # Label by synset ID & Synset ID by source
    _term_label_def_dict = {}
    def _load_term_label_def(self):
        if not self._term_label_def_dict and TERM_LABEL_DEF_FILE.exists():
            with open(TERM_LABEL_DEF_FILE, "r") as file:
                self._term_label_def_dict = json.load(file)
        atexit.register(self._save_term_label_def)


    def _save_term_label_def(self):
        with open(TERM_LABEL_DEF_FILE, "r") as file:
            json.dump(self._term_label_def_dict)


    def _get_term_label_def(self,
                            synset_member_uris: list[str],
                            synset_dicts: list[dict],
                            data: dict,
                            all_terms: list[str]) -> tuple[str, str, str]:
        """
        Find a potential term for a synonym set by
        finding acronyms, aperture (m), and checking whether
        it exists in the all_terms history. If it does, it
        will ask the LLM to generate a term.
        Save in dict (and load above for term & definition & label)
        This dictionary can be modified by the user (it should be saved in the data folder, not in cache)
        This dict is then used to:
            - be loaded from any of the URIs in the synset (above)
            - provide the pref label, definition & term for this synset
            - if the synsets are not the same anymore as in this file (split into two synsets in the cache), then we choose one of them and set one as deprecated with use instead to the other one.)


        Args:
            synset_uris: URIs of source (as in linked.ttl)
            data: merged dict
            all_terms: history of all generated terms
        """
        if not self._term_label_def_dict:
            self._load_synset_file()

        term, pref_label, definition = "", "", ""
        for syn_uri in synset_member_uris:
            synset_id = self._term_label_def_dict.get(syn_uri, None)
            if synset_id:
                break
        values = self._term_label_def_dict.get(synset_id, dict())
        term = values.get("term", "")
        pref_label = values.get("label", "")
        definition = values.get("definition", "")
        method_str = values.get("method_str", "")
        if not pref_label:
            pref_label = LLMConnection.generate_label_for_synset(synset = synset_member_uris,
                                                                 merged_data = data,
                                                                 from_cache = True)
            method_str += "label:LLM "

        if not definition:
            definition = LLMConnection.generate_definition_for_synset(synset = synset_member_uris,
                                                                      merged_data = data,
                                                                      from_cache = True)
            method_str += "definition:LLM "

        if not synset_id:
            synset_id = str(uuid1())
        for syn_uri in synset_member_uris:
            self._term_label_def_dict[syn_uri] = synset_id
            self._term_label_def_dict[synset_id] = {"term": term,
                                                    "label": pref_label,
                                                    "definition": definition,
                                                    "method": method_str.strip()}
        return term, pref_label, definition


    def _fix_internal_link(self):
        attrs = [properties.has_part, properties.is_part_of]
        for attr in attrs:
            for old_obj, new_obj in self._term_by_synonym_uri.items():
                for subj, pred, _ in self._output_graph.triples((None, attr, old_obj)):
                    self._output_graph.remove((subj, pred, old_obj))
                    self._output_graph.add((subj, pred, properties.OBS[new_obj]))
            # Remove Wikidata links (for WD hasPart/isPartOf that was not linked to any authoritative list)
            for subj, pred, obj in self._output_graph.triples((None, attr, None)):
                if "wikidata#" in str(obj):
                    self._output_graph.remove((subj, pred, obj))
                if "wikidata#" in str(subj):
                    self._output_graph.remove((subj, pred, obj))
            # TODO do this for every list that is not in community here (instead of in post process)


    def _check_terms_unicity(self,
                             terms: list[str]) -> list:
        """
        Check that terms are unique.

        Returns:
            a list of non-unique terms
        """
        duplicate = set()
        for i, term in enumerate(terms):
            if term in terms[:i]:
                duplicate.add(term)
        return duplicate


    def write_ttl(self):
        """
        Write the output ontology. Before that, complete the basic triples.
        """
        # Add triples
        # basic classes
        for s, p, o in self._graph.triples((None, RDFS.subClassOf, None)):
            self._output_graph.add((s, p, o))
        # source lists
        for s, _, _ in self._graph.triples((None, None, PROV.Entity)):
            for _, p, o in self._graph.triples((s, None, None)):
                self._output_graph.add((s, p, o))

        with open(self._output_ontology, 'w') as file:
            file.write(self._output_graph.serialize())
            print(f"Ontology saved in {self._output_ontology}")


def main(input_ontology,
         output_ontology,
         community_views: list[str] = []):
    """
    Args:
        community_views: list of communities that should be considered as authoritative for this extraction. Values: IHDEA, IVOA, IPDA, OGC.
    """
    merger = MergeURIs(input_ontology,
                       output_ontology,
                       community_views)
    merger.to_synonym_list()
    Graph()._graph = merger._output_graph # Replace graph
    output_graph = Graph()
    post_processor = post_process.PostProcess(output_graph)
    post_processor()
    merger.write_ttl()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog = "onto_portal.py",
        description = "Generate OntoPortal ontologies (obsf, instruments) from a linked ontology.")
    parser.add_argument("-i",
                        "--intput-ontology",
                        dest = "input_ontology",
                        type = str,
                        required = True,
                        help = "Input ontology (that has been mapped and contains exactMatch relations).")

    parser.add_argument("-o",
                        "--outout-ontology",
                        dest = "output_ontology",
                        required = False,
                        default = "output_onto_portal.ttl",
                        help = "Output ontology filename (OntoPortal format)")

    parser.add_argument("-c",
                        "--community-views",
                        dest = "community_views",
                        type = str,
                        nargs = "*",
                        choices = set(Updater.SOURCE_BY_PRIMARY_COMMUNITY),
                        help = "Consider lists for which the primary community (or alliance) is amongst this list as authoritative, and ignore synonym sets (or lonely entities) that did not match with any entity in these lists.")

    args = parser.parse_args()
    main(args.input_ontology,
         args.output_ontology,
         args.community_views)
