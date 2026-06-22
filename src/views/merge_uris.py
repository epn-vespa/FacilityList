"""
Generate an intermediate ontology from the output of disambiguation.
This ontology contains both facilities & instruments.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import argparse

from graph.graph import Graph
from graph.properties import Properties
from graph.entity import Entity
from graph.value import Value
from graph.extractor.extractor_lists import ExtractorLists
from views import post_process
from utils.string_utilities import standardize_uri
from utils.dict_utilities import majority_voting_merge
from rdflib import Graph as G, URIRef, RDFS, XSD, Literal, SKOS, OWL, PROV
from datetime import timezone
from update import Updater
from llm.llm_connection import LLMConnection
from collections import defaultdict

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
            data = majority_voting_merge(synset_dicts) # TODO merge the labels that are the same, adding more provenances.

            old_pref_label = data.get(properties.label)
            pref_label = LLMConnection.generate_label_for_synset(synset = synset,
                                                                 merged_data = data,
                                                                 from_cache = True)
            data[properties.alt_label].add(old_pref_label)
            data[properties.label] = pref_label
            term = standardize_uri(str(pref_label)) # TODO replace this by a better term generation strategy
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
                if type(values) not in (set, list):
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
