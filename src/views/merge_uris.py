"""
Generate a public ontology from the output of disambiguation.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import setup_path
import argparse

from graph.graph import Graph
from graph.properties import Properties
from graph.entity import Entity
from graph.extractor.extractor_lists import ExtractorLists
from utils.string_utilities import standardize_uri
from utils.dict_utilities import majority_voting_merge
from rdflib import Graph as G, URIRef, RDFS, XSD, Literal, SKOS, OWL, DCTERMS
from datetime import timezone

properties = Properties()

class MergeURIs():


    def __init__(self,
                 input_ontologies: list[str],
                 output_ontology: str):

        self._graph = Graph(input_ontologies)
        self._output_ontology = output_ontology
        self._output_graph = G() # rdflib's Graph

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
                                  properties.OBS["location_confidence"],
                                  properties.OBS["deprecated"],
                                 ]


    def get_synsets(self) -> set[frozenset[Entity]]:
        """
        Get all synsets (including entities that are not in a synset
        if they are from an authoritative list)
        """
        synsets = set() # Store synsets & entities
        # Get authoritative lists
        for extractor in ExtractorLists.AUTHORITATIVE_EXTRACTORS:
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
            for s in synset:
                all_uris_in_synset.append(s.uri)
                d = dict()
                for key in s._data:
                    return_language = False
                    if key in [SKOS.altLabel, SKOS.prefLabel]:
                        return_language = True
                    d[key] = s.get_values_for(key,
                                              extend_to_synonyms = False,
                                              return_language = return_language)
                synset_dicts.append(d)
            # synset_dicts = [s._data for s in synset]
            data = majority_voting_merge(synset_dicts)
            pref_label = data[SKOS.prefLabel]
            if type(pref_label) == tuple and len(pref_label) == 2:
                pref_label = pref_label[0]
            term = standardize_uri(pref_label)
            #while term in all_terms:
            #    term += "-bis"
            #all_terms.append(term)
            for uri in all_uris_in_synset:
                term_by_synonym_uri[str(uri)] = term
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
                    else:
                        if datatype != XSD.string:
                            if datatype == XSD.dateTime:
                                if type(value) == tuple:
                                    value = value[0]
                                # dt = dateparser.isoparse(str(value)).astimezone(timezone.utc)
                                dt = value.astimezone(timezone.utc)
                                value = Literal(dt.isoformat(), datatype=XSD.dateTime)
                            elif datatype == URIRef:
                                value = properties.OBS[value]
                            else:
                                value = Literal(value, datatype = datatype)
                        else:
                            if type(value) == tuple and len(value) == 2:
                                lang = value[1]
                                if lang:
                                    value = Literal(value[0], lang = lang)
                                else:
                                    value = Literal(value[0], datatype = datatype)
                            else:
                                value = Literal(value, datatype = datatype)
                    property = properties.convert_attr(property)
                    self._output_graph.add((entity, property, value))
        self._term_by_synonym_uri = term_by_synonym_uri
        self._fix_internal_link()


    def _fix_internal_link(self):
        attrs = [DCTERMS.hasPart, DCTERMS.isPartOf]
        for attr in attrs:
            for old_obj, new_obj in self._term_by_synonym_uri.items():
                old_obj = URIRef(old_obj)
                for subj, pred, _ in self._output_graph.triples((None, attr, old_obj)):
                    #print("!!!!found!", subj, pred, old_obj, "with new obj:", new_obj)
                    self._output_graph.remove((subj, pred, old_obj))
                    self._output_graph.add((subj, pred, Properties().OBS[new_obj]))
            # Remove Wikidata links (for WD hasPart/isPartOf that was not linked to any authoritative list)
            for subj, pred, obj in self._output_graph.triples((None, attr, None)):
                if "wikidata#" in str(obj):
                    print("removing:", obj)
                    self._output_graph.remove((subj, pred, obj))
                if "wikidata#" in str(subj):
                    print("removing:", subj)
                    self._output_graph.remove((subj, pred, obj))


    def write_ttl(self):
        """
        Write the output ontology. Before that, complete the basic triples.
        """
        # Add triples
        # basic classes
        for s, p, o in self._graph.triples((None, RDFS.subClassOf, None)):
            self._output_graph.add((s, p, o))
        # source lists
        for s, _, _ in self._graph.triples((None, None, properties.OBS["facility-list"])):
            for _, p, o in self._graph.triples((s, None, None)):
                self._output_graph.add((s, p, o))

        with open(self._output_ontology, 'w') as file:
            file.write(self._output_graph.serialize())
            print(f"Ontology saved in {self._output_ontology}")


def main(input_ontology,
         output_ontology):
    merger = MergeURIs(input_ontology,
                       output_ontology)
    merger.to_synonym_list()
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
    args = parser.parse_args()
    main(args.input_ontology,
         args.output_ontology)
