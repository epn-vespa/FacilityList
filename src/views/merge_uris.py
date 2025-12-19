"""
Generate a public ontology from the output of disambiguation.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import setup_path
import argparse

from collections import Counter
from graph.graph import Graph
from graph.properties import Properties
from graph.entity import Entity
from graph.extractor.extractor_lists import ExtractorLists
from graph.extractor.wikidata_extractor import WikidataExtractor
from views.generate_csv_json_views import CSVJsonGenerator
from utils.string_utilities import standardize_uri
from utils.dict_utilities import merge_into, majority_voting_merge
from rdflib import Graph as G, URIRef, RDFS, XSD, Literal, SKOS, OWL
from datetime import timezone
from dateutil import parser as dateparser


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
                                  self._graph.PROPERTIES.OBS["label"],
                                  self._graph.PROPERTIES.OBS["type_confidence"],
                                  self._graph.PROPERTIES.OBS["location_confidence"],
                                  self._graph.PROPERTIES.OBS["deprecated"],
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
        for synset in synsets:
            synset_dicts = []
            for s in synset:
                d = dict()
                for key in s._data:
                    d[key] = s.get_values_for(key, extend_to_synonyms = False)
                synset_dicts.append(d)
            # synset_dicts = [s._data for s in synset]
            data = majority_voting_merge(synset_dicts)
            term = standardize_uri(data[SKOS.prefLabel])
            entity = self._graph.PROPERTIES.OBS[term]
            for property, values in data.items():
                if property in self.IGNORE_PROPERTIES:
                    continue
                if not values:
                    continue
                if property in self._graph.PROPERTIES._MAPPING:
                    datatype = self._graph.PROPERTIES._MAPPING[property].get("objtype", None)
                else:
                    datatype = XSD.string
                if type(values) not in (set, tuple, list):
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
                                pass
                            else:
                                value = Literal(value, datatype = datatype)
                        else:
                            if type(value) == tuple and len(value) == 2:
                                lang = value[1]
                                if lang:
                                    value = Literal(value[0], lang = value[1])
                                else:
                                    value = Literal(value[0], datatype = datatype)
                            else:
                                value = Literal(value, datatype = datatype)
                    property = properties.convert_attr(property)
                    self._output_graph.add((entity, property, value))


    def write_ttl(self):
        """
        Write the output ontology
        """
        # Add triples
        # basic classes
        for s, p, o in self._graph.triples((None, RDFS.subClassOf, None)):
            self._output_graph.add((s, p, o))
        # source lists
        for s, _, _ in self._graph.triples((None, None, self._graph.PROPERTIES.OBS["facility-list"])):
            for _, p, o in self._graph.triples((s, None, None)):
                self._output_graph.add((s, p, o))

        with open(self._output_ontology, 'w') as file:
            file.write(self._output_graph.serialize())


def main(input_ontology,
         output_ontology):
    onto_portal = MergeURIs(input_ontology,
                            output_ontology)
    onto_portal.to_synonym_list()
    onto_portal.write_ttl()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog = "onto_portal.py",
        description = "Generate OntoPortal ontologies (obsf, instruments) from a linked ontology.")
    parser.add_argument("-i",
                        "--intput-ontology",
                        dest = "input_ontology",
                        type = str,
                        required = True,
                        help = "Input ontology containing SynonymSets.")

    parser.add_argument("-o",
                        "--outout-ontology",
                        dest = "output_ontology",
                        required = False,
                        default = "output_onto_portal.ttl",
                        help = "Output ontology filename")

    args = parser.parse_args()
    main(args.input_ontology, args.output_ontology)
