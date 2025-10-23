"""
Generate a public ontology from the output of disambiguation (merge.py).

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import setup_path
import argparse

from collections import Counter
from graph.graph import Graph
from graph.entity import Entity
from graph.extractor.extractor_lists import ExtractorLists
from graph.extractor.wikidata_extractor import WikidataExtractor
from views.generate_csv_json_views import CSVJsonGenerator
from utils.string_utilities import standardize_uri
from utils.dict_utilities import merge_into
from rdflib import Graph as G, URIRef, RDF, RDFS, XSD, Literal, SKOS, DCTERMS
from datetime import timezone


class OntoPortal(CSVJsonGenerator):

    def __init__(self,
                 input_ontologies: list[str],
                 output_ontology: str):

        self._graph = Graph(input_ontologies)
        self._output_ontology = output_ontology
        super().__init__(input_ontologies, None, None)
        self._output_graph = G() # rdflib's Graph

        # Bind namespaces
        for prefix, namespace in self._graph.namespaces():
            self._output_graph.bind(prefix, namespace)


        self.IGNORE_PROPERTIES = [# RDF.type, # Added manually
                                  SKOS.exactMatch,
                                  DCTERMS.modified,
                                  #SKOS.prefLabel,
                                  #SKOS.altLabel,
                                  self._graph.PROPERTIES.OBS["label"],
                                  self._graph.PROPERTIES.OBS["type_confidence"],
                                  self._graph.PROPERTIES.OBS["location_confidence"],
                                 ]


    def get_synsets(self) -> set[frozenset[Entity]]:
        """
        Get all synsets (including entities that are not in a synset
        if they are from an authoritative list)
        """
        synsets = set() # Store synsets & entities
        # Get authoritative lists
        for extractor in ExtractorLists.AUTHORITATIVE_EXTRACTORS:
            for entity_uri, in self._graph.get_entities_from_list(extractor()):
                entity = Entity(uri = entity_uri)
                entities_uri = entity.get_synonyms()
                entities = {Entity(uri) for uri in entities_uri}
                entities.add(entity)
                synsets.add(frozenset(entities))
        return synsets


    def to_synonym_list(self):
        """
        Overrides super's to_synonym_list
        """
        synsets = self.get_synsets()
        term_by_synonym_uri = dict()
        all_uris = 0
        for synset in synsets:
            all_labels = []
            pref_label = None
            identifiers = set() # Cannot be used as main label but should appear in aliases
            for synonym in synset:
                all_uris += 1
                label = synonym.get_values_for("label", unique = True, languages = "en")
                all_labels.append(label)
                alt_labels = synonym.get_values_for("alt_label", languages = "en")
                all_labels.extend(alt_labels)
                source = synonym.get_values_for("source", unique = True)
                if source == WikidataExtractor.URI:
                    pref_label = label
                identifiers.update(synonym.get_values_for("code"))
                identifiers.update(synonym.get_values_for("MPC_ID"))
                identifiers.update(synonym.get_values_for("NAIF_ID"))
                identifiers.update(synonym.get_values_for("NSSDCA_ID"))
                identifiers.update(synonym.get_values_for("COSPAR_ID"))

            # Get the shortest label from the most represented labels
            if not pref_label:
                if not set(all_labels).issubset(identifiers):
                    # Remove identifiers from labels that may become pref label
                    all_labels = [label for label in all_labels if label not in identifiers]
                count_labels = Counter(all_labels)
                labels = sorted(count_labels.items(), key = lambda x: x[1], reverse = True)
                candidate_labels = [label for label, count in labels if count == labels[0][1]]
                candidate_labels = {label: len(label) for label in candidate_labels}
                labels = sorted(candidate_labels.items(), key = lambda x: x[1], reverse = True)
                labels = [label[0] for label in labels]
                pref_label = labels[0]
            all_labels = set(all_labels)
            # all_labels.update(identifiers)

            # Term
            term = standardize_uri(pref_label)
            """
            for synonym in synset:
                uri = synonym.uri
                uri = str(uri)
                term_by_synonym_uri[uri] = term
            """

            # Data
            data = dict()
            for synonym in synset:
                data_ = synonym._data
                merge_into(data, data_)
            entity = self._graph.PROPERTIES.OBS[term]
            # self._output_graph.add((entity, SKOS.prefLabel, Literal(pref_label)))
            added_pref_label = False
            for property, values in data.items():
                if property in self.IGNORE_PROPERTIES:
                    continue
                if property in self._graph.PROPERTIES._MAPPING:
                    datatype = self._graph.PROPERTIES._MAPPING[property].get("objtype", XSD.string)
                else:
                    datatype = XSD.string
                if not values:
                    continue
                for value in values:
                    if not value:
                        continue
                    if type(value) == URIRef:
                        pass
                    else:
                        if datatype != XSD.string:
                            if datatype == XSD.dateTime:
                                
                                dt = parser.isoparse(str(value)).astimezone(timezone.utc)
                                value = Literal(dt.isoformat(), datatype=XSD.dateTime)
                            elif datatype == URIRef:
                                pass
                            else:
                                value = Literal(value[0], datatype = datatype)
                        else:
                            if type(value) == tuple and len(value) == 2:
                                value = Literal(value[0], lang = value[1])
                            else:
                                value = Literal(value)
                            # Restore prefLabel & altLabel
                            if property == SKOS.prefLabel:
                                if str(value) != pref_label:
                                    property = SKOS.altLabel
                                #else:
                                #    continue
                            elif property == SKOS.altLabel:
                                if str(value) == pref_label:
                                    property = SKOS.prefLabel
                    if property == SKOS.prefLabel:
                        added_pref_label = True
                    self._output_graph.add((entity, property, value))

            if not added_pref_label:
                self._output_graph.add((entity, SKOS.prefLabel, Literal(pref_label)))


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
    onto_portal = OntoPortal(input_ontology,
                             output_ontology)
    onto_portal.to_synonym_list()
    onto_portal.write_ttl()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog = "onto_portal.py",
        description = "Generate a public ontology from the output of disambiguation (merge.py).")
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
