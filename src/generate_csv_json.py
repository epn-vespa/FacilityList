#!/bin/python3
"""
Transform the Synonym Sets from a merged ontology and generate a dictionary csv file.
https://www.ivoa.net/documents/Vocabularies/20230206/REC-Vocabularies-2.1.html#tth_sEcA.1

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import setup_path # import first
from argparse import ArgumentParser
from collections import Counter
import json

from data_merger.entity import Entity
from graph import Graph
from data_merger.synonym_set import SynonymSetManager, SynonymSet
from data_updater.extractor.extractor_lists import ExtractorLists


class CSVJsonGenerator():


    def __init__(self,
                 input_ontologies: list[str],
                 output_csv: str,
                 output_json: str):

        self._graph = Graph(input_ontologies)
        self._output_csv = output_csv
        self._output_json = output_json
        self._SSM = SynonymSetManager()


    def get_synsets(self) -> set[set[Entity]]:
        """
        Get all synsets & entities that are not in a synset
        if they are from an authoritative list.
        """
        synsets = set() # Store synsets & entities
        # Get authoritative lists
        for extractor in ExtractorLists.AUTHORITATIVE_EXTRACTORS:
            for entity_uri, synset_uri in self._graph.get_entities_from_list(extractor()):
                if synset_uri:
                    synset = SynonymSet(uri = synset_uri)
                    entities = synset.synonyms
                    synsets.add(frozenset(entities))
                else:
                    entity = Entity(uri = entity_uri)
                    synsets.add(frozenset({entity}))
        return synsets


    def to_entity_dict(self,
                       synsets: set[set[Entity]]) -> dict[Entity]:
        """
        Flatten the synonym sets into a dictionary:
            Entity: {synonyms of this Entity}

        Keyword arguments:
        synsets -- the Synonym sets extracted with get_synsets()
        """
        res = dict()
        for synset in synsets:
            for entity in synset:
                res[entity] = synset - {entity}
        return res


    def to_csv(self):
        """
        Generates the output CSV file:
        term, level, label, description, more_relations
        """
        g = self._graph
        res = ""
        for extractor in ExtractorLists.AVAILABLE_EXTRACTORS:
            for entity_uri, _ in self._graph.get_entities_from_list(extractor()):
                entity = Entity(entity_uri)
                term = entity_uri.rsplit("#")[-1]
                level = 1
                label = entity.get_values_for("label", unique = True)
                description = entity.get_values_for("description", unique = True)
                if not description:
                    description = entity.get_values_for("definition", unique = True)
                if not description:
                    description = ""
                description = description[:300] # Shorten description
                more_relations = ""
                more_relations += self._to_string(entity, relation = "is_part_of")
                more_relations += self._to_string(entity, relation = "has_part")
                res += f'{term};1;"{label}";"{description}";{more_relations}\n'

        with open(self._output_csv, "w") as file:
            file.write(res)


    def _to_string(self,
                   entity: Entity,
                   relation: str):
        """
        Get the IVOA string for the more_relations field
        (ex: skos:exactMatch(value))

        Keyword arguments:
        relation -- get values for this predicate
        """
        res = ""
        value_set = entity.get_values_for(relation)
        relation = str(Graph().OM.convert_attr(relation))
        for value in value_set:
            if value is not None:
                res += f"{relation}({value}) "
        return res


    
    def to_synonym_list(self):
        """
        Generate the Synonym list in a json format
        """
        res = {}
        synsets = self.get_synsets()
        for entity, synonyms in self.to_entity_dict(synsets).items():
            all_labels = []
            for synonym in synonyms:
                label = synonym.get_values_for("label", unique = True, language = "en")
                all_labels.append(label)
                alt_labels = synonym.get_values_for("alt_label", language = "en")
                all_labels.extend(alt_labels)
            label = entity.get_values_for("label", unique = True, language = "en")
            all_labels.append(label)
            alt_labels = entity.get_values_for("alt_label", language = "en")
            all_labels.extend(alt_labels)

            count_labels = Counter(all_labels)
            labels = sorted(count_labels, key = lambda x: x[1], reverse = True)
            pref_label = labels[0]
            alt_labels = set(all_labels) - {pref_label}
            res[pref_label] = list(alt_labels)
        with open(self._output_json, "w", encoding = "utf-8") as file:
            json.dump(res, file, indent = 2, ensure_ascii = False)


def main(input_ontologies: list[str],
         output_csv: str,
         output_json: str):
    csv_generator = CSVJsonGenerator(input_ontologies,
                                     output_csv,
                                     output_json)
    csv_generator.to_synonym_list()
    csv_generator.to_csv()


if __name__ == "__main__":

    parser = ArgumentParser(
        prog = "generate_csv.py",
        description = "Transform the Synonym Sets from a merged ontology and generate a dictionary csv file")

    parser.add_argument("-i",
                        "--input-ontologies",
                        dest = "input_ontologies",
                        nargs = "+",
                        default = [],
                        type = str,
                        required = True,
                        help = "Input ontology or ontologies to extract the json & csv from. Must contain SynonymSet instances.")

    parser.add_argument("-c",
                        "--output-csv",
                        dest = "output_csv",
                        default = "output.csv",
                        type = str,
                        required = False,
                        help = "Output csv file to save the IVOA list.")

    parser.add_argument("-j",
                        "--output-json",
                        dest = "output_json",
                        default = "output.json",
                        type = str,
                        required = False,
                        help = "Output json file to save the synonyms dictionary.")

    args = parser.parse_args()

    main(args.input_ontologies,
         args.output_csv,
         args.output_json)