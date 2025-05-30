#!/bin/python3
"""
Transform the Synonym Sets from a merged ontology and generate a dictionary csv file.
https://www.ivoa.net/documents/Vocabularies/20230206/REC-Vocabularies-2.1.html#tth_sEcA.1

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import setup_path # import first
from argparse import ArgumentParser
from collections import Counter, defaultdict
import json
import re

from data_merger.entity import Entity
from graph import Graph
from rdflib import URIRef
from data_merger.synonym_set import SynonymSetManager, SynonymSet
from data_updater.extractor.extractor_lists import ExtractorLists
from utils.utils import standardize_uri


class CSVJsonGenerator():


    # Mapping between IVOA authorized relations and Obs Facilities relations
    _IVOA_RELATIONS = {
            "is_part_of": "skos:broader",
            "url": "owl:equivalentClass",
            "uri": "owl:equivalentClass",
            "alt_label": "skos:altLabel",
            }


    def __init__(self,
                 input_ontologies: list[str],
                 output_csv: str,
                 output_json: str):

        self._graph = Graph(input_ontologies)
        self._output_csv = output_csv
        self._output_json = output_json
        self._SSM = SynonymSetManager()


    def get_synsets(self) -> set[frozenset[Entity]]:
        """
        Get all synsets (including entities that are not in a synset
        if they are from an authoritative list)
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


    def _to_string(self,
                   entity: Entity,
                   relation: str):
        """
        Get the IVOA string for the more_relations field
        (ex: skos:altLabel("value"))
        Only get relations that are not internal refs (e.g. objtype is URIRef).

        Keyword arguments:
        entity -- the entity for this row
        relation -- get values for this predicate
        relation_str -- if set, use this as a relation
        """
        res = ""
        value_set = entity.get_values_for(relation)
        if self._graph.OM._MAPPING[relation].get("objtype") == URIRef:
            return ""
        relation = self._IVOA_RELATIONS[relation]
        for value in value_set:
            if value is not None:
                if type(value) == str:
                    res += f'{relation}("{value}") '
                else:
                    res += f'{relation}({value}) '
        return res


    def to_synonym_list(self):
        """
        Generate the Synonym list in a json format:
        {"term": [aliases]}
        aliases includes the prefLabel. Term is a standardized uri
        of the prefLabel.
        The prefLabel is selected with the maximum count of labels.
        """
        json_res = {}
        csv_res = {}
        synsets = self.get_synsets()
        term_by_synonym_uri = dict() # used to find the term if we know the original URI
        all_uris = 0
        # TODO keep track of hasPart & isPartOf between synsets (intermediate step?)
        for synset in synsets:
            all_labels = []
            identifiers = set() # Cannot be used as main label but should appear in aliases
            # TODO see if we can merge both functions ? (also write the csv)
            for synonym in synset:
                all_uris += 1
                label = synonym.get_values_for("label", unique = True, language = "en")
                all_labels.append(label)
                alt_labels = synonym.get_values_for("alt_label", language = "en")
                all_labels.extend(alt_labels)
                identifiers.update(synonym.get_values_for("code"))
                identifiers.update(synonym.get_values_for("MPC_ID"))
                identifiers.update(synonym.get_values_for("NAIF_ID"))
                identifiers.update(synonym.get_values_for("NSSDCA_ID"))
                identifiers.update(synonym.get_values_for("COSPAR_ID"))

            count_labels = Counter(all_labels)
            # Remove identifiers from labels
            """
            if not all(label in identifiers for label in count_labels):
                for label in count_labels.copy():
                    if label in identifiers:
                        del count_labels[label]
            """
            labels = sorted(count_labels, key = lambda x: x[1], reverse = True)
            pref_label = labels[0]
            all_labels = set(all_labels)
            all_labels.update(identifiers)

            # Term
            term = standardize_uri(pref_label)
            for synonym in synset:
                uri = synonym.uri
                uri = str(uri)
                term_by_synonym_uri[uri] = term

            # Level
            level = 1

            # Description
            description = ""
            for entity in synset:
                if description:
                    break # Only keep one description
                description = entity.get_values_for("description", unique = True)
                if not description:
                    description = entity.get_values_for("definition", unique = True)
                if not description: # may have returned None
                    description = ""
            description = description.replace("\n", "")[:500]

            # More relations (for IVOA csv)
            """
            ivoasem:preliminary,
            ivoasem:deprecated,
            ivoasem:useInstead,
            rdfs:subClassOf,
            rdfs:subPropertyOf,
            skos:broader,
            skos:exactMatch,
            skos:related,
            skos:altLabel
            """
            more_relations = ""
            for entity in synset:
                # Only get relations that are not internal references
                # in the first place
                # After all external relations have a pref "term", we can
                # restore internal relations using term_by_synonym_uri.
                fields = ["alt_label", "uri"]
                for field in fields:
                    more_relations += self._to_string(entity, relation = field)
            for alt_label in alt_labels:
                more_relations += f'skos:altLabel("{alt_label}") '

            # Json
            json_res[term] = list(all_labels)

            # CSV
            csv_res[term] = {"level": 1, "label": label, "description": description, "more_relations": more_relations, "synset": synset}

        with open(self._output_json, "w", encoding = "utf-8") as file:
            json.dump(json_res, file, indent = 2, ensure_ascii = False)

        with open(self._output_csv, "w") as file:
            csv_res_str = ""
            for term, values in csv_res.items():
                level = values["level"]
                label = values["label"]
                description = values["description"]
                more_relations = values["more_relations"]
                csv_res_str += f'"{term}";{level};"{label}";"{description}";{more_relations}'

                # Finally, get relations that are internal references
                synset = values["synset"] # From synset, get internal relations
                for entity in synset:
                    internal_relations = [
                            "is_part_of",
                            #"has_part",
                            ]
                    for relation in internal_relations:
                        parts = entity.get_values_for(relation)
                        for part in parts:
                            part_of = term_by_synonym_uri.get(part, None)
                            if part_of:
                                csv_res_str += f"{self._IVOA_RELATIONS[relation]}({part_of}) "
                csv_res_str += "\n"
            file.write(re.sub(r" +", " ", csv_res_str))


def main(input_ontologies: list[str],
         output_csv: str,
         output_json: str):
    csv_generator = CSVJsonGenerator(input_ontologies,
                                     output_csv,
                                     output_json)
    csv_generator.to_synonym_list()


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
