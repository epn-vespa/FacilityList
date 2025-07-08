#!/bin/python3
"""
Transform the Synonym Sets from a merged ontology and generate a dictionary csv file.
https://www.ivoa.net/documents/Vocabularies/20230206/REC-Vocabularies-2.1.html#tth_sEcA.1

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import setup_path # import first

from data_updater.extractor.wikidata_extractor import WikidataExtractor
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
            # "url": "owl:equivalentClass",
            "uri": "skos:sameAs",
            # "alt_label": "skos:altLabel",
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
        value_set = entity.get_values_for(relation, language = "en")
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


    def standardize_label_format(self,
                                 label: str) -> str:
        """
        Standardize label format for labels that are only
        slightly different from other labels.
        - Replace '_' by space
        """
        label = label.replace('_', ' ')
        return label


    def get_pref_label(self,
                       synset: SynonymSet):
        """
        Returns the pref label (str) and all other labels (set)

        Keyword arguments:
        synset -- a Synonym Set
        """
        pref_label = None
        all_labels = []
        identifiers = set() # Cannot be used as main label but should appear in aliases

        for synonym in synset:
            label = synonym.get_values_for("label", unique = True, language = "en")
            all_labels.append(label)
            alt_labels = synonym.get_values_for("alt_label", language = "en")
            all_labels.extend(alt_labels)
            # Standardize label format
            all_labels = [self.standardize_label_format(label) for label in all_labels]
            source = synonym.get_values_for("source", unique = True)
            if source.rsplit('#')[-1] == WikidataExtractor.URI:
                pref_label = label
            identifiers.update(synonym.get_values_for("MPC_ID"))
            identifiers.update(synonym.get_values_for("NAIF_ID"))
            identifiers.update(synonym.get_values_for("NSSDCA_ID"))
            identifiers.update(synonym.get_values_for("COSPAR_ID"))

        # Get the shortest label from the most represented labels
        # Get the label with the most letters from the most represented labels
        if not pref_label:
            if not set(all_labels).issubset(identifiers):
                # Remove identifiers from labels that may become pref label
                all_labels = [label for label in all_labels if label not in identifiers]
            count_labels = Counter(all_labels)
            labels = sorted(count_labels.items(), key = lambda x: x[1], reverse = True)
            candidate_labels = [label for label, count in labels if count == labels[0][1]]
            #candidate_labels = {label: len(label) for label in candidate_labels}
            # Candidate label with the most letters
            candidate_labels = {label: len(re.findall(r"[a-zA-Z]", label)) for label in candidate_labels}
            labels = sorted(candidate_labels.items(), key = lambda x: x[1], reverse = True)
            labels = [label[0] for label in labels]
            pref_label = labels[0]
            if '(' in pref_label:
                clean_pref_label = pref_label
                while '(' in clean_pref_label:
                    # Repeat for each parenthesis
                    clean_pref_label = clean_pref_label[0:clean_pref_label.find('(')] + ' ' + clean_pref_label[clean_pref_label.find(')') + 1:]
                clean_pref_label = re.sub(' +', ' ', clean_pref_label).strip()
                if clean_pref_label:
                    pref_label = clean_pref_label
                    all_labels.append(pref_label)
        identifiers.update({self.standardize_label_format(code) for code in synonym.get_values_for("code")})
        all_labels = set(all_labels)
        all_labels.update(identifiers)
        return pref_label, all_labels


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

        for synset in synsets:
            pref_label, all_labels = self.get_pref_label(synset)

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

            more_relations = ""
            for entity in synset:
                # Only get relations that are not internal references
                # in the first place
                # After all external relations have a pref "term", we can
                # restore internal relations using term_by_synonym_uri.
                fields = [#"alt_label",
                          "uri"]
                for field in fields:
                    more_relations += self._to_string(entity, relation = field)
            # altLabel is not yet supported by IVOA
            #for alt_label in alt_labels:
            #    more_relations += f'skos:altLabel("{alt_label}") '

            # Json
            json_res[term] = list(all_labels)

            # CSV
            csv_res[term] = {"level": None, "label": pref_label, "description": description, "more_relations": more_relations, "synset": synset}

        self.term_by_synonym_uri = term_by_synonym_uri
        self.json_res = json_res
        self.csv_res = csv_res


    def sort_csv(self) -> list[str]:
        """
        Sort CSV by hierarchy of broaders and use different levels.
        self.csv_res is transformed into a list of dictionaries.
        """
        # First, retrieve broader entities
        for term, values in self.csv_res.items():

            more_relations = values["more_relations"]

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
                        # Only keep parts that will be in the CSV
                        part_of = self.term_by_synonym_uri.get(part, None)
                        if part_of:
                            #    csv_res_str += f"{self._IVOA_RELATIONS[relation]}({part_of}) "
                            values["more_relations"] += f"{self._IVOA_RELATIONS[relation]}({part_of}) "

        children_by_broader = defaultdict(list)
        broader_by_children = defaultdict(list)

        # First, we feed the children & broader dicts
        for term, values in self.csv_res.items():
            more_relations = values["more_relations"].split(' ')
            for relation in more_relations:
                if relation.startswith('skos:broader'):
                    broader = re.findall(r'\((.+)\)', relation)[0]
                    children_by_broader[broader].append(term)
                    broader_by_children[term].append(broader)
                    # Remove broader from more_relations
                    values["more_relations"] = values["more_relations"].replace(relation, "") # Remove this broader relation

        csv_res = []
        already_in = set()
        for broader in children_by_broader.copy().keys():
            if broader in already_in:
                continue
            self.get_recursive(children_by_broader, broader, csv_res, already_in)
        for term, entity in self.csv_res.items():
            entity["term"] = term
            entity["level"] = 1
            csv_res.append(entity)
        self.csv_res = csv_res



    def get_recursive(self, children_by_broader, broader, csv_res: list, already_in: set, level: int = 1):
        entity = self.csv_res.pop(broader)
        entity["level"] = level
        entity["term"] = broader
        csv_res.append(entity)
        already_in.add(broader)
        for child in children_by_broader.pop(broader, []):
            if child in already_in:
                continue
            self.get_recursive(children_by_broader, child, csv_res, already_in, level + 1)



    def write_json_csv(self):
        with open(self._output_json, "w", encoding = "utf-8") as file:
            json.dump(self.json_res, file, indent = 2, ensure_ascii = False)

        with open(self._output_csv, "w") as file:
            csv_res_str = ""
            #for term, values in self.csv_res.items():
            for values in self.csv_res:
                term = values["term"]
                level = values["level"]
                label = values["label"]
                description = values["description"]
                more_relations = values["more_relations"]
                csv_res_str += f'"{term}";{level};"{label}";"{description}";{more_relations}'
                csv_res_str += "\n"
            file.write(re.sub(r" +", " ", csv_res_str))


def main(input_ontologies: list[str],
         output_csv: str,
         output_json: str):
    csv_generator = CSVJsonGenerator(input_ontologies,
                                     output_csv,
                                     output_json)
    csv_generator.to_synonym_list()
    csv_generator.sort_csv()
    csv_generator.write_json_csv()


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
