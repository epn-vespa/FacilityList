"""
Generate a public ontology from the output of disambiguation.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import setup_path
import argparse
import json
import re

from collections import Counter, defaultdict
from graph.graph import Graph
from graph.properties import Properties
from graph.entity import Entity
from graph.extractor.extractor_lists import ExtractorLists
from views.generate_csv_json_views import CSVJsonGenerator
from utils.string_utilities import standardize_uri
from utils.dict_utilities import merge_into, majority_voting_merge
from rdflib import Graph as G, URIRef, RDFS, RDF, XSD, Literal, SKOS, OWL
from datetime import timezone
from dateutil import parser as dateparser


class MergeURIs():

    # Mapping between IVOA relations and Obs Facilities relations
    _IVOA_RELATIONS = {
            "is_part_of": "skos:broader",
            # "url": "owl:equivalentClass",
            "uri": "skos:sameAs",
            # "alt_label": "skos:altLabel",
            }


    def __init__(self,
                 input_ontologies: list[str],
                 output_ontology: str,
                 output_csv: str,
                 output_json: str):

        self._graph = Graph(input_ontologies)
        self._output_ontology = output_ontology
        self._output_csv = output_csv
        self._output_json = output_json
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
        term_by_synonym_uri = dict()

        for synset in synsets:
            synset_dicts = []
            for s in synset:
                d = dict()
                for key in s._data:
                    d[key] = s.get_values_for(key, extend_to_synonyms = False)
                synset_dicts.append(d)
            # synset_dicts = [s._data for s in synset]
            data = majority_voting_merge(synset_dicts)
            uri = data[SKOS.prefLabel]
            term = standardize_uri(uri)
            term_by_synonym_uri[uri] = term
            entity = self._graph.PROPERTIES.OBS[term]
            for property, values in data.items():
                # convert to str
                property = self._graph.PROPERTIES.get_attr_name(property)
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


    def _to_string(self,
                   entity: Entity,
                   relation: str):
        """
        Get the IVOA string for the more_relations field
        (ex: skos:altLabel("value"))
        Only get relations that are not internal refs (e.g. objtype is URIRef).

        Args:
            entity: the entity for this row
            relation: get values for this predicate
            relation_str: if set, use this as a relation
        """
        res = ""
        value_set = entity.get_values_for(relation, languages = "en")
        if self._graph.PROPERTIES._MAPPING[relation].get("objtype") == URIRef:
            return ""
        relation = self._IVOA_RELATIONS[relation]
        for value in value_set:
            if value is not None:
                if type(value) == str:
                    res += f'{relation}("{value}") '
                else:
                    res += f'{relation}({value}) '
        return res



    def _sort_csv(self):
        """
        Sort CSV by hierarchy of broaders and use different levels.
        The res_csv is transformed into a list of dictionaries.
        """
        # First, retrieve broader entities
        for term, values in self._res_csv.items():
            more_relations = values["more_relations"]

            # Finally, get relations that are internal references
            entity = values["entity"] # From synset, get internal relations
            internal_relations = [
                    "is_part_of",
                    #"has_part",
                    ]
            for relation in internal_relations:
                parts = entity.get_values_for(relation)
                print("parts=", parts)
                for part in parts:
                    # Only keep parts that will be in the CSV
                    part = str(part)
                    print("part=", part)
                    part_of = self._term_by_synonym_uri.get(part, None)
                    print("part of found?:", part_of)
                    print("not found from:", self._term_by_synonym_uri)
                    if part_of:
                        #    res_csv_str += f"{self._IVOA_RELATIONS[relation]}({part_of}) "
                        values["more_relations"] += f"{self._IVOA_RELATIONS[relation]}({part_of}) "

        children_by_broader = defaultdict(list)

        # First, we feed the children & broader dicts
        for term, values in self._res_csv.items():
            more_relations = values["more_relations"].split(' ')
            for relation in more_relations:
                if relation.startswith('skos:broader'):
                    broader = re.findall(r'\((.+)\)', relation)[0]
                    print("found broader:", relation, broader)
                    children_by_broader[broader].append(term)
                    # Remove broader from more_relations
                    values["more_relations"] = values["more_relations"].replace(relation, "") # Remove this broader relation
                    break
        res_csv = []
        already_in = set()
        print("children by broader=", children_by_broader)
        for broader in children_by_broader.copy().keys():
            if broader in already_in:
                continue
            self._get_recursive(children_by_broader, broader, res_csv, already_in)
        for term, entity in self._res_csv.items():
            entity["term"] = term
            entity["level"] = 1
            res_csv.append(entity)
        self._res_csv = res_csv


    def _get_recursive(self, children_by_broader, broader, res_csv: list, already_in: set, level: int = 1):
        entity = self._res_csv.pop(broader)
        entity["level"] = level
        entity["term"] = broader
        res_csv.append(entity)
        already_in.add(broader)
        for child in children_by_broader.pop(broader, []):
            if child in already_in:
                continue
            self._get_recursive(children_by_broader, child, res_csv, already_in, level + 1)


    def write_ttl(self):
        """
        Write the output ontology. Before that, complete the basic triples.
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


    def generate_json_csv(self):
        res_json = defaultdict(list)
        res_csv = dict()
        res = self._output_graph.triples((None, RDF.type, None))

        # Set the merged graph as the main graph
        # so that Entity objects can be created from it.
        Graph._GRAPH = None
        Graph._initialized = False
        _ = Graph(self._output_ontology)
        for uri, _, _ in res:
            res_labels = self._output_graph.triples((uri, SKOS.prefLabel, None))
            pref_label = None
            for _, _, pref_label in res_labels:
                break
            if not pref_label:
                continue
            term = standardize_uri(str(pref_label))
            if term in res_json:
                print("Duplicate term:", term)
                # TODO use the type in the term / other information
            else:
                res_json[term].append(str(pref_label))

            entity = Entity(uri)

            # Alt labels (JSON)
            alt_labels = entity.get_values_for("alt_label",
                                               unique = False,
                                               languages = None, # List of languages for alt labels to keep in both formats
                                               )
            for alt_label in alt_labels:
                if str(alt_label) in res_json[term]:
                    continue # Make it behave like a set
                res_json[term].append(str(alt_label))

            # CSV
            description = entity.get_values_for("description", unique = True)
            if not description:
                description = entity.get_values_for("definition", unique = True)
            if not description: # may have returned None
                description = ""
            description = description.replace("\n", "")[:500]

            more_relations = ""
            fields = [#"alt_label",
                      "uri"]
            for field in fields:
                more_relations += self._to_string(entity, relation = field)
            
            res_csv[term] = {"level": None,
                             "label": pref_label,
                             "description": description,
                             "more_relations": more_relations,
                             "entity": entity}

        self._res_json = res_json
        self._res_csv = res_csv

        self._sort_csv()
        """
        children_by_broader = self._childen_by_broader()
        res_csv = []
        already_in = set()
        for broader in children_by_broader.copy().keys():
            if broader in already_in:
                continue
            self._get_recursive(children_by_broader, broader, res_csv, already_in)
        for term, entity in self._res_csv.items():
            entity["term"] = term
            entity["level"] = 1
            res_csv.append(entity)
        self._res_csv = res_csv
        """


    def _childen_by_broader(self) -> dict:
        children_by_broader = defaultdict(list)

        # First, we feed the children & broader dicts
        for term, values in self._res_csv.items():
            more_relations = values["more_relations"].split(' ')
            for relation in more_relations:
                if relation.startswith('skos:broader'):
                    broader = re.findall(r'\((.+)\)', relation)[0]
                    children_by_broader[broader].append(term)
                    # Remove broader from more_relations
                    values["more_relations"] = values["more_relations"].replace(relation, "") # Remove this broader relation
                    break
        return children_by_broader


    def write_json(self):
        """
        Write the JSON file from the ontology's merged synonym sets.
        """
        with open(self._output_json, 'w') as file:
            json.dump(self._res_json, file,
                      indent = 4,
                      ensure_ascii = False,
                      sort_keys = True)
            print(f"JSON output saved in {self._output_json}.")


    def write_csv(self):
        """
        Write the CSV file from the ontology's merged synonym sets.
        It follows the IVOA vocabularies standards (grouped by isPartOf).
        """
        with open(self._output_csv, "w") as file:
            res_csv_str = ""
            for values in self._res_csv:
                term = values["term"]
                level = values["level"]
                label = values["label"]
                description = values["description"]
                more_relations = values["more_relations"]
                res_csv_str += f'"{term}";{level};"{label}";"{description}";{more_relations}'
                res_csv_str += "\n"
            file.write(re.sub(r" +", " ", res_csv_str))
            print(f"CSV output saved in {self._output_csv}.")


def main(input_ontology,
         output_ontology,
         output_csv,
         output_json):
    merger = MergeURIs(input_ontology,
                            output_ontology,
                            output_csv,
                            output_json)
    merger.to_synonym_list()
    merger.write_ttl()
    merger.generate_json_csv()
    merger.write_json()
    merger.write_csv()


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
                        "--output-csv",
                        dest = "output_csv",
                        required = False,
                        default = "output_ivoa.tsv",
                        help = "Output CSV filename (IVOA format)")
    parser.add_argument("-j",
                        "--output-json",
                        dest = "output_json",
                        required = False,
                        default = "output_name_resolver.json",
                        help = "Output JSON filename (Name Resolver format)")

    args = parser.parse_args()
    main(args.input_ontology,
         args.output_ontology,
         args.output_csv,
         args.output_json)
