"""
This transforms an Ontology into an IVOA vocabulary csv file
and a JSON file of synonyms (Name Resolver).

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import argparse
import re
import json
from collections import defaultdict
from graph.graph import Graph
from graph.entity import Entity
from graph.properties import Properties
from rdflib import RDFS, RDF, SKOS, URIRef
from utils.string_utilities import standardize_uri

properties = Properties()

class CsvJson():

    # Mapping between IVOA relations and Obs Facilities relations
    _IVOA_RELATIONS = {
            "is_part_of": "skos:broader",
            # "url": "owl:equivalentClass",
            "uri": "skos:sameAs",
            # "alt_label": "skos:altLabel",
            }


    def __init__(self,
                 input_file,
                 csv_file,
                 json_file):
        self._input_file = input_file
        self._csv_file = csv_file
        self._json_file = json_file


    def _sort_csv(self):
        """
        Sort CSV by hierarchy of broaders and use different levels.
        The res_csv is transformed into a list of dictionaries.
        Also, the broader entity that is not in the list of terms,
        must be linked to the Facility URI (for instruments) with:
            skos:broader(obsf#term)
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
                parts = entity.get_values_for(relation,
                                              return_language = False)
                for part in parts:
                    # Only keep parts that will be in the CSV
                    part = str(part).split("#")[-1]
                    if part:
                        if part not in self._res_csv:
                            part = "obsf#" + part.split("#")[-1] # External link to the OBSF IVOA vocabulary
                        values["more_relations"] += f"{self._IVOA_RELATIONS[relation]}({part}) "

        children_by_broader = defaultdict(list)

        # First, we feed the children & broader dicts
        for term, values in self._res_csv.items():
            more_relations = values["more_relations"].split(' ')
            for relation in more_relations:
                if relation.startswith('skos:broader'):
                    broader = re.findall(r'\((.+)\)', relation)[0]
                    # broader = broader.split('#')[-1]
                    if broader not in self._res_csv:
                        print("BUG: BROADER NOT IN RES_CSV:", broader)
                    else:
                        children_by_broader[broader].append(term)
                        # Remove broader from more_relations
                        values["more_relations"] = values["more_relations"].replace(relation, "") # Remove this broader relation
                        break
        res_csv = []
        already_in = set()

        # keys that are in no values
        roots = self._find_roots(children_by_broader)
        for broader in roots:# children_by_broader.copy().keys():
            broader = broader.split("#")[-1]
            if broader in already_in:
                continue
            self._get_recursive(children_by_broader, broader, res_csv, already_in)
        for term, entity in self._res_csv.items():
            # Remaining entities that do not have a broad entity
            entity["term"] = term
            entity["level"] = 1
            res_csv.append(entity)
        self._res_csv = res_csv


    def _find_roots(self, children_by_broader):
        roots = []
        for broader in children_by_broader.keys():
            is_root = True
            for children in children_by_broader.values():
                if broader in children:
                    is_root = False
                    break
            if is_root:
                roots.append(broader)
        return roots


    def _get_recursive(self, children_by_broader, broader, res_csv: list, already_in: set, level: int = 1):
        """
        Finds the level of the entities recursively
        """
        # broader = self._term_by_synonym_uri[broader]
        entity = self._res_csv.pop(broader)
        entity["level"] = level
        entity["term"] = broader
        res_csv.append(entity)
        already_in.add(broader)
        for child in children_by_broader.pop(broader, []):
            child = child.split("#")[-1]
            if child in already_in:
                continue
            self._get_recursive(children_by_broader, child, res_csv, already_in, level + 1)


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
        value_set = entity.get_values_for(relation,
                                          languages = "en",
                                          return_language = False)
        if properties._MAPPING[relation].get("objtype") == URIRef:
            return ""
        relation = self._IVOA_RELATIONS[relation]
        for value in value_set:
            if value is not None:
                if type(value) == str:
                    res += f'{relation}("{value}") '
                else:
                    res += f'{relation}({value}) '
        return res


    def generate_json_csv(self,
                          allowed_ext_ref: list = []):
        """

        Args:
            allowed_ext_ref: if more than one output file is generated,
            they can reference each other. In this case, we may want to
            inform each other about the terms.
        """
        self._input_graph = Graph(self._input_file, replace = True)
        # self._input_graph.parse(self._input_file)
        res_json = defaultdict(list)
        res_csv = dict()
        res = self._input_graph.triples((None, RDF.type, None))

        for uri, _, _ in res:
            res_labels = self._input_graph.triples((uri, SKOS.prefLabel, None))
            pref_label = None
            for _, _, pref_label in res_labels:
                break
            if not pref_label:
                continue
            term = standardize_uri(str(pref_label))

            if term in res_json:
                pass
                #print("Duplicate term:", term) # TODO also modify the term (same as in the other function)
                # TODO use the type in the term / other information
                # Problem: most entities (URI) has several types!
            else:
                res_json[term].append(str(pref_label))

            entity = Entity(uri)

            # Alt labels (JSON)
            alt_labels = entity.get_values_for("alt_label",
                                               unique = False,
                                               languages = None, # List of languages for alt labels to keep in both formats
                                               return_language = False
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
                             "label": str(pref_label),
                             "description": description,
                             "more_relations": more_relations,
                             "entity": entity}

        self._res_json = res_json
        self._res_csv = res_csv
        self._sort_csv()
        allowed_ext_ref.extend(res_json.keys()) # TODO apres manger l'utiliser dans l'input de l'autre appel
        return allowed_ext_ref


    def write_json(self):
        """
        Write the JSON file from the ontology's merged synonym sets.
        """
        with open(self._json_file, 'w') as file:
            json.dump(self._res_json, file,
                      indent = 4,
                      ensure_ascii = False,
                      sort_keys = True)
            print(f"JSON output saved in {self._json_file}.")


    def write_csv(self):
        """
        Write the CSV file from the ontology's merged synonym sets.
        It follows the IVOA vocabularies standards (grouped by isPartOf).
        """
        with open(self._csv_file, "w") as file:
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
            print(f"CSV output saved in {self._csv_file}.")


def main(input_ontology: str,
         output_csv: str = None,
         output_json: str = None):
    if not output_csv:
        output_csv = input_ontology.removesuffix(".ttl") + "_ivoa.csv"
    if not output_json:
        output_json = input_ontology.removesuffix(".ttl") + "_name_resolver.json"
    CJ = CsvJson(input_ontology,
                 output_csv,
                 output_json)
    CJ.generate_json_csv([])
    CJ.write_json()
    CJ.write_csv()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog = "generate_csv_json.py",
        description = "Generate OntoPortal ontologies (obsf, instruments) from a linked ontology.")
    parser.add_argument("-i",
                        "--intput-ontology",
                        dest = "input_ontology",
                        type = str,
                        required = True,
                        help = "Input ontology (that has been mapped and contains exactMatch relations).")
    parser.add_argument("-c",
                        "--output-csv",
                        dest = "output_csv",
                        required = False,
                        default = None,
                        help = "Output CSV filename (IVOA format)")
    parser.add_argument("-j",
                        "--output-json",
                        dest = "output_json",
                        required = False,
                        default = None,
                        help = "Output JSON filename (Name Resolver format)")

    args = parser.parse_args()
    main(args.input_ontology,
         args.output_csv,
         args.output_json)
