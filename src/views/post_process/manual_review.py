#!/usr/bin/env python
"""
Manual review of automatically validated mappings.
Take an output folder as input. Go through every
LLM-validated mappings and ask the user to validate or
unvalidate a mapping. Propose to change the relation type
too (isPartOf, hasPart).

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import setup_path # import first
from __version__ import __version__
import argparse
import pathlib
import shutil
import re
import atexit
import uuid

import config
from config import USERNAME
from collections import defaultdict
from rdflib import URIRef, Literal, XSD, DCTERMS, OWL, RDF, SKOS
from graph.graph import Graph
from graph.mapping_graph import MappingGraph
from graph.extractor import extractor_lists
from datetime import datetime

class ManualReviewer():

    def __init__(self,
                 folder: str,
                 validators: list[str],
                 lists: list[str],
                 begin: str,
                 end: str,
                 terminal: bool = False):
        version = 2
        new_version = re.findall(r"v([0-9]+)", folder, flags = re.DOTALL)
        if new_version:
            version = int(new_version[0])
            version += 1
        path = pathlib.Path(folder)
        if not path.is_dir():
            raise FileExistsError(f"Folder {folder} does not exist.")
        self._input_dir = path
        self._output_dir = path.parent / f"{str(datetime.now().strftime("%Y%m%d-%H%M%S"))}-v{version}"
        linked = path / "linked.ttl"
        mapping = path / "mapping.ttl"
        self._linked_input_file = linked
        self._mapping_input_file = mapping
        username = input("Enter the name of the reviewer: ")
        self._reviewer = Literal(username)
        self._linked = Graph(linked)
        self._mapping = MappingGraph(mapping,
                                     reviewer = username)
        self._validators = validators
        self._lists = lists
        if begin:
            begin = datetime.strptime(begin, format = "%Y-%m-%d %H:%M:%S")
        self._begin = begin
        if end:
            end = datetime.strptime(end, format = "%Y-%m-%d %H:%M:%S")
        self._end = end
        self._terminal = terminal
        self._initialize_mapping_set(self._reviewer)


    def _curate_mapping(self,
                        old_mapping_uri: URIRef,
                        new_relation: URIRef,
                        justification: str):
        """
        """
        #query = f"""SELECT ?relation ?justification WHERE
        #{{ <{old_mapping_uri}> sssom:predicate_id ?relation . }}"""
        #for old_relation, in self._mapping.query(query):
        #    if old_relation == new_relation:
        #       ... # Only verify about the synsets' integrity.
        new_mapping_uri = MappingGraph._OBS[str(uuid.uuid4())]
        # first of all copy the subject, object, relation to the new mapping
        query = f"""
        SELECT ?property ?obj WHERE {{
            <{old_mapping_uri}> ?property ?obj .
            FILTER (?property != sssom:mapping_set_id)
        }}
        """
        # Copy all previous mapping's information into the new mapping
        mapping_dict = defaultdict(list)
        for property, obj in self._mapping.query(query):
            self._mapping.add((new_mapping_uri, property, obj))
            mapping_dict[property].append(obj)

        self._mapping.add((new_mapping_uri, MappingGraph._SSSOM.reviewer_label, Literal(self._reviewer)))
        self._mapping.add((new_mapping_uri, MappingGraph._SSSOM.mapping_date, Literal(datetime.now(), datatype=XSD.dateTimeStamp)))
        self._mapping.add((old_mapping_uri, OWL.deprecated, Literal(True, datatype = XSD.boolean)))
        self._mapping.add((old_mapping_uri, DCTERMS.isReplacedBy, new_mapping_uri))
        self._mapping.add((new_mapping_uri, DCTERMS.replaces, old_mapping_uri))
        self._mapping.add((new_mapping_uri, MappingGraph._SSSOM.mapping_set_id, self._mapping_set))

        if new_relation == OWL.differentFrom:
            self.remove_relation(mapping_dict = mapping_dict)

    def remove_relation(self,
                        mapping_dict: dict):
        """
        Function that removes the exactMatch between the entities
        of the synonym set, keeping the previous validated mappings.
        Example:
            Mapping 1: e1 ≅ e2
            Mapping 2: e1 ≅ e3
            Mapping 3: e3 ≅ e4
            Delete mapping 1: e1 ≅ e2, e2 ≅ e3, e2 ≅ e4 will be removed.
            Delete mapping 2: e1 ≅ e3, e1 ≅ e4, e2 ≅ e3 will be removed.
            Delete mapping 3: e1 ≅ e4, e2 ≅ e4, e2 ≅ e4 will be removed. 
        """
        # Change the relation between the two and the related entities too
        # according to the history of mappings
        # 1. Get entities in synset
        subj_entity = mapping_dict[MappingGraph._SSSOM.subject_id][0]
        obj_entity = mapping_dict[MappingGraph._SSSOM.object_id][0]
        synonym_set = {subj_entity}
        for syn, in self._linked.query(f"""SELECT ?synonym WHERE {{
                                        <{subj_entity}> skos:exactMatch ?synonym .
                                        }}"""):
            synonym_set.add(syn)
        print("synonym_set=")
        print(synonym_set)
        print("\n")
        # 2. Get history of mappings
        mappings_history = []
        all_entities_in_synset = {subj_entity, obj_entity}
        for syn in synonym_set:
            query = f"""SELECT ?obj WHERE {{
                        ?mapping sssom:subject_id <{syn}> .
                        ?mapping sssom:object_id ?obj .
                        ?mapping sssom:predicate_id skos:exactMatch .
                        }}
                        """
            print(query)
            for obj, in self._mapping.query(query):
                subj = syn
                print(subj, obj)
                all_entities_in_synset.add(subj)
                all_entities_in_synset.add(obj)
                # Remove the exactMatch relation in place
                self._linked.remove((subj, SKOS.exactMatch, obj))
                if {subj, obj} == {subj_entity, obj_entity}:
                    pass
                else:
                    mappings_history.append((subj, obj))
            print(mappings_history)
        print("\n")
        print(mappings_history)
        # 3. Re-compute synonym sets' relations (the synonym set should be split into two sets)
        synonym_sets = []
        for subj, obj in mappings_history:
            added = False
            for synset in synonym_sets:
                if subj in synset or obj in synset:
                    synset.add(subj)
                    synset.add(obj)
                    added = True
            if not added:
                synonym_sets.append({subj, obj})
        print("Synonym sets=", synonym_sets)
        print(len(synonym_sets))
        assert len(synonym_sets) == 2 # Should have been split in two
        # 3. Remove previous relations of synsets
        for ent in all_entities_in_synset:  
            self._linked.remove((ent, SKOS.exactMatch, None))
        # 4. Re-create the relations
        for ent1 in all_entities_in_synset:
            synset = [s for s in synonym_sets if ent1 in s]
            for ent2 in all_entities_in_synset:
                if ent1 >= ent2:
                    continue
                if ent2 in synset:
                    # add relation
                    self._linked.add((ent1, SKOS.exactMatch, ent2))
                    self._linked.add((ent2, SKOS.exactMatch, ent1))


    def _get_filters(self):
        """
        Get filters for SPARQL query
        """
        # Filter on date
        filter_date = ""
        filter_begin = ""
        filter_end = ""
        and_oper = ""
        if self._begin:
            filter_begin = f"?date >= \"{self._begin}\"^^xsd:datetime"
        if self._end:
            filter_end = f"?date <= \"{self._end}\"^^xsd:datetime"
        if filter_begin and filter_end:
            and_oper = "&&"
        if filter_begin or filter_end:
            filter_date = f""" ?mapping sssom:mapping_date ?date .
            FILTER (
            {filter_begin} {and_oper}
            {filter_end}
            )
            """

        # Filter lists
        filter_lists = ""
        if self._lists:
            allowed_sources = '\n'.join(["obsf:" + s + "_list" for s in self._lists])
            filter_lists = f"""
            ?mapping sssom:subject_source ?subject_source ;
            sssom:object_source ?object_source .
            ?allowed_sources {{
                {allowed_sources}
            }}
            FILTER (
                ?subject_source IN (?allowed_sources) ||
                ?object_source IN (?allowed_sources)
            )
            """

        # filter validators
        filter_validators = ""
        if self._validators:
            allowed_validators = '\n'.join([f"\"{r}\"^^xsd:string" for r in self._validators])
            filter_validators = f"""
            ?mapping sssom:reviewer_label ?reviewer .

            ?allowed_validators {{
                {allowed_validators}
            }}
            FILTER (
                ?reviewer IN (?allowed_validators)
            )
            """
        return filter_date, filter_lists, filter_validators


    def _get_availaible_reviewers(self) -> list[str]:
        """
        Get the reviewers' labels to recommand. Run this function if
        the user provided reviewers not in the mapping ontology.
        """

        query = f"""
        PREFIX sssom: <https://w3id.org/sssom/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

        SELECT DISTINCT ?reviewer WHERE {{
            ?mapping sssom:reviewer_label ?reviewer .
        }}
        """
        res = self._mapping.query(query)
        for reviewer, in res:
            print("Available reviewers:", reviewer)
        return list(res)


    def _get_mappings(self):
        """
        Get all mappings to review.
        """
        filter_date, filter_lists, filter_reviewers = self._get_filters()
        # Mappings that were reviewed by LLM
        query = f"""
        SELECT ?mapping WHERE {{
            ?mapping a sssom:Mapping .
            ?mapping sssom:reviewer_label ?reviewer_lbl .
            {filter_date}
            {filter_lists}
            {filter_reviewers}
            FILTER NOT EXISTS {{
                ?mapping owl:deprecated true .
            }}
        }}
        """
        print(query)
        return self._mapping.query(query)


    def _initialize_mapping_set(self,
                                author: str):
        """
        Create the mapping set for this review.
        """
        self._mapping_set = MappingGraph._OBS[str(uuid.uuid4())]
        self._mapping.add((self._mapping_set, RDF.type, MappingGraph._SSSOM.MappingSet))
        self._mapping.add((self._mapping_set, MappingGraph._SSSOM.creator_label, Literal(author, datatype=XSD.string)))
        self._mapping.add((self._mapping_set, MappingGraph._SSSOM.mapping_set_description, Literal(f"Review of mappings in {self._input_dir} by {self._reviewer}", datatype=XSD.string)))
        self._mapping.add((self._mapping_set, MappingGraph._SSSOM.mapping_date, Literal(datetime.now(), datatype=XSD.dateTimeStamp)))
        self._mapping.add((self._mapping_set, MappingGraph._SSSOM.mapping_tool, Literal("https://doi.org/10.5281/zenodo.17199128", datatype=XSD.anyURI)))
        self._mapping.add((self._mapping_set, MappingGraph._SSSOM.mapping_tool_version, Literal(__version__, datatype=XSD.string)))


    def review(self):
        """
        Review mappings (main loop)
        """
        atexit.register(self.write)
        # Open web service (async)
        for mapping_uri, in self._get_mappings():
            # Get infos and transmit to the web interface
            print(mapping_uri)
            if self._terminal:
                changed, new_relation, justification = self._terminal_validation(mapping_uri) # TODO
            else:
                raise NotImplementedError("Not any support for GUI yet. Please use terminal instead.")
            if changed:
                self._curate_mapping(mapping_uri,
                                     new_relation,
                                     justification)
            else:
                self._validate_mapping(mapping_uri) # TODO add a reviewer label


    def _terminal_validation(self,
                             mapping_uri: URIRef) -> tuple[bool, URIRef, str]:
        """
        Print the informations of a mapping and ask the user to input
        a justification, a new relation or validate the mapping.

        Returns:
            A tuple with 3 variables:
            [bool] weither the relation was changed
            [URIRef] the new relation to use
            [str] the new justification string to add in the new mapping

        Arguments:
            mapping_uri: the current mapping's URI that is being reviewed
        """
        res = self._mapping.query(f"""SELECT ?subj ?rel ?obj ?justification WHERE {{
                                  <{mapping_uri}> sssom:predicate_id ?rel ;
                                                  sssom:subject_id ?subj ;
                                                  sssom:object_id ?obj ;
                                                  rdfs:comment ?justification .
                                  }}""")
        old_rel = None
        print(res)
        for subj, rel, obj, justification in res:
            old_rel = rel
            print(subj, rel, obj)
            print(justification)
            changed = input("Modify relation ?\n0 -> No\n1 -> change to owl:differentFrom\n2 -> change to skos:broadMatch (obj part of subj)\n3 -> skos:narrowMatch (subj part of obj)\n >>> ")
            changed = int(changed)
            match changed:
                case 0:
                    new_relation = rel
                case 1:
                    new_relation = OWL.differentFrom
                case 2:
                    new_relation = SKOS.broadMatch
                case 3:
                    new_relation = SKOS.narrowMatch
            new_justification = input("Justification (leave empty to keep previous justification, spaces to get no justification) >>>")
            if new_justification == "":
                new_justification = justification
            elif new_justification.strip() == "":
                new_justification = None
            else:
                pass
            return changed, new_relation, new_justification
        if not old_rel:
            raise ValueError(f"The mapping with id {mapping_uri} does not have a predicate_id property.")


    def write(self):
        output_dir = self._output_dir
        output_dir.mkdir(parents = True, exist_ok = True)
        output_ontology = output_dir / 'linked.ttl'
        self._linked.serialize(destination = output_ontology,
                               format = "turtle",
                               encoding = "utf-8")
        
        self._mapping.serialize(output_dir = self._output_dir)
                                # execution_id = None)
        progress_file = self._input_dir / "progress.pkl"
        progress_file
        shutil.copy2(progress_file, output_dir / "progress.pkl")
        atexit.unregister(self.write)


def main(folder: str,
         validators: list[str],
         lists: list[str],
         begin: str,
         end: str,
         terminal: bool):
    mr = ManualReviewer(folder, validators, lists, begin, end, terminal)
    mr.review()
    #filter_date, filter_lists, filter_reviewers = mr._get_filters()
    #print(filter_date)
    #print(filter_lists)
    #print(filter_reviewers)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog = "manual_review.py",
                                     description = "Manually review mappings.")
    parser.add_argument("-f",
                        "--folder",
                        required = True,
                        type = str)

    parser.add_argument("-r",
                        "--reviewer",
                        required = False,
                        nargs = "*",
                        type = str,
                        help = "For which reviewer(s) shall the verification be done.")

    parser.add_argument("-l",
                        "--lists",
                        required = False,
                        nargs = "*",
                        type = str,
                        choices = [e.NAMESPACE for e in extractor_lists.ExtractorLists.AVAILABLE_EXTRACTORS],
                        help = "Which list(s) should be included in the mappings to review.")

    parser.add_argument("-b",
                        "--begin",
                        required = False,
                        type = str,
                        help = "Begin date (only review mappings after this datetime). Format: yyyy-mm-dd hh-mm-ss")

    parser.add_argument("-e",
                        "--end",
                        required = False,
                        type = str,
                        help = "End date (only review mappings created before this datetime). Format: yyyy-mm-dd hh-mm-ss")

    parser.add_argument("-t",
                        "--terminal",
                        action = "store_true",
                        default = False,
                        help = "If True, will perform the review in the Terminal instead of opening a GUI.")



    args = parser.parse_args()
    main(args.folder, args.reviewer, args.lists, args.begin, args.end, args.terminal)