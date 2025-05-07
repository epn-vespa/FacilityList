#!/bin/python3

"""
Perform merging between all lists.
Compute scores between entities from the lists in the input ontology
and create CandidatePair entities to save the similarity scores.
Finally, group entities in a Synonym Set if their similarity score
is high enough.

Arguments:

Author:
    Liza Fretel (liza.fretel@obsmp.fr)
"""
import setup_path # import first
from data_updater.extractor.nssdc_extractor import NssdcExtractor

from data_merger.scorer.acronym_scorer import AcronymScorer
from argparse import ArgumentParser
import atexit
from typing import List
import uuid

from graph import Graph
from data_merger.candidate_pair import CandidatePairsManager, CandidatePairsMapping
from data_merger.identifier_merger import IdentifierMerger
from data_merger.scorer.score import Score
from data_merger.scorer.scorer_lists import ScorerLists
from data_merger.synonym_set import SynonymSetManager
from data_merger.scorer.fuzzy_scorer import FuzzyScorer
from data_updater.extractor.extractor import Extractor
from data_updater.extractor.extractor_lists import ExtractorLists
from data_updater.extractor.iaumpc_extractor import IauMpcExtractor
from data_updater.extractor.naif_extractor import NaifExtractor
from data_updater.extractor.wikidata_extractor import WikidataExtractor
from utils.performances import timeit

from config import CONF_DIR # type: ignore

class Merger():


    def __init__(self,
                 input_ontologies: list[str],
                 output_ontology: str = ""):
        # Instanciate the Synonym Set Manager
        SynonymSetManager()
        # Instanciate the Graph's singleton

        # merge ontologies' triples into one ontology
        self._graph = Graph(input_ontologies)
        if input_ontologies:
            self.init_graph() # Create basic classes
        self._output_ontology = output_ontology
        self._execution_id = str(uuid.uuid4())


    @property
    def graph(self):
        return self._graph


    @property
    def output_ontology(self):
        return self._output_ontology


    @property
    def execution_id(self):
        return self._execution_id


    def init_graph(self):
        """
        Add CandidatePair & SynSet classes in the OBS namespace.
        """
        # TODO use update() on graph.


    def merge_identifiers(self):
        im = IdentifierMerger()

        if (self.graph.is_available("naif") and
            self.graph.is_available("wikidata")):
            CPM_wiki_naif = CandidatePairsManager(WikidataExtractor(),
                                                  NaifExtractor())
            #CPM_wiki_naif = CandidatePairsMapping(WikidataExtractor,
            #                                      NaifExtractor)

            # merge wiki naif if the namespaces are available.
            if im.merge_wikidata_naif(CPM_wiki_naif):
                # Disambiguate cases with two candidates
                # (necessary because NAIF has duplicate identifiers
                # for different entities)
                CPM_wiki_naif.disambiguate_candidates(scores = [AcronymScorer,
                                                                FuzzyScorer])
                # Save the remaining candidate pairs.
                CPM_wiki_naif.save_json(execution_id = self.execution_id)

            del(CPM_wiki_naif)

        if (self.graph.is_available("iaumpc") and
            self.graph.is_available("wikidata")):
            # P717. Doublon: P5736

            CPM_wiki_iaumpc = CandidatePairsMapping(WikidataExtractor(),
                                                    IauMpcExtractor())
            im.merge_on(CPM_wiki_iaumpc,
                        attr1 = "MPC_Obs_ID",
                        attr2 = "code")
            del(CPM_wiki_iaumpc)
        if (self.graph.is_available("nssdc") and
            self.graph.is_available("wikidata")):
            # P247 (COSPAR ID) Doublon: P8913 (NSSDCA)
            # Some of them appear more than once in wikidata.

            CPM_wiki_nssdc = CandidatePairsMapping(WikidataExtractor(),
                                                   NssdcExtractor())
            im.merge_on(CPM_wiki_nssdc,
                        attr1 = "NSSDCA_ID",
                        attr2 = "code")
            im.merge_on(CPM_wiki_nssdc,
                        attr1 = "COSPAR_ID",
                        attr2 = "code")
            del(CPM_wiki_nssdc)

        del(im)

        #TODO Merge identifiers between wikidata & IAUMPC


    def make_mapping_between_lists(self,
                                   list1: Extractor,
                                   list2: Extractor,
                                   scores: List[Score]):
        """
        Computes a mapping between two lists and disambiguate.

        Keyword arguments:
        list1 -- the first list's Extractor
        list2 -- the second list's Extractor
        scores -- scores to compute for those lists
        """
        try:
            CPM = CandidatePairsMapping(list1,
                                        list2)
            CPM.generate_mapping()
            CPM.disambiguate(scores = scores)
            # CPM.save_to_graph()
            CPM.save_json(self.execution_id)
        except InterruptedError:
            CPM.save_json(self.execution_id)
            SynonymSetManager._SSM.save_all()
            exit()
        del(CPM)


    def merge_mapping(self,
                      conf_file: str = ""):
        """
        Define the merging strategy (merging order).

        Compute a mapping between classes from namespaces two by two.
        Generates candidate pairs before performing disambiguation.
        Repeat for every mapping until the mapping is done.
        Mapping will take already existing Synonym Sets into account,
        and link a term to a synonym set instead of creating a new one
        if there is a synonym set that already exists for one of the
        candidates.
        TODO FIXME if there are two candidates in a synset, then merge synsets
        """

        with open(conf_file, 'r') as file:
            for i, line in enumerate(file.readlines()):
                line = line.split('#')[0].strip()
                if not line:
                    continue # skip line if empty or comment line
                if line.count(':') != 1:
                    raise ValueError(f"Error line {i}: There must be exactly one ':' per line.")
                extractors, scores = line.split(':')
                if not scores.strip():
                    raise ValueError(f"Error on line {i} of {conf_file}: " +
                                     f"No score set.")
                extractors = extractors.split(',')
                extractors = [e.strip() for e in extractors if e.strip()]
                if len(extractors) != 2:
                    raise ValueError(f"Error on line {i} of {conf_file}: " +
                                     f"There must be two list names per line.")
                extractor1_str = extractors[0]
                extractor2_str = extractors[1]
                if extractor1_str not in ExtractorLists.EXTRACTORS_BY_NAMES.keys():
                    raise ValueError(f"Error at line {i} in {conf_file}: " +
                                     f"{extractor1_str} is not a valid list name.\n" +
                                     f"Available list names: {' '.join(ExtractorLists.EXTRACTORS_BY_NAMES.keys())}")
                extractor1 = ExtractorLists.EXTRACTORS_BY_NAMES[extractor1_str]
                if extractor2_str not in ExtractorLists.EXTRACTORS_BY_NAMES.keys():
                    raise ValueError(f"Error at line {i} in {conf_file}: " +
                                     f"{extractor2_str} is not a valid list name.\n" +
                                     f"Available list names: {' '.join(ExtractorLists.EXTRACTORS_BY_NAMES.keys())}")
                extractor2 = ExtractorLists.EXTRACTORS_BY_NAMES[extractor2_str]

                if not (extractor1.POSSIBLE_TYPES.union(extractor2.POSSIBLE_TYPES)):
                    print(f"Warning at line {i} in {conf_file}: " +
                          f"No type intersection between {extractor1.NAMESPACE} and {extractor2.NAMESPACE}. Ignoring.")

                scores = scores.split(',')
                scores = [s.strip() for s in scores if s.strip()]
                scores_to_compute = set()
                except_scores = set()
                for score in scores:
                    if score[0] == '-':
                        # Do not compute those scores
                        score = score[1:].strip()
                        if score not in ScorerLists.SCORES_BY_NAMES.keys():
                            raise ValueError(f"Error at line {i} in {conf_file}: " +
                                             f"{score} is not a valid score name.\n" +
                                             f"Available score names: {' '.join(ScorerLists.SCORES_BY_NAMES.keys())}")
                        else:
                            except_scores.add(ScorerLists.SCORES_BY_NAMES[score])
                    elif score == 'all':
                        scores_to_compute.update(ScorerLists.ALL_SCORES)
                    elif score not in ScorerLists.SCORES_BY_NAMES.keys():
                        raise ValueError(f"Error at line {i} in {conf_file}: " +
                                         f"{score} is not a valid score name.\n" +
                                         f"Available score names: {' '.join(ScorerLists.SCORES_BY_NAMES.keys())}")
                    else:
                        scores_to_compute.add(ScorerLists.SCORES_BY_NAMES[score])
                if (self.graph.is_available(extractor1_str) and
                    self.graph.is_available(extractor2_str)):

                    scores = scores_to_compute - except_scores
                    if scores:
                        self.make_mapping_between_lists(extractor1(), extractor2(), scores)
                    else:
                        print(f"Warning at line {i} in {conf_file}: " +
                              f"no scores to compute for {extractor1_str} and {extractor2_str}. Ignoring.")
                else:
                    print(f"Warning at line {i} in {conf_file}: " +
                          f"{extractor1_str} and/or {extractor2_str} are not available in the provided ontologies. Ignoring.")
        """
        self.make_mapping_between_lists(AasExtractor(),
                                        WikidataExtractor())
        self.make_mapping_between_lists(SpaseExtractor(),
                                        WikidataExtractor())
        self.make_mapping_between_lists(IauMpcExtractor(),
                                        WikidataExtractor())
        self.make_mapping_between_lists(PdsExtractor(),
                                        WikidataExtractor())
        """

        # /!\ Save the synonym sets in the graph (do not remove)
        SynonymSetManager._SSM.save_all()

    @timeit
    def write(self):
        print(f"Writing the result ontology into {self.output_ontology}...")
        with open(self.output_ontology, 'wb') as file:
            file.write(self.graph.serialize(encoding = "utf-8"))


    def print_execution_id(self):
        """
        Called at exit to know where the execution files are saved.
        """
        print("Execution id:", self.execution_id)


@timeit
def main(input_ontologies: list[str] = [],
         output_ontology: str = "",
         merging_strategy_file: str = ""):

    merger = Merger(input_ontologies,
                    output_ontology)
    atexit.register(merger.print_execution_id)
    # atexit.register(merger.write)

    merger.merge_identifiers()
    merger.merge_mapping(conf_file = merging_strategy_file)
    merger.write()


if __name__ == "__main__":

    parser = ArgumentParser(
        prog = "updater.py",
        description = "Compute scores between entities from different lists " +
            "and create CandidatePair with similarity scores between entities.")

    parser.add_argument("-i",
                        "--input-ontologies",
                        dest = "input_ontologies",
                        nargs = "+",
                        default = "",
                        type = str,
                        required = True,
                        help = "Input ontology or ontologies on which we will perform " +
                        "entity mapping between different sources.")

    parser.add_argument("-o",
                        "--output-ontology",
                        dest = "output_ontology",
                        default = "output_merged.ttl",
                        type = str,
                        required = False,
                        help = "Output ontology file to save the merged data.")

    parser.add_argument("-m",
                        "--merging-strategy",
                        dest = "merging_strategy_file",
                        default = CONF_DIR / "merging_strategy.conf",
                        type = str,
                        required = False,
                        help = "Merging strategy file name."
)

    args = parser.parse_args()


    main(args.input_ontologies,
         args.output_ontology,
         args.merging_strategy_file)
