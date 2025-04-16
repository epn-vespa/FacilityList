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
from argparse import ArgumentParser
import atexit
from pathlib import Path
from typing import List
import uuid

from graph import Graph
from data_merger.candidate_pair import CandidatePairsManager, CandidatePairsMapping
from data_merger.identifier_merger import IdentifierMerger
from data_merger.scorer.acronym_scorer import AcronymScorer
from data_merger.scorer.cosine_similarity_scorer import CosineSimilarityScorer
from data_merger.scorer.score import Score
from data_merger.scorer.scorer_lists import ScorerLists
from data_merger.synonym_set import SynonymSetManager
from data_merger.scorer.fuzzy_scorer import FuzzyScorer
from data_updater.extractor.aas_extractor import AasExtractor
from data_updater.extractor.extractor import Extractor
from data_updater.extractor.extractor_lists import ExtractorLists
from data_updater.extractor.iaumpc_extractor import IauMpcExtractor
from data_updater.extractor.naif_extractor import NaifExtractor
from data_updater.extractor.pds_extractor import PdsExtractor
from data_updater.extractor.spase_extractor import SpaseExtractor
from data_updater.extractor.wikidata_extractor import WikidataExtractor
from utils.performances import printtimes, timeit

class Merger():


    def __init__(self,
                 input_ontology: str = "",
                 output_ontology: str = "",
                 scores: List[Score] = ScorerLists.ALL_SCORES):
        # Instanciate the Synonym Set Manager
        SynonymSetManager()
        # Instanciate the Graph's singleton
        self._graph = Graph(input_ontology)
        if input_ontology:
            self.init_graph() # Create basic classes
        self._scores = scores
        self._output_ontology = output_ontology
        self._execution_id = str(uuid.uuid4())


    @property
    def graph(self):
        return self._graph


    @property
    def scores(self):
        return self._scores


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

        if not (self._graph.is_available("wikidata")
            and self._graph.is_available("naif")):
            return
        im = IdentifierMerger(self._graph)
        CPM_wiki_naif = CandidatePairsManager(WikidataExtractor.NAMESPACE,
                                              NaifExtractor.NAMESPACE)

        # merge wiki naif if the namespaces are available.
        if im.merge_wikidata_naif(CPM_wiki_naif):
            # Disambiguate cases with two candidates
            # (necessary because NAIF has duplicate identifiers
            # for different entities)
            CPM_wiki_naif.disambiguate_candidates(scores = [FuzzyScorer])
            # Save the remaining candidate pairs.
            CPM_wiki_naif.save_json(execution_id = self.execution_id)

        del(CPM_wiki_naif)
        del(im)

        #TODO Merge identifiers between wikidata & IAUMPC


    def make_mapping_between_lists(self,
                                   list1: Extractor,
                                   list2: Extractor,
                                   scores: List[Score]):
        """
        Computes a mapping between two lists and disambiguate.
        """
        try:
            CPM = CandidatePairsMapping(list1,
                                        list2)
            CPM.generate_mapping(self.graph)
            CPM.disambiguate(scores = scores)
            # CPM.save_to_graph()
            CPM.save_json(self.execution_id)
        except InterruptedError:
            CPM.save_json(self.execution_id)
            SynonymSetManager._SSM.save_all()
            self.write()
            exit()
        del(CPM)


    def merge_mapping(self):
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
        conf_file = Path(__file__).parent.parent / 'conf' / 'merging_strategy.conf'
        conf_file = str(conf_file)
        with open(conf_file, 'r') as file:
            for i, line in enumerate(file.readlines()):
                line = line.split('#')[0].strip()
                if not line:
                    continue # skip line if empty or comment line
                if line.count(':') != 1:
                    raise ValueError(f"Error line {i}: There must be exactly one ':' per line.")
                extractors, scores = line.split(':')
                extractors = extractors.split(',')
                extractors = [e.strip() for e in extractors if e.strip()]
                if len(extractors) != 2:
                    raise ValueError(f"Error on line {i} of {file}: " +
                                     f"There must be two list names and at least one score name.")
                extractor1_str = extractors[0]
                extractor2_str = extractors[1]
                if extractor1_str not in ExtractorLists.EXTRACTORS_BY_NAMES.keys():
                    raise ValueError(f"Error at line {line} in {file}: {extractor1_str} is not a valid list name.\n" +
                                     f"Available list names: {' '.join(ExtractorLists.EXTRACTORS_BY_NAMES.keys())}")
                extractor1 = ExtractorLists.EXTRACTORS_BY_NAMES[extractor1_str]
                if extractor2_str not in ExtractorLists.EXTRACTORS_BY_NAMES.keys():
                    raise ValueError(f"Error at line {line} in {file}: {extractor2_str} is not a valid list name.\n" +
                                     f"Available list names: {' '.join(ExtractorLists.EXTRACTORS_BY_NAMES.keys())}")
                extractor2 = ExtractorLists.EXTRACTORS_BY_NAMES[extractor2_str]

                scores = scores.split(',')
                scores = [s.strip() for s in scores if s.strip()]
                scores_to_compute = set()
                except_scores = set()
                for score in scores:
                    if score[0] == '-':
                        # Do not compute those scores
                        score = score[1:].strip()
                        if score not in ScorerLists.SCORES_BY_NAMES.keys():
                            raise ValueError(f"Error at line {line} in {file}: {score} is not a valid score name.\n" +
                                             f"Available score names: {' '.join(ScorerLists.SCORES_BY_NAMES.keys())}")
                        else:
                            except_scores.add(ScorerLists.SCORES_BY_NAMES[score])
                    elif score == 'all':
                        scores_to_compute.update(ScorerLists.ALL_SCORES)
                    elif score not in ScorerLists.SCORES_BY_NAMES.keys():
                            raise ValueError(f"Error at line {line} in {file}: {score} is not a valid score name.\n" +
                                             f"Available score names: {' '.join(ScorerLists.SCORES_BY_NAMES.keys())}")
                    else:
                        scores_to_compute.add(ScorerLists.SCORES_BY_NAMES[score])
                if (self.graph.is_available(extractor1_str) and
                    self.graph.is_available(extractor2_str)):
                    self.make_mapping_between_lists(extractor1(), extractor2(), scores_to_compute - except_scores)
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
        with open(self.output_ontology, 'w') as file:
            file.write(self.graph.serialize())


    def print_execution_id(self):
        """
        Called at exit to know where the execution files are saved.
        """
        print("Execution id:", self.execution_id)


@timeit
def main(input_ontology: str = "",
         output_ontology: str = "",
         scores: List[Score] = None):

    merger = Merger(input_ontology,
                    output_ontology,
                    scores = scores)
    atexit.register(merger.print_execution_id)
    # atexit.register(merger.write)

    merger.merge_identifiers()
    merger.merge_mapping()
    merger.write()


if __name__ == "__main__":

    parser = ArgumentParser(
        prog = "updater.py",
        description = "Compute scores between entities from different lists " +
            "and create CandidatePair with similarity scores between entities.")

    parser.add_argument("-i",
                    "--input-ontology",
                    dest = "input_ontology",
                    default = "",
                    type = str,
                    required = True,
                    help = "Input ontology on which we will perform " +
                    "entity mapping between different sources.")

    parser.add_argument("-o",
                    "--output-ontology",
                    dest = "output_ontology",
                    default = "output_merged.ttl",
                    type = str,
                    required = False,
                    help = "Output ontology file to save the merged data.")

    """
    parser.add_argument("-s",
                        "--scores",
                        default = ["all"],
                        type = str,
                        nargs = "+",
                        required = False,
                        choices = ["all"] + list(ScorerLists.SCORES_BY_NAMES.keys()),
                        help = "Scores to compute")
    """
    args = parser.parse_args()
    """
    if args.scores != ["all"]:
        scores = []
        for score in args.scores:
            scores.append(ScorerLists.SCORES_BY_NAMES[score])
    else:
        scores = ScorerLists.ALL_SCORES
    """

    main(args.input_ontology,
         args.output_ontology)
