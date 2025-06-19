#!/bin/python3

"""
Perform merging between all lists.
Compute scores between entities from the lists in the input ontology
and create CandidatePair entities to save the similarity scores.
Finally, group entities in a Synonym Set if their similarity score
is high enough.

Author:
    Liza Fretel (liza.fretel@obsmp.fr)
"""
import setup_path # import first

from data_merger.scorer.distance_scorer import DistanceScorer
from data_updater import entity_types
from data_merger.scorer.acronym_scorer import AcronymScorer
from argparse import ArgumentParser
import atexit
from typing import List
import uuid
import os
import sys

from graph import Graph
from data_merger.candidate_pair import CandidatePairsManager, CandidatePairsMapping
from data_merger.identifier_merger import IdentifierMerger
from data_merger.scorer.score import Score
from data_merger.scorer.scorer_lists import ScorerLists
from data_merger.scorer.type_incompatibility_scorer import TypeIncompatibilityScorer
from data_merger.synonym_set import SynonymSetManager
from data_merger.scorer.fuzzy_scorer import FuzzyScorer
from data_updater.extractor.extractor import Extractor
from data_updater.extractor.extractor_lists import ExtractorLists
from data_updater.extractor.iaumpc_extractor import IauMpcExtractor
from data_updater.extractor.imcce_extractor import ImcceExtractor
from data_updater.extractor.naif_extractor import NaifExtractor
from data_updater.extractor.wikidata_extractor import WikidataExtractor
from data_updater.extractor.nssdc_extractor import NssdcExtractor
from data_updater.extractor.pds_extractor import PdsExtractor
from utils.performances import timeit
from collections import defaultdict

from config import CONF_DIR # type: ignore


class Merger():


    def __init__(self,
                 input_ontologies: list[str],
                 output_ontology: str = "",
                 limit: int = -1):
        """
        limit -- maximum entities per list (for debug)
        """
        # Instanciate the Graph's singleton
        # merge ontologies' triples into one ontology
        self._graph = Graph(input_ontologies)

        # Instanciate the Synonym Set Manager
        SynonymSetManager()

        self._output_ontology = output_ontology
        self._execution_id = str(uuid.uuid4())
        self._limit = limit
        self._description = f"script: {os.path.basename(sys.argv[0])}\n" + \
            f"execution id: {self._execution_id}\n" + \
            f"source: {' '.join(input_ontologies)}\n" + \
            f"filename: {output_ontology}\n"


    @property
    def graph(self):
        return self._graph


    @property
    def output_ontology(self):
        return self._output_ontology


    @property
    def execution_id(self):
        return self._execution_id


    @property
    def limit(self):
        return self._limit


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
                # (necessary to have a separate function
                # because NAIF has duplicate identifiers
                # for different entities)
                CPM_wiki_naif.disambiguate_candidates(scores = [AcronymScorer,
                                                                FuzzyScorer])
                # Save the remaining candidate pairs
                # (should be 0 as wikidata/naif is well mapped)
                # CPM_wiki_naif.save_json(execution_id = self.execution_id)
            self._description += "merge identifiers: naif, wikidata\n"
            del(CPM_wiki_naif)
        if (self.graph.is_available("iaumpc") and
            self.graph.is_available("wikidata")):
            # P717. Doublon: P5736

            CPM_wiki_iaumpc = CandidatePairsMapping(WikidataExtractor(),
                                                    IauMpcExtractor())
            im.merge_on(CPM_wiki_iaumpc,
                        attr1 = "MPC_ID",
                        attr2 = "code",
                        map_remaining = False) # TODO: True
            # CPM_wiki_iaumpc.compute_scores()
            self._description += "merge identifiers: iaumpc, wikidata\n"
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
            self._description += "merge identifiers: nssdc, wikidata\n"
            del(CPM_wiki_nssdc)
        if (self.graph.is_available("imcce") and
            self.graph.is_available("wikidata")):
            CPM_wiki_imcce = CandidatePairsMapping(WikidataExtractor(),
                                                   ImcceExtractor())
            im.merge_on(CPM_wiki_imcce,
                        attr1 = "NSSDCA_ID",
                        attr2 = "alt_label")
            im.merge_on(CPM_wiki_imcce,
                        attr1 = "COSPAR_ID",
                        attr2 = "alt_label")
            del(CPM_wiki_imcce)
        if (self.graph.is_available("imcce") and
            self.graph.is_available("nssdc")):
            CPM_nssdc_imcce = CandidatePairsMapping(NssdcExtractor(),
                                                    ImcceExtractor())
            im.merge_on(CPM_nssdc_imcce,
                        attr1 = "code",
                        attr2 = "alt_label")
            del(CPM_nssdc_imcce)
        if (self.graph.is_available("pds") and
            self.graph.is_available("nssdc")):
            CPM_nssdc_pds = CandidatePairsMapping(NssdcExtractor(),
                                                  PdsExtractor())
            im.merge_on(CPM_nssdc_pds,
                        attr1 = "alt_label",
                        attr2 = "code")
            self._description += "merge identifiers: pds, nssdc\n"
            del(CPM_nssdc_pds)

        del(im)


    def make_mapping_between_lists(self,
                                   list1: Extractor,
                                   list2: Extractor,
                                   scores: set[Score],
                                   types: set[str] = None,
                                   checkpoint_id: str = None,
                                   human_validation: bool = False):
        """
        Compute a mapping between two lists and disambiguate.
        Compute the mapping for a set of scores and types if both
        sources' types are known. Else, split the mapping into
        2 kinds: space (spacecraft, mission) and ground (all the others).

        Troubleshooting:
            It would be better to ignore type and compute the full mapping,
            but it is currently too time consuming (theoretically
            4 times slower in the worst case)

        Keyword arguments:
        list1 -- the first list's Extractor
        list2 -- the second list's Extractor
        scores -- scores to compute for those lists
        types -- types the scores apply to
        """
        if not scores:
            print(f"No scores to compute for {list1}, {list2}. Ignoring.")
            return

        # If the program gets interrupted by an error, save the output
        # ontology anyways.
        atexit.register(SynonymSetManager._SSM.save_all)
        atexit.register(self.write)
        try:
            if list1.TYPE_KNOWN == 1 and list2.TYPE_KNOWN == 1:
                # If types are known in both lists, do one mapping per type
                if TypeIncompatibilityScorer in scores:
                    print(f"Warning: {list1.NAMESPACE} and {list2.NAMESPACE}'s types are known." + \
                           "Therefore, they are mapped on types, no need to use the type score. Ignoring.")
                for ent_type in types:
                    if ent_type not in list1.POSSIBLE_TYPES:
                        print(f"Warning: {ent_type} is not available for {list1}. Ignoring.")
                        continue
                    if ent_type not in list2.POSSIBLE_TYPES:
                        print(f"Warning: {ent_type} is not available for {list2}. Ignoring.")
                        continue
                    do_not_compute = {TypeIncompatibilityScorer}
                    if ent_type in entity_types.NO_ADDR:
                        do_not_compute.add(DistanceScorer)
                    CPM = CandidatePairsMapping(list1,
                                                list2,
                                                ent_type = ent_type,
                                                checkpoint_id = checkpoint_id)
                    scores_str = []
                    for score in scores - do_not_compute:
                        scores_str.append(str(score))
                    scores_str = ', '.join(scores_str)
                    self._description += f"mapping on: {list1.NAMESPACE}, {list2.NAMESPACE}," + \
                                         f"with scores: {scores_str}"
                    if not checkpoint_id:
                        CPM.generate_mapping(limit = self.limit)
                        CPM.compute_scores(scores = scores - do_not_compute)
                        if all([score in ScorerLists.DISCRIMINANT_SCORES for score in scores]):
                            # Only discriminant scores
                            print("Only discriminant scores. No disambiguation required. Returning.")
                            del(CPM)
                            return
                        CPM.disambiguate(SynonymSetManager._SSM,
                                         human_validation)
                        scores_str = []
                    else:
                        CPM.disambiguate(SynonymSetManager._SSM,
                                         human_validation)
                    del(CPM)
            else:
                # Types are partially known or unknown in at least one of both lists
                for ent_types in (entity_types.NO_ADDR, entity_types.MAY_HAVE_ADDR):
                    do_not_compute = set()
                    if ent_types == entity_types.NO_ADDR:
                        do_not_compute.add(DistanceScorer)
                    ent_types -= set(types)
                    CPM = CandidatePairsMapping(list1,
                                                list2,
                                                ent_type = ent_types,
                                                checkpoint_id = checkpoint_id)
                    if not checkpoint_id:
                        CPM.generate_mapping(limit = self.limit)
                        CPM.compute_scores(scores = scores - do_not_compute)
                        CPM.disambiguate(SynonymSetManager._SSM,
                                            human_validation)
                        self._description += f"mapping on: {list1.NAMESPACE}, {list2.NAMESPACE}," + \
                            f"with scores: {' '.join(scores - do_not_compute)}\n"
                    else:
                        CPM.disambiguate(SynonymSetManager._SSM,
                                            human_validation)
                    del(CPM)
        except InterruptedError:
            # CPM.save_json(self.execution_id)
            exit()


    # {list1-list2: type: [scores]}
    strategy = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    @timeit
    def merge_mapping(self,
                      conf_file: str = "",
                      checkpoint_id: str = None,
                      human_validation: bool = False):
        """
        Define the merging strategy (merging order).

        Compute a mapping between classes from namespaces two by two.
        Generates candidate pairs before performing disambiguation.
        Repeat for every mapping until the mapping is done.
        Mapping will take already existing Synonym Sets into account,
        and link a term to a synonym set instead of creating a new one
        if there is a synonym set that already exists for one of the
        candidates.
        """

        with open(conf_file, 'r') as file:
            for i, line in enumerate(file.readlines()):
                i = i + 1
                line = line.split('#')[0].strip()
                if not line:
                    continue # skip line if empty or comment line
                if line.count(':') != 1:
                    raise ValueError(f"Error at line {i} in {conf_file}: " +
                                     f"There must be exactly one ':' per line.")
                extractors, scores = line.split(':')
                if not scores.strip():
                    raise ValueError(f"Error at line {i} in {conf_file}: " +
                                     f"No score set.")
                 # Types
                on_types = set(entity_types.ALL_TYPES)
                begin_types = extractors.find("[")
                if begin_types > 0:
                    end_types = extractors.find("]")
                    if end_types < begin_types:
                        raise ValueError(f"Error at line {i} in {conf_file}: " +
                                         f"[ and ] do not match. There must be exactly one [ and one ].")
                    types_str = extractors[begin_types+1:end_types]
                    on_types_lst = set(types_str.split(','))
                    extractors = extractors[:begin_types] + extractors[end_types+1:].strip()
                    except_types = set()
                    on_types = set()
                    for i, type_ in enumerate(on_types_lst):
                        type_ = type_.strip()
                        if type_ == "all":
                            on_types = set(entity_types.ALL_TYPES)
                            continue
                        elif type_.startswith('-'):
                            type_ = type_[1:].strip()
                            except_types.add(type_)
                        else:
                            on_types.add(type_)
                        if type_ not in entity_types.ALL_TYPES:
                            raise ValueError(f"Error at line {i} in {conf_file}: " +
                                                f"{type_} is not a valid type.\n" +
                                                f"Valid types are: {' '.join(entity_types.ALL_TYPES)}")
                    on_types -= except_types
                if len(on_types) == 0:
                    print(f"Warning at line {i} in {conf_file}: " +
                          f"No types selected. Ignoring.")
                    continue
                extractors = extractors.split(',')
                extractors = [e.strip() for e in extractors if e.strip()]
                if len(extractors) != 2:
                    raise ValueError(f"Error at line {i} in {conf_file}: " +
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
                    if DistanceScorer in scores_to_compute:
                        if ((entity_types.GROUND_OBSERVATORY not in extractor1.POSSIBLE_TYPES and
                             entity_types.TELESCOPE not in extractor1.POSSIBLE_TYPES) or
                            (entity_types.GROUND_OBSERVATORY not in extractor2.POSSIBLE_TYPES and
                             entity_types.TELESCOPE not in extractor2.POSSIBLE_TYPES)):
                            scores_to_compute.remove(DistanceScorer)
                            print(f"Warning at line {i} in {conf_file}: " +
                                  f"Can not compute distance between {extractor1_str} and {extractor2_str}. Ignoring.")
                if (self.graph.is_available(extractor1_str) and
                    self.graph.is_available(extractor2_str)):

                    scores = scores_to_compute - except_scores
                    if scores:
                        if extractor2_str < extractor1_str:
                            extractor1, extractor2 = extractor2, extractor1
                            extractor1_str, extractor2_str = extractor2_str, extractor1_str
                        #for type in on_types:
                        self.strategy[extractor1][extractor2][frozenset(on_types)] = scores
                    else:
                        print(f"Warning at line {i} in {conf_file}: " +
                              f"No score to compute for {extractor1_str} and {extractor2_str}. Ignoring.")
                else:
                    print(f"Warning at line {i} in {conf_file}: " +
                          f"{extractor1_str} and/or {extractor2_str} are not available in the provided ontologies. Ignoring.")

        # execute the strategy
        for extractor1, extractor2_ in self.strategy.items():
            for extractor2, types_ in extractor2_.items():
                for types, scores in types_.items():
                    self.make_mapping_between_lists(extractor1(),
                                                    extractor2(),
                                                    scores = scores,
                                                    types = types,
                                                    checkpoint_id = checkpoint_id,
                                                    human_validation = human_validation)


    @timeit
    def write(self):
        print(f"Writing the result ontology into {self.output_ontology}...")
        self.graph.add_metadata(self._description)
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
         limit: int = -1,
         merging_strategy_file: str = "",
         checkpoint_id: str = None,
         human_validation: bool = False):
    merger = Merger(input_ontologies,
                    output_ontology,
                    limit)
    atexit.register(merger.print_execution_id)

    merger.merge_identifiers()
    merger.merge_mapping(conf_file = merging_strategy_file,
                         checkpoint_id = checkpoint_id,
                         human_validation = human_validation)

    # /!\ Save the synonym sets in the graph (do not remove)
    SynonymSetManager._SSM.save_all()
    merger.write()


if __name__ == "__main__":

    parser = ArgumentParser(
        prog = "merge.py",
        description = "Compute scores between entities from different lists " +
            "and create SynonymPairs & SynonymSets into an output ontology.")

    parser.add_argument("-i",
                        "--input-ontologies",
                        dest = "input_ontologies",
                        nargs = "+",
                        default = [],
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

    parser.add_argument("-l",
                        "--limit",
                        default = -1,
                        type = int,
                        required = False,
                        help = "Set a limit to fasten tests. " +
                        "Only N x N entities will be mapped between each pair of lists.")

    parser.add_argument("-s",
                        "--merging-strategy",
                        dest = "merging_strategy_file",
                        default = CONF_DIR / "merging_strategy.conf",
                        type = str,
                        required = False,
                        help = "Merging strategy file name.")

    parser.add_argument("-c",
                        "--checkpoint",
                        default = None,
                        type = str,
                        help = "Restart scores computation & merging from a previous checkpoint.")


    parser.add_argument("--human-validation",
                        dest = "human_validation",
                        action = "store_true",
                        help = "Disambiguate manually after score computation (use for test purpose).")



    args = parser.parse_args()

    main(args.input_ontologies,
         args.output_ontology,
         args.limit,
         args.merging_strategy_file,
         args.checkpoint,
         args.human_validation)
