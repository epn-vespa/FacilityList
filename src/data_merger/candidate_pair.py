"""
CandidatePair class to store candidate pairs.

CandidatePairManager class can add candidate pairs to the graph
and has methods to keep track of the candidate pairs for each namespace
(one namespace corresponds to one facility list).

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from collections import defaultdict
from enum import Enum
import json
import shutil
from typing import Dict, Generator, List, Type, Union
import uuid
import hashlib
import os
import numpy as np
import re

from rdflib import RDF, XSD, Literal, URIRef
from tqdm import tqdm
from data_merger.scorer.cosine_similarity_scorer import CosineSimilarityScorer
from graph import Graph
from data_merger.scorer.score import Score
from data_merger.scorer.scorer_lists import ScorerLists
from data_merger.synonym_set import SynonymSet, SynonymSetManager
from data_merger.entity import Entity
from data_updater.extractor.extractor import Extractor
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

from config import DATA_DIR, OLLAMA_MODEL # type: ignore
from utils.llm_connection import LLM
from utils.performances import deprecated, timeall, timeit


JSON = DATA_DIR / "checkpoint"# "../../cache/error.log"
JSON.mkdir(parents = True, exist_ok = True)
LATEST = JSON / "latest"


class CandidatePair():
    pass


class CandidatePair():

    candidate_pairs = dict()

    def __new__(cls,
                first: Union[Entity, SynonymSet] = None,
                second: Union[Entity, SynonymSet] = None,
                uri: URIRef = None):
        if uri is not None and uri in cls.candidate_pairs:
            return cls.candidate_pairs[uri]
        else:
            instance = super().__new__(cls)
            return instance


    def __init__(self,
                 first: Union[Entity, SynonymSet] = None,
                 second: Union[Entity, SynonymSet] = None,
                 uri: URIRef = None):
        """
        Instantiate a CandidatePair object. When reading a graph
        with some CandidatePair, use its original URI. Then, retrieve its
        scores and scores values and add it to this candidate pair's scores.

        Keyword arguments:
        first -- first member of the pair
        second -- second member of the pair. Can be an entity or a synonym set.
        node -- the node of the candidate pair in the graph if exists
        """
        self._member1 = first
        self._member2 = second
        self._scores = dict()

        self._list1 = str(first.uri).split('#')[0].split('/')[-1]
        self._list2 = str(second.uri).split('#')[0].split('/')[-1]

        if uri is None:
            uri_str = first.uri.replace('#', '-').replace('/', '_')
            uri = Graph().OBS[str(uuid.uuid4())]
            self._uri = uri
            # self._uri = str(uuid.uuid4())
        else:
            # Retrieve and save the scores of the graph's candidate pair.
            self._uri = uri
            self.init_data()

        CandidatePair.candidate_pairs[uri] = self


    def __repr__(self):
        return f"CandidatePair@{self.uri}"


    @property
    def member1(self) -> Union[Entity, SynonymSet]:
        return self._member1


    @property
    def member2(self) -> Union[Entity, SynonymSet]:
        return self._member2


    @property
    def list1(self) -> str:
        return self._list1


    @property
    def list2(self) -> str:
        return self._list2


    @property
    def uri(self) -> URIRef:
        return self._uri


    @property
    def scores(self) -> dict:
        return self._scores


    def init_data(self):
        graph = Graph()
        for _, score_name, score_value in graph.triples((self.uri,
                                                         None,
                                                         None)):
            if score_name in (graph.OBS["hasMember"],
                              RDF.type):
                continue
            score_name = str(score_name).split('#')[-1]
            score_value = float(score_value)
            self.add_score(score_name, score_value)


    def add_score(self,
                  score_name: str,
                  score: float):
        """
        Add a score to the candidate pair.

        Keyword arguments:
        score_name -- the name of the score (ex: "cos_similarity")
        score -- the float value (between 0 and 1)
        """
        self._scores[score_name] = score


    def get_score(self,
                  score_name: str) -> float:
        """
        Return a score. Return -1 if the score name is not saved.

        Keyword arguments:
        score_name -- the name of the score (ex: "cos_similarity")
        """
        if score_name in self._scores:
            return self._scores[score_name]
        else:
            return -1


    def compute_global_score(self) -> float:
        """
        Return a unique score from the candidate pair's other scores.
        Weighted sum with ReLU (only scores > 0).
        """
        weights = {"fuzzy_levenshtein": 0.3,
                   "acronym": 0.7}
        score = 0
        n_scores = 0
        for score_name, score_value in self._scores.items():
            if score_value < 0:
                # no score were computed
                # negative scores are often used by scorers
                # for candidate pair elimination or when no
                # score could be computed.
                pass
            else:
                weight = weights.get(score_name, 1)
                score += score_value * weight
                n_scores += weight
        if n_scores > 0:
            score = score / n_scores
        self.add_score(score_name = "global",
                       score = score) # averaged score
        return score


    def save_to_graph(self,
                      decisive_score: str,
                      justification: str = None):
        """
        Add a Candidate Pair in the graph. Use it after
        a Synonym Set was created from this Candidate Pair to keep
        track of the Synonym Set's origin.

        Keyword arguments:
        decisive_score -- score on which the Synonym Set was decided
        justification -- text that explains why this decision was taken
        """
        graph = Graph()

        candidate_pair_uri = self.uri

        # Add the candidate pair in the graph
        graph.add((candidate_pair_uri, RDF.type,
                   graph.OBS["SynonymPair"])) # Not a Candidate anymore
        graph.add((candidate_pair_uri, graph.OBS["hasMember"],
                   self.member1.uri))
        graph.add((candidate_pair_uri, graph.OBS["hasMember"],
                   self.member2.uri))
        for score, value in self.scores.items():
            graph.add((candidate_pair_uri,
                        graph.OBS[score],
                        Literal(value, datatype = XSD.float)))
        if decisive_score:
            graph.add((candidate_pair_uri,
                    graph.OBS["provenance"],
                    Literal(decisive_score)))
        if justification:
            graph.add((candidate_pair_uri,
                       graph.OBS["justification"],
                       Literal(justification)))


    def __eq__(self,
               candidate_pair: CandidatePair) -> bool:
        if candidate_pair is None:
            return False
        return (candidate_pair.member1 == self.member1 and candidate_pair.member2 == self.member2 or
                candidate_pair.member1 == self.member2 and candidate_pair.member2 == self.member1)


    def __hash__(self) -> int:
        m1 = str(self.member1)
        m2 = str(self.member2)
        str_ver = m1 + '_' + m2 if m1 < m2 else m2 + '_' + m1
        return int(hashlib.sha256(str_ver.encode('utf-8')).hexdigest(), 16) % 10**8


class State(Enum):
    """
    Used to return a state after computing a score.
    """
    ADMITTED = 1
    ELIMINATED = 2
    UNCLEAR = 3


class CandidatePairsManager():
    """
    Save the candidate pairs inside a dictionary of sets. Useful to manage
    candidate pairs by facility lists. Every mapping of two lists should have
    its own CandidatePairsManager.
    This class is optimized for computation when we already have a fixed amount
    of candidate pairs to disambiguate, like in NAIF-Wikidata. For a full-mapping
    disambiguation, use CandidatePairsMapping class.
    """

    def __init__(self,
                 list1: Extractor,
                 list2: Extractor):
        # to get candidate pairs by entity
        self._candidate_pairs_dict = defaultdict(list)
        # to loop over candidate pairs
        self._candidate_pairs = list()
        self._list1 = list1
        self._list2 = list2


    def __del__(self):
        del(self._candidate_pairs)
        del(self._candidate_pairs_dict)


    @property
    def candidate_pairs(self) -> List[CandidatePair]:
        return self._candidate_pairs


    @property
    def candidate_pairs_dict(self) -> Dict[str, CandidatePair]:
        return self._candidate_pairs_dict


    @property
    def list1(self):
        return self._list1


    @property
    def list2(self):
        return self._list2


    def add_candidate_pairs(self,
                            candidate_pairs: Union[CandidatePair,
                                                   List[CandidatePair]]):
        """
        Add one or more candidate pair(s) to the candidate pairs manager.

        Keyword arguments:
        candidate_pairs -- the candidate pair(s) to add
        """
        if type(candidate_pairs) != list:
            candidate_pairs = [candidate_pairs]
        for candidate_pair in candidate_pairs:
            # Verification takes too much time
            #if candidate_pair not in self.candidate_pairs_dict[candidate_pair.member1]:
            self.candidate_pairs_dict[candidate_pair.member1].append(candidate_pair)
            #if candidate_pair not in self.candidate_pairs_dict[candidate_pair.member2]:
            self.candidate_pairs_dict[candidate_pair.member2].append(candidate_pair)
            #if candidate_pair not in self.candidate_pairs:
            self.candidate_pairs.append(candidate_pair)


    def get_candidate_pairs(self,
                            entity: URIRef) -> set:
        """
        Get the candidate pairs for an entity.

        Keword arguments:
        entity -- the entity to get candidate pairs from.
        """
        if entity not in self._candidate_pairs_dict:
            return []
        return self._candidate_pairs_dict[entity]


    def get_candidate_pair(self,
                           entity1: Union[Entity, SynonymSet],
                           entity2: Union[Entity, SynonymSet]) -> CandidatePair:
        """
        Get a candidate pair object from the namespaces and entities.
        """
        for pair in self.candidate_pairs:
            if (pair.member1 == entity1 and pair.member2 == entity2
                or pair.member2 == entity1 and pair.member1 == entity2):
                return pair
        # Absent from the Candidate Pair Manager.
        # We create a new candidate_pair, save it and return it.
        candidate_pair = CandidatePair(entity1, entity2)

        self.add_candidate_pairs(candidate_pair)

        return candidate_pair


    def del_candidate_pair(self,
                           candidate_pair: CandidatePair):
        """
        Remove one candidate pair from the list of candidate pairs.
        Do not delete the other candidate pairs in the line and column
        (candidate pairs that share one entity with this candidate pair).

        Keyword arguments:
        candidate_pair -- the candidate pair to remove.
        """
        self._candidate_pairs.remove(candidate_pair)
        self._candidate_pairs_dict[candidate_pair.member1].remove(candidate_pair)
        self._candidate_pairs_dict[candidate_pair.member2].remove(candidate_pair)
        del CandidatePair.candidate_pairs[candidate_pair.uri]


    def del_candidate_pairs(self,
                            entity: Union[Entity, SynonymSet]):
        """
        Remove all candidate pairs that contain the entity.

        Keyword arguments:
        entity -- the entity to remove pairs that contain it.
        """
        for pair in self.candidate_pairs:
            if (pair.member1 == entity or
                pair.member2 == entity):
                self.del_candidate_pair(pair)
        if entity in self.candidate_pairs_dict:
            del self.candidate_pairs_dict[entity]


    @timeit
    def save_all(self):
        """
        Save the remaining candidate pairs into the graph.
        This will add CandidatePair entities in the graph
        in case they were not all disambiguated.
        """
        graph = Graph()
        for candidate_pair in tqdm(self.candidate_pairs,
                                   desc = f"Saving Candidate Pairs for {self._list1}, {self._list2}"):
            candidate_pair_uri = candidate_pair.uri

            # Add the candidate pair in the graph
            graph.add((candidate_pair_uri, RDF.type,
                       graph.OBS["CandidatePair"]))
            graph.add((candidate_pair_uri, graph.OBS["hasMember"],
                       candidate_pair.member1.uri))
            graph.add((candidate_pair_uri, graph.OBS["hasMember"],
                       candidate_pair.member2.uri))
            for score, value in candidate_pair.scores.items():
                graph.add((candidate_pair_uri,
                           graph.OBS[score],
                           Literal(value, datatype = XSD.float)))


    @timeit
    def save_json(self,
                  execution_id: str):
        """
        Save candidate pairs in a json format to prevents saving
        all Candidate Pairs in the ontology (there might be billions of CP).
        Save the scores into a json file. For scores, unlike synonym sets,
        we do not save them in the ontology.

        Keyword arguments:
        execution_id -- the id of this execution (generated in merge.py)
        """
        # Empty latest directory
        LATEST.mkdir(parents = True, exist_ok = True)
        for latest in os.listdir(LATEST):
            if not execution_id in latest:
                os.rename(LATEST / latest, JSON / latest)
        directory = LATEST / execution_id
        directory.mkdir(parents = True, exist_ok = True)

        filename = f"{self._list1}_{self._list2}.json"
        path = directory / filename
        with open(str(path), 'w') as file:
            file.write("{\n")
            for cp in self._candidate_pairs:# ._candidate_pairs:
                file.write("\"" + str(cp.member1.uri) + '|' + str(cp.member2.uri) + "\":")
                file.write(str(cp.scores))
                #for score, value in cp.scores.items():
                #    file.write('\t' + score + '\t' + str(value))
                file.write(',\n')

            file.write("\n}")
        #with open(filename, 'w') as file:
        #    json.dump(data, file)


    @timeit
    def disambiguate_candidates(self,
                                scores: List[Score] = None):
        """
        Disambiguate only entities that already have a candidate pair.
        This is useful for lists where we already have an idea of what to
        disambiguate (example: wikidata / naif with ambiguous identifiers).

        Keyword arguments:
        scores -- list of scores to perform. If None, perform on all scores.
                  Only take discriminant scores.
        """
        for score in ScorerLists.DISCRIMINANT_SCORES:
            if score not in scores:
                continue
            for candidate_pair in tqdm(self.candidate_pairs.copy(),
                                       desc = f"Computing {score.NAME} on {self._list1}, {self._list2}"):
                # discriminant
                self.compute(score = score,
                             candidate_pair = candidate_pair,
                             create_synset = True)
            # TODO verify that there is no candidate pairs left and no orpheline from the smaller list
            # (naif/wikidata: DSS-05 is orpheline)
            # for candidate pairs that remain, we must perform the disambiguation algorithm (eliminate candidates from highest average score to lowest)


    def compute(self,
                score: Score,
                candidate_pair: CandidatePair,
                create_synset: bool = False) -> State:
        """
        Compute the score for a candidate pair. Add the candidate pair
        in a synset if the score was discriminant and remove the
        candidate pair from the candidate pairs list.

        Keyword arguments:
        score -- the Score to compute
        candidate_pair -- the candidate pair of entities to compare
        """
        graph = Graph()
        score_value = score.compute(graph,
                                    candidate_pair.member1,
                                    candidate_pair.member2)
        candidate_pair.add_score(score.NAME, score_value)
        if ScorerLists.ELIMINATE.get(score, lambda x: False)(score_value):
            # self.candidate_pairs.remove(candidate_pair)
            self.del_candidate_pair(candidate_pair)
            return State.ELIMINATED
        elif (create_synset and
              ScorerLists.ADMIT.get(score, lambda x: False)(score_value)):
            # Remove from candidate pairs
            self.candidate_pairs.remove(candidate_pair)

            # Remove the candidate pairs that contain member1 & member2
            # (they do not need to be mapped anymore)
            self.del_candidate_pairs(candidate_pair.member1)
            self.del_candidate_pairs(candidate_pair.member2)

            # Add it to the graph
            SynonymSetManager._SSM.add_synset(candidate_pair.member1,
                                              candidate_pair.member2)
            # Add Candidate Pair to graph for traceability
            candidate_pair.add_score(score_name = score.NAME, score = score_value)
            candidate_pair.save_to_graph(decisive_score = score.NAME)
            return State.ADMITTED
        else:
            # The candidate pair needs to be re-processed with other scores
            return State.UNCLEAR


class CandidatePairsMapping():
    """
    Generate a mapping between all entities of two lists.
    Use a 2D list instead of a list and dict (see CandidatePairsManager).
    This mapping is then used to optimize the selection of the right mapping.
    """

    def __init__(self,
                 list1: Extractor,
                 list2: Extractor,
                 checkpoint_id: str = None):
        self._list1 = list1
        self._list2 = list2

        self._mapping = [] # 2D list to represent the mapping (graph)
        # lines: list1,
        # columns: list2
        self._list1_indexes = [] # indexes by entity in the mapping
        self._list2_indexes = [] # indexes by entity in the mapping

        if checkpoint_id:
            self.load_checkpoint(checkpoint_id)


    @property
    def list1(self):
        return self._list1


    @property
    def list2(self):
        return self._list2


    def __len__(self):
        if not self._mapping:
            return 0
        # count None
        none = 0
        for l in self._mapping:
            none += l.count(None)
        return len(self._mapping) * len(self._mapping[0]) - none


    def __iter__(self):
        self.row_index = len(self._mapping) - 1
        if len(self._mapping) == 0:
            return self
        self.col_index = len(self._mapping[0]) - 1
        return self


    def _increment(self):
        """
        Increment from bottom to top & from right to left.
        """
        self.col_index -= 1
        if self.col_index < 0:
            self.row_index -= 1
            self.col_index = len(self._mapping[0]) - 1


    def __next__(self):
        mapping = self._mapping
        rows = len(mapping)
        if self.row_index < 0 or rows == 0 or len(mapping[0]) == 0:
            raise StopIteration
        current_element = None
        while self.row_index >= 0:
            current_element = mapping[self.row_index][self.col_index]
            self._increment()
            if current_element is not None:
                return current_element
        raise StopIteration


    def iter_mapping(self) -> Generator[int, int, CandidatePair]:
        """
        Optimized method to loop over Candidate Pairs.
        """
        mapping = self._mapping
        if not mapping or not mapping[0]:
            return  # mapping vide ou lignes vides

        for i in reversed(range(len(self._mapping))):
            for j in reversed(range(len(self._mapping[0]))):
                candidate_pair = self._mapping[i][j]
                if candidate_pair is not None:
                    yield (i, j, candidate_pair)


    @timeit
    def generate_mapping(self,
                         limit: int = -1,
                         entities1: List[Entity] = None,
                         entities2: List[Entity] = None):
        """
        Generate candidate pairs between both lists. Only
        generate candidate pairs for entities that are not linked
        to each other's list already.
        [[CandidatePair1.1, CandidatePair1.2],
         [CandidatePair2.1, CandidatePair2.2]]

        TODO create a relation differentFrom for each relation
        that is eliminated, in order to prevent multiple mapping ?
        /!\ OR create a relation "noEquivalentInList source_list" (easier)

        Keyword arguments:
        limit -- do not generate a mapping for the whole lists (use for tests)
        """
        graph = Graph()
        if self._mapping:
            # do not generate twice
            return
        entities1 = graph.get_entities_from_list(self._list1,
                                                 no_equivalent_in = self._list2,
                                                 limit = limit)
        entities1 = list(entities1)

        entities2 = graph.get_entities_from_list(self._list2,
                                                 no_equivalent_in = self._list1,
                                                 limit = limit)
        entities2 = list(entities2)

        self._mapping = []

        # Initialize the 2D array
        for _ in range(len(entities1)):
            self._mapping.append([None] * len(entities2))

        # Fill the 2D array
        for i, (entity1_uri, synset1_uri) in enumerate(tqdm(entities1,
                                                       desc = f"Generating mapping for {self._list1}, {self._list2}")):
            if synset1_uri is not None:
                entity1 = SynonymSet(uri = synset1_uri)
            else:
                entity1 = Entity(entity1_uri)
            self._list1_indexes.append(entity1)
            for j, (entity2_uri, synset2_uri) in enumerate(entities2):

                if synset2_uri is not None:
                    entity2 = SynonymSet(uri = synset2_uri)
                else:
                    entity2 = Entity(uri = entity2_uri)
                if i == 0: # Only append for the first loop.
                    self._list2_indexes.append(entity2)

                """
                cp_uri = list(graph.get_candidate_pair_uri(entity1.uri,
                                                           entity2.uri))
                if cp_uri:
                    cp_uri, = cp_uri[0]
                else:
                    cp_uri = None
                """
                cp_uri = None
                cp = CandidatePair(entity1, entity2, cp_uri)
                self._mapping[i][j] = cp


    def del_candidate_pairs(self,
                            entity: Union[Entity, SynonymSet]):
        """
        Remove all candidate pairs involving the given entity from the
        mapping and index lists.

        Keyword arguments:
        entity -- the Entity or SynonymSet to remove from mapping
        """
        if entity in self._list1_indexes:
            entity_index = self._list1_indexes.index(entity)
            self._mapping.pop(entity_index)
            self._list1_indexes.pop(entity_index)
        elif entity in self._list2_indexes:
            entity_index = self._list2_indexes.index(entity)
            for row in self._mapping:
                if entity_index < len(row):
                    row.pop(entity_index)
            self._list2_indexes.pop(entity_index)


        # Remove from the list too
        """
        i = len(self._candidate_pairs) - 1
        while i >= 0:
            pair = self._candidate_pairs[i]
            if pair.member1 == entity or pair.member2 == entity:
                del(self._candidate_pairs[i])
            i -= 1
        """

    def del_candidate_pair(self,
                           candidate_pair: CandidatePair):
        """
        Delete a candidate pair from the mapping. Do not delete the
        other candidate pairs in the line and column (candidate pairs that
        share one entity with this candidate pair).

        Keyword arguments:
        candidate_pair -- the candidate pair to delete
        """
        index1, index2 = self._get_indexes(candidate_pair)
        if index1 is not None:
            self._mapping[index1][index2] = None


    def _get_indexes(self,
                     candidate_pair: CandidatePair):
        """
        Get indexes in the mapping for a candidate pair. Then to retrieve the
        candidate pair from the mapping, use self._mapping[x][y].

        Keyword arguments:
        candidate_pair -- the candidate pair to get indexes from.
        """
        try:
            index1 = self._list1_indexes.index(candidate_pair.member1)
            index2 = self._list2_indexes.index(candidate_pair.member2)
            return index1, index2
        except ValueError:
            # index1 not in list1 or index2 not in list 2
            try:
                index1 = self._list1_indexes.index(candidate_pair.member2)
                index2 = self._list2_indexes.index(candidate_pair.member1)
                return index1, index2
            except ValueError:
                return None, None


    def _compute(self,
                 score: Score,
                 candidate_pair: CandidatePair) -> State:
        """
        Compute the score for a candidate pair. Add the candidate pair
        in a synset if the score was discriminant and remove the
        candidate pair from the candidate pairs list.

        Keyword arguments:
        score -- the Score to compute
        candidate_pair -- the candidate pair of entities to compare
        """
        if candidate_pair is None:
            return None
        graph = Graph()
        score_value = score.compute(graph,
                                    candidate_pair.member1,
                                    candidate_pair.member2)

        if ScorerLists.ELIMINATE.get(score, lambda x: False)(score_value):
            self.del_candidate_pair(candidate_pair)
            return State.ELIMINATED
        elif ScorerLists.ADMIT.get(score, lambda x: False)(score_value):
            self.del_candidate_pairs(candidate_pair.member1)
            self.del_candidate_pairs(candidate_pair.member2)
            # Add it to the graph
            SynonymSetManager._SSM.add_synset(candidate_pair.member1,
                                              candidate_pair.member2)
            # Add Candidate Pair to graph for traceability
            candidate_pair.add_score(score_name = score.NAME, score = score_value)
            candidate_pair.save_to_graph(decisive_score = score.NAME)
            return State.ADMITTED

        candidate_pair.add_score(score.NAME, score_value)
        return State.UNCLEAR


    def _compute_one_score(self,
                           score: Score,
                           candidate_pair: CandidatePair):
        """
        Compute a score without removing the candidate pair from the mapping
        at all. Use this for non-discriminant scores.
        """
        if candidate_pair is None:
            return
        graph = Graph()
        score_value = score.compute(graph,
                                    candidate_pair.member1,
                                    candidate_pair.member2)

        candidate_pair.add_score(score.NAME, score_value)


    @deprecated
    def _compute_all(self,
                     candidate_pair_list: List,
                     score: Score) -> None:
        """
        Compute all the scores & save them in the candidate pairs.
        Use this function to loop on the mapping on non-discriminant scores.
        It can be used with multithreading as it won't remove candidate pairs
        from the mapping.

        Keyword arguments:
        candidate_pair_list -- a row of the mapping
        score -- the score to compute
        """
        for candidate_pair in candidate_pair_list:
            if candidate_pair is None:
                continue
            score_value = score.compute(Graph(),
                                        candidate_pair.member1,
                                        candidate_pair.member2)
            candidate_pair.add_score(score.NAME, score_value)


    def compute_scores(self,
                       scores: List[Score]):
        """
        Eliminate entities that are incompatible and select
        entities that are compatible.

        Keyword arguments:
        scores -- list of scores to perform. If None, perform on all scores.
        """
        if scores is None:
            discriminant_scores = ScorerLists.DISCRIMINANT_SCORES
            other_scores = ScorerLists.OTHER_SCORES
            cuda_scores = ScorerLists.CUDA_SCORES
        else:
            discriminant_scores = []
            other_scores = []
            cuda_scores = []
            for score in ScorerLists.DISCRIMINANT_SCORES:
                if score in scores:
                    discriminant_scores.append(score)
            for score in ScorerLists.OTHER_SCORES:
                if score in scores:
                    other_scores.append(score)
            for score in ScorerLists.CUDA_SCORES:
                if score in scores:
                    cuda_scores.append(score)

        print(f"Count of candidate pairs: {len(self)}")
        self._disambiguate_discriminant(discriminant_scores)
        self._compute_cuda_scores(cuda_scores)
        self._compute_other_scores(other_scores)

        # Create a global score for each candidate pair
        self._compute_global()


    @timeit
    def disambiguate(self,
                     SSM: SynonymSetManager,
                     human_validation: bool):
        """
        Disambiguation algorithm: find the best global score,
        create a Synonym Set if high enough until stop.

        Human verification (input): Assisted Disambiguation.

        Keyword arguments:
        SSM -- this run's SynonymSetManager
        """
        scores = []
        none_cp = 0
        for l in self._mapping:
            scores_l = []
            for candidate_pair in l:
                if candidate_pair is None:
                    none_cp += 1
                    scores_l.append(np.nan)
                else:
                    if "global" in candidate_pair.scores:
                        scores_l.append(candidate_pair.scores.get("global"))
                    else:
                        scores_l.append(candidate_pair.compute_global_score())
            scores.append(scores_l)
        if not scores:
            print("Nothing to disambiguate.")
            return
        scores = self._transform_scores(scores)

        # Human validation
        if human_validation:
            self.human_validation(scores, SSM)

        # LLM validation
        else:
            self.ai_validation(scores, SSM)

    def _transform_scores(self,
                         scores: List[float]) -> np.array:
        """
        Transform scores array into a numpy array
        while standardizing it.

        Keyword arguments:
        scores -- list of scores with the same coordinates as
                  the mapping
        """
        scores = np.array(scores, dtype = float)
        # Standardize scores with mean and standard deviation by cols
        col_means = np.nanmean(scores, axis = 0, keepdims = True)
        col_std = np.nanstd(scores, axis = 0, keepdims = True)
        col_std[col_std == 0] = 1.0
        scores = (scores - col_means) / col_std # Standardize

        # Standardize scores by rows too
        row_means = np.nanmean(scores, axis = 1, keepdims = True)
        row_std = np.nanstd(scores, axis = 1, keepdims = True)
        row_std[row_std == 0] = 1.0
        scores = (scores - row_means) / row_std
        return scores


    PROMPT_BASE = "You are an ontology matching tool, able to detect semantical, spatio-temporal differences and similarities within entities. " + \
        "You have to answer this questions: are those two entities the same ? " + \
        "Additionally, a satellite that is part of a mission, " + \
        "or telescope that is a part of an observatory, are distinct.\n" + \
        "Only select one choice from below:" + \
        "\nsame\ndistinct\n" + \
        "Reply with this format:\n" + \
        "**<choice>**\n" + \
        "<Justification (why do you think it is the good choice)>\n\n" + \
        "Example:\n" + \
        "**same**\n" + \
        "both refer to the same observatory, just with slightly different naming conventions. therefore, they are the same entity.\n" + \
        "\n\nExample 2:\n" + \
        "**distinct**\n" + \
        "one of the entities refer to the spacecraft, while the other to the mission. therefore, the second entity is a part of the first one.\n"


    @timeall
    def ai_validation(self,
                      scores: np.array,
                      SSM: SynonymSetManager):
        """
        Ask a LLM to take decisions for the highest scores
        iteratively, like in human_validation.

        Keyword arguments:
        scores -- a 2D array of the scores of the candidate pairs
        SSM -- this run's SynonymSetManager
        """
        choice = None


        # TODO: define a stop condition (looped unsuccessfully n times, for example 5% of the CandidatePairs ?)
        n_fail = 0
        score = np.nanargmax(scores)
        n_success = 0
        # std_dev
        std_dev = np.nanstd(scores)
        mean = np.nanmean(scores)
        # Eliminate 97.5 %
        threshold = mean + 1.96 * std_dev
        print(threshold)
        while len(scores) and choice != "2" and score > threshold:
            #if n_fail > n_success :
                # Failed more time than it succeeded:
            #    break

            left = np.sum(np.where(np.isnan(scores), 0, 1))
            # Stop condition: nans are more than 5% of the table ?
            print(f"\n\n\tThere are {left} candidate pairs to review.")
            if np.isnan(scores).all():
                break
            x, y = np.unravel_index(np.nanargmax(scores), scores.shape)
            score = scores[x][y]
            if score < 0:
                break # Stop condition: score is too low

            best_candidate_pair = self._mapping[x][y]
            member1 = best_candidate_pair.member1
            member2 = best_candidate_pair.member2

            exclude = ["code",
                       "url",
                       "NSSDCA_ID",
                       "uri",
                       "ext_ref",
                       "COSPAR_ID",
                       "NAIF_ID",
                       "MPC_ID",
                       "equivalent_class",
                       "type_confidence",
                       "location_confidence"]
            prompt = "Entity1: " + member1.to_string(exclude=exclude)[:500] + "\n\n"
            prompt += "Entity2: " + member2.to_string(exclude=exclude)[:500] + "\n\n"
            prompt += self.PROMPT_BASE
            response = LLM().generate(prompt)
            answer, justification = self._parse_ai_response(response)
            if answer == "same":
                best_candidate_pair.save_to_graph(decisive_score = f"{OLLAMA_MODEL} validation",
                                                  justification = justification)
                scores = np.delete(scores, x, axis = 0)
                scores = np.delete(scores, y, axis = 1)
                self.del_candidate_pairs(member1)
                self.del_candidate_pairs(member2)
                SSM.add_synset(member1, member2)
                n_success += 1
            elif answer == "distinct":
                self.del_candidate_pair(best_candidate_pair)
                scores[x][y] = np.nan
                n_fail += 1
        # Now review once the best scores in lines & cols
        for x in range(len(self._mapping)):
            y = np.unravel_index(np.nanargmax(scores[x]), scores[x].shape)


    def _parse_ai_response(self,
                           response: str) -> tuple:
        """
        Parse the AI's response. Return the answer and the justification.

        Keyword arguments:
        repsonse -- a string generated by a LLM.
        """
        answers = re.findall(r"\*\*.+\*\*", response)
        if len(answers) == 1:
            answer_str = answers[0]
            answer = answer_str[2:-2]
        elif len(answers) == 0:
            raise ValueError(f"{OLLAMA_MODEL} did not reply a valid answer (format: **<answer>**).\nResponse was: {response}")
        else:
            raise ValueError(f"{OLLAMA_MODEL} replied more than one answer.\nResponse was: {response}")
        print("answer:", answer)
        justification = response.replace(answer_str, "").strip()
        print("justification:", justification)
        return answer, justification


    def human_validation(self,
                         scores: np.array,
                         SSM: SynonymSetManager):
        """
        Human validation algorithm. Propose candidate pairs that are
        the most likely to be correct according to the computed scores.
        Ask the user for a feedback interactively at each step.

        Keyword arguments:
        scores -- a 2D array of the scores of the candidate pairs
        SSM -- this run's SynonymSetManager
        """
        choice = None
        while len(scores) and choice != "2":
            left = np.sum(np.where(np.isnan(scores), 0, 1))
            print(f"\n\n\tThere are {left} candidate pairs to review.")
            if np.isnan(scores).all():
                break
            x, y = np.unravel_index(np.nanargmax(scores), scores.shape)
            score = scores[x][y]

            #if score == 0:
            #    print("Best score is 0. Every Candidate Pair is eliminated.")
            #    break
            best_candidate_pair = self._mapping[x][y]
            member1 = best_candidate_pair.member1
            member2 = best_candidate_pair.member2
            self._align_repr(member1, member2)

            # Human validation
            choice = input(f"Highest score {score} for\n{member1},\n{member2}.\nSame: 0 (default)\nDifferent: 1\nStop: 2\nisPartOf: 3\nhasPart: 4\ncheck 5 best: 5\n>>> ")
            if choice == "5":
                non_nan_idx = ~np.isnan(scores[x])
                top5_idx = np.argsort(scores[x][non_nan_idx])[-5:][::-1]
                for i, idx in enumerate(top5_idx):
                    pair = self._mapping[x][idx]
                    print(f"{i}. {round(scores[x][idx], 2)} {pair.member2.get_values_for('label')}")
                choice2 = input(f"Which entity seems to match {member1.get_values_for('label')} ? None of them (default)\n>>> ")
                if choice2.strip().isdigit() and int(choice2) in range(0, 5):
                    print("choice2 = ", choice2)
                    y2 = top5_idx[int(choice2)]
                    scores[x][y2] = score + 1 # higher than the current CP's score.
                    continue # Will check for this pair next.
                else:
                    # Delete 5 pairs (& replace score by np.nan)
                    print("choice2 = ", choice2)
                    for y2 in top5_idx:
                        self.del_candidate_pair(self._mapping[x][y2])
                        scores[x][y2] = np.nan

            elif choice == "4":
                Graph().add((member1.uri, "has_part", member2.uri))
                Graph().add((member2.uri, "is_part_of", member1.uri))
                self.del_candidate_pair(best_candidate_pair)
                scores[x][y] = np.nan
                continue
            elif choice == "3":
                Graph().add((member1.uri, "is_part_of", member2.uri))
                Graph().add((member2.uri, "has_part", member1.uri))
                self.del_candidate_pair(best_candidate_pair)
                scores[x][y] = np.nan
                continue
            elif choice == "2":
                cancel = input("Leaving disambiguation... Type anything to cancel: ")
                break
            elif choice == "1":
                cancel = input("Did not create a Synonym Set. Type anything to cancel: ")
                if cancel != "":
                    continue
                self.del_candidate_pair(best_candidate_pair)
                scores[x][y] = np.nan
                continue
            cancel = input(f"Create a synonym set with {member1}, {member2}. Type anything to cancel: ")
            if cancel:
                continue
            scores = np.delete(scores, x, axis = 0)
            scores = np.delete(scores, y, axis = 1)
            self.del_candidate_pairs(member1)
            self.del_candidate_pairs(member2)
            SSM.add_synset(member1, member2)
            best_candidate_pair.save_to_graph(decisive_score = "human validation")


    def _align_repr(self,
                    member1: Union[Entity, SynonymSet],
                    member2: Union[Entity, SynonymSet]):
        """
        Print representation of two candidate pairs aligned on their attributes
        in a string.

        Keyword arguments:
        member1 -- the first entity (or synonym set).
        member2 -- the second entity (or synonym set).
        """
        n_cols = shutil.get_terminal_size(fallback=(80,20)).columns
        col_width = n_cols // 2 - 4

        all_keys = sorted(set(member1.data.keys()) | set(member2.data.keys()))

        rows = []
        largest_key_len = 0
        for key in all_keys:
            key = Graph().OM.get_attr_name(key)
            if len(key) > largest_key_len:
                largest_key_len = len(key)
            val1 = '\n'.join([str(x) for x in member1.get_values_for(key)])
            val2 = '\n'.join([str(x) for x in member2.get_values_for(key)])
            rows.append([key, val1, val2])
        col_width -= largest_key_len // 2
        print("*" * largest_key_len + " " + "*" * col_width + " " + "*" * col_width )
        for key, val1, val2 in rows:
            if key in ["source"]:
                continue
            val1 = str(val1) if val1 else ""
            val2 = str(val2) if val2 else ""
            # split val1 & val2 to a table
            val1_rows = []
            val2_rows = []
            while val1:
                val1_rows.extend([x[:col_width] for x in val1.split("\n")])
                val1 = val1[col_width:]
            while val2:
                # val2_rows.extend(val2[:col_width])
                val2_rows.extend([x[:col_width] for x in val2.split("\n")])
                val2 = val2[col_width:]
            if len(val1_rows) < len(val2_rows):
                val1_rows.extend([""] * (len(val2_rows)-len(val1_rows)))
            elif len(val2_rows) < len(val1_rows):
                val2_rows.extend([""] * (len(val1_rows)-len(val2_rows)))
            for i, (col1, col2) in enumerate(zip(val1_rows, val2_rows)):
                if i == 0:
                    print(key + " " * (largest_key_len - len(key)), end = "|")
                else:
                    print(" " * (largest_key_len), end = "|")
                print(col1 + " " * (col_width - len(col1)), end = "|")
                print(col2) # \n
                if i == 3:
                    break # Only print 3 lines per attr


    @timeit
    def _disambiguate_discriminant(self,
                                   scores: List[Type[Score]]):
        """
        Disambiguate discriminant scores. This will remove
        the candidate pairs for which we are sure that they are not
        compatible or they refer to the same entity.

        Keyword arguments:
        scores -- list of scores to compute.
        """
        for score in ScorerLists.DISCRIMINANT_SCORES:
            if score not in scores:
                continue
            state = None
            i = 0
            print(f"Computing {score.NAME} on {self._list1}, {self._list2}")
            print(f"0/{len(self._mapping)}")
            while i < len(self._mapping):
                print(f"\033[F\033[{0}G {i+1}/{len(self._mapping)}")
                candidate_pair_list = self._mapping[i]
                # discriminant
                for candidate_pair in candidate_pair_list:
                    state = self._compute(score = score,
                                          candidate_pair = candidate_pair)
                    if state == State.ADMITTED:
                        break
                if state == State.ADMITTED:
                    i += 0 # The mapping's size was reduced so
                    # if we increment i, it will jump over the next element.
                    # self._mapping[i][self._list2_indexes.index(candidate_pair.member2)]
                else:
                    i += 1


    @timeit
    def _compute_cuda_scores(self,
                             scores: List[Type[Score]]):
        """
        Compute scores that cannot be computed in a multiprocess (because they
        use CUDA) for the remaining candidate pairs.
        Create batches for candidate pairs.

        Keyword arguments:
            scores -- Scores that use CUDA
        """
        for score in scores:
            len_e1 = len(self._list1_indexes)
            len_e2 = len(self._list2_indexes)
            n_candidates = len_e1 * len_e2
            print(f"Computing {score.NAME} on {self._list1}, {self._list2}" +
                  f" on {n_candidates} candidate pairs.")
            if score == CosineSimilarityScorer:
                # this score does batches for performances.
                entities1, entities2 = zip(*[(x.member1, x.member2) for x in self])
                score_values = score.compute(Graph(),
                                             entities1,
                                             entities2)
                for score_value, candidate_pair in zip(score_values, self):
                    candidate_pair.add_score(score.NAME, score_value)
                continue
            for _, _, candidate_pair in self.iter_mapping():
                score_value = score.compute(Graph(),
                                            candidate_pair.member1,
                                            candidate_pair.member2)
                candidate_pair.add_score(score.NAME, score_value)


    @timeit
    def _compute_other_scores(self, scores: List[Score]):
        """
        Compute other scores for the remaining candidate pairs.

        Keyword arguments:
            scores -- Scores that use CUDA
        """
        if not scores:
            return
        """
        DEBUG = True
        if DEBUG:
            for _, _, candidate_pair in self.iter_mapping():
                score_values = _compute_scores(candidate_pair.member1,
                                               candidate_pair.member2,
                                               candidate_pair.uri,
                                               scores)
                for score, score_value in zip(scores, score_values):
                    candidate_pair.add_score(score.NAME, score_values)
            return
        """
        with ProcessPoolExecutor() as executor:
            print(f"Computing other scores for the remaining candidate pairs.")
            candidate_pairs = [(pair.member1, pair.member2, pair.uri)
                               for _, _, pair in self.iter_mapping()]
            futures = [executor.submit(_compute_scores,
                                       member1,
                                       member2,
                                       uri,
                                       scores) for member1, member2, uri in candidate_pairs]
            for future in tqdm(as_completed(futures), total = len(candidate_pairs)):
                score_values, candidate_pair_uri = future.result()
                for score, score_value in zip(scores, score_values):
                    # Make sure that we add it in the right CandidatePair.
                    CandidatePair.candidate_pairs[candidate_pair_uri].add_score(score.NAME, score_value)


    def _compute_global(self):
        """
        Compute the global score for each candidate pair.
        """
        for _, _, candidate_pair in self.iter_mapping():
            candidate_pair.compute_global_score()


    @deprecated
    @timeit
    def save_to_graph(self):
        """
        Deprecated:
            We do not want to save a whole mapping into the Ontology.
        """
        graph = Graph()
        for candidate_pair_list in tqdm(self._mapping, #.copy(), # DOING try to see if we need .copy() or not
                                        desc = f"Saving Candidate Pairs for {self._list1}, {self._list2}"):
            for candidate_pair in candidate_pair_list:
                if not candidate_pair:
                    continue
                candidate_pair_uri = candidate_pair.uri

                # Add the candidate pair in the graph
                graph.add((candidate_pair_uri, RDF.type,
                        graph.OBS["CandidatePair"]))
                graph.add((candidate_pair_uri, graph.OBS["hasMember"],
                        candidate_pair.member1.uri))
                graph.add((candidate_pair_uri, graph.OBS["hasMember"],
                        candidate_pair.member2.uri))
                for score, value in candidate_pair.scores.items():
                    graph.add((candidate_pair_uri,
                               graph.OBS[score],
                               Literal(value, datatype = XSD.float)))


    def load_checkpoint(self,
                        checkpoint_id: str):
        """
        Load a checkpoint

        Keyword arguments:
        checkpoint_id -- the checkpoint ID to retrieve candidate pairs from
        """
        if checkpoint_id == "latest":
            latest = os.listdir(LATEST)
            if len(latest) > 1:
                raise ValueError(f"There are more than one latest checkpoints. Please move out a checkpoint from {LATEST}.")
            elif len(latest) == 0:
                raise FileExistsError(f"There is no checkpoint in {LATEST}.")
            checkpoint = LATEST / latest[0]

        elif (LATEST / checkpoint_id).exists():
            checkpoint = LATEST / checkpoint_id
        elif (JSON / checkpoint_id).exists():
            checkpoint = JSON / checkpoint_id
        elif os.path.exists(checkpoint_id):
            checkpoint = checkpoint_id
        else:
            raise FileExistsError(checkpoint_id)
        checkpoint_file = checkpoint / f"{self.list1}_{self.list2}.json"
        with open(checkpoint_file, 'r') as file:
            candidate_pairs = json.load(file)
            for uris in candidate_pairs.keys():
                uri1, uri2 = uris.split('|')
                entity1 = Entity(URIRef(uri1))
                entity2 = Entity(URIRef(uri2))
                if entity1 not in self._list1_indexes:
                    self._list1_indexes.append(entity1)
                if entity2 not in self._list2_indexes:
                    self._list2_indexes.append(entity2)
            for _ in range(len(self._list1_indexes)):
                line = []
                for _ in range(len(self._list2_indexes)):
                    line.append(None)
                self._mapping.append(line)

            for uris, scores in candidate_pairs.items():
                uri1, uri2 = uris.split('|')
                entity1 = Entity(URIRef(uri1))
                entity2 = Entity(URIRef(uri2))
                cp = CandidatePair(first = entity1,
                                   second = entity2)
                for score_name, score_value in scores.items():
                    cp.add_score(score_name = score_name,
                                 score = score_value)
                self._mapping[self._list1_indexes.index(entity1)][self._list2_indexes.index(entity2)] = cp


    def save_json(self,
                  execution_id: str):
        """
        Save candidate pairs in a json format to prevents saving
        all Candidate Pairs in the ontology (there might be billions of CP).
        Save the scores into a json file. For scores, unlike synonym sets,
        we do not save them in the ontology.

        Keyword arguments:
        execution_id -- the id of this execution (generated in merge.py)
        """
        # Empty latest directory
        LATEST.mkdir(parents = True, exist_ok = True)
        for latest in os.listdir(LATEST):
            if not execution_id in str(latest):
                os.rename(LATEST / latest, JSON / latest)
        directory = LATEST / execution_id
        directory.mkdir(parents = True, exist_ok = True)

        filename = f"{self._list1}_{self._list2}.json"
        path = directory / filename
        res = ""
        with open(str(path), 'w') as file:
            file.write("{\n")
            print("") # To not erase the previous printed line
            for i, cp in enumerate(self):#._candidate_pairs:
                print(f"\033[F\033[{0}G Saving candidate pairs to checkpoint:{i}/{len(CandidatePair.candidate_pairs)}")
                res += "\"" + str(cp.member1.uri) + '|' + str(cp.member2.uri) + "\":"
                res += "{"
                for score, value in cp.scores.items():
                    res += f"\"{score}\":{value}, "
                res = res[:-2]# remove last ", "
                res += "}"
                #for score, value in cp.scores.items():
                #    file.write('\t' + score + '\t' + str(value))
                res += ",\n"
            res = res[:-2] # remove last ",\n"
            res += "\n}"
            file.write(res)
        #with open(filename, 'w') as file:
        #    json.dump(data, file)


def _compute_scores(member1: Union[Entity, SynonymSet],
                    member2: Union[Entity, SynonymSet],
                    uri: str,
                    scores: List[Score]) -> List[float]:

    """
    Asynchronous method to compute all scores for one candidate pair.
    Useful for the non-discriminant scores: prevent looping multiple
    times on the mapping for each score.
    Use this for non-discriminant scores.

    Keyword arguments:
    member1 -- the candidate pair's first member
    member1 -- the candidate pair's second member
    uri -- the candidate pair's uri.
    scores -- the scores to compute
    """
    scores_values = []
    for score in scores:
        score_value = _compute_one_score(score, member1, member2)
        scores_values.append(score_value)
    return scores_values, uri


def _compute_one_score(score: Score,
                       member1: Union[Entity, SynonymSet],
                       member2: Union[Entity, SynonymSet]) -> float:
    """
    Asynchronous method to compute a score without removing the
    candidate pair from the mapping at all.
    Use this for non-discriminant scores.

    Keyword arguments:
    member1 -- the candidate pair's first member
    member1 -- the candidate pair's second member
    scores -- the score to compute
    """
    if not member1 or not member2:
        return
    graph = Graph()
    score_value = score.compute(graph,
                                member1,
                                member2)
    return score_value


if __name__ == "__main__":
    pass
