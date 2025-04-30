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
from typing import Dict, List, Union
import uuid
import hashlib

from rdflib import RDF, XSD, Literal, URIRef
from tqdm import tqdm
from graph import Graph
from data_merger.scorer.score import Score
from data_merger.scorer.scorer_lists import ScorerLists
from data_merger.synonym_set import SynonymSet, SynonymSetManager
from data_merger.entity import Entity
from data_updater.extractor.extractor import Extractor
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

from config import DATA_DIR
from utils.performances import deprecated, timeit


JSON = DATA_DIR / 'checkpoint'# "../../cache/error.log"
JSON.mkdir(parents = True, exist_ok = True)


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


    def __eq__(self,
               candidate_pair: CandidatePair) -> bool:
        return (candidate_pair.member1 == self.member1 and candidate_pair.member2 == self.member2
                or candidate_pair.member1 == self.member2 and candidate_pair.member2 == self.member1)


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
        directory = JSON / execution_id
        directory.mkdir(parents = True, exist_ok = True)

        filename = f"{self._list1}_{self._list2}.json"
        path = directory / filename
        with open(str(path), 'w') as file:
            file.write("{\n")
            for cp in self._candidate_pairs:# ._candidate_pairs:
                file.write("\"" + str(cp.member1.uri) + '\t' + str(cp.member2.uri) + "\":")
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
                 list2: Extractor):
        self._list1 = list1
        self._list2 = list2

        self._mapping = [] # 2D list to represent the mapping (graph)
        # lines: list1,
        # columns: list2
        self._list1_indexes = [] # indexes by entity in the mapping
        self._list2_indexes = [] # indexes by entity in the mapping

        # self._candidate_pairs = []


    def __len__(self):
        if not self._mapping:
            return 0
        # return len(self._candidate_pairs)
        return len(self._mapping) * len(self._mapping[0])


    def __iter__(self):
        self.row_index = len(self._mapping) - 1
        if len(self._mapping) == 0:
            return self
        self.col_index = len(self._mapping[0]) - 1
        return self


    def _increment(self):
        """
        Increment from bottom to top & from left to write.
        """
        self.col_index -= 1
        if self.col_index < 0:
            self.row_index -= 1
            self.col_index = len(self._mapping[0]) - 1


    def __next__(self):
        if self.row_index < 0:
            raise StopIteration
        if len(self._mapping) == 0 or len(self._mapping[0]) == 0:
            raise StopIteration
        current_element = None
        while current_element is None:
            current_element = self._mapping[self.row_index][self.col_index]
            self._increment()
            if (current_element is None and
                self.row_index < 0):
                raise StopIteration
        return current_element


    @timeit
    def generate_mapping(self,
                         graph: Graph):
        """
        Generate candidate pairs between both lists. Only
        generate candidate pairs for entities that are not linked
        to each other's list already.
        [[CandidatePair1.1, CandidatePair1.2],
         [CandidatePair2.1, CandidatePair2.2]]

        TODO create a relation differentFrom for each relation
        that is eliminated, in order to prevent multiple mapping ?
        /!\ OR create a relation "noEquivalentInList source_list" (easier)
        """
        if self._mapping:
            # do not generate twice
            return
        entities1 = graph.get_entities_from_list(self._list1,
                                                 no_equivalent_in = self._list2)
        entities1 = list(entities1)

        entities2 = graph.get_entities_from_list(self._list2,
                                                 no_equivalent_in = self._list1)
        entities2 = list(entities2)
        self._mapping = []

        # Initialize the 2D array
        for i in range(len(entities1)):
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
                # self._candidate_pairs.append(cp)


    def del_candidate_pairs(self,
                            entity: Union[Entity, SynonymSet]):
        """
        Remove all candidate pairs that contain an entity from the
        mapping and the CandidatePairManager.

        Keyword arguments:
        entity -- remove all pairs with this entity.
        """
        if entity in self._list1_indexes:
            entity_index = self._list1_indexes.index(entity)
            del self._mapping[entity_index]
            del self._list1_indexes[entity_index]
        elif entity in self._list2_indexes:
            entity_index = self._list2_indexes.index(entity)
            for e1 in self._mapping:
                del e1[entity_index]
            del self._list2_indexes[entity_index]
        else:
            pass # it can be removed twice when removing a line and a column
            # raise ValueError(f"{entity} not in mapping.")


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

        # Remove from the list too
        # del(self._candidate_pairs[index1 * len(self._mapping) + index2])

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
        with open("out", "w") as f:
            f.write(candidate_pair)


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


    # @timeit
    def disambiguate(self,
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
        self._compute_cuda_scores(cuda_scores)
        self._disambiguate_discriminant(discriminant_scores)
        self._compute_other_scores(other_scores)


    @timeit
    def _disambiguate_discriminant(self,
                                   scores: List[Score]):
        """
        Disambiguate discriminant scores. This will remove
        the candidate pairs for which we are sure that they are not
        compatible or they refer to the same entity.
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
                             scores: List[Score]):
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
            print(f"Computing {score.NAME} for {self._list1}, {self._list2}" +
                  f" on {len_e1 * len_e2} entities.")
            scores = list(score.compute(Graph(), self._list1_indexes, self._list2_indexes))
            for n, score_value in enumerate(scores):
                i = n % len_e1
                j = int((n - i) / len_e1)
                if self._mapping[i][j] is not None:
                    self._mapping[i][j].add_score(score.NAME, score_value)


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
            for candidate_pair in self:
                score_values = _compute_scores(candidate_pair, scores)
                for score, score_value in zip(scores, score_values):
                    candidate_pair.add_score(score.NAME, score_values)
            return
        """
        with ProcessPoolExecutor() as executor:
            print(f"Computing other scores for the remaining candidate pairs.")
            futures = [executor.submit(_compute_scores,
                                       candidate_pair,
                                       scores) for candidate_pair in self]#._candidate_pairs]

            for i, future in tqdm(enumerate(as_completed(futures)), total = len(futures)):
                score_values, candidate_pair_uri = future.result()
                for score, score_value in zip(scores, score_values):
                        # Make sure that we add it in the right CandidatePair.
                        CandidatePair.candidate_pairs[candidate_pair_uri].add_score(score.NAME, score_value)


    @timeit
    @deprecated
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

        directory = JSON / execution_id
        directory.mkdir(parents = True, exist_ok = True)
        filename = f"{self._list1}_{self._list2}.json"
        path = directory / filename
        with open(str(path), 'w') as file:
            file.write("{\n")
            for cp in self:#._candidate_pairs:
                file.write("\"" + str(cp.member1.uri) + '|' + str(cp.member2.uri) + "\":")
                file.write(str(cp.scores))
                #for score, value in cp.scores.items():
                #    file.write('\t' + score + '\t' + str(value))
                file.write(',\n')

            file.write("\n}")
        #with open(filename, 'w') as file:
        #    json.dump(data, file)


def _compute_scores(candidate_pair: CandidatePair,
                    scores: List[Score]) -> List[float]:

    """
    Asynchronous method to compute all scores for one candidate pair.
    Useful for the non-discriminant scores: prevent looping multiple
    times on the mapping for each score.
    Use this for non-discriminant scores.

    Keyword arguments:
    candidate_pair -- the candidate pair
    scores -- the scores to compute
    """
    scores_values = []
    for score in scores:
        score_value = _compute_one_score(score, candidate_pair)
        scores_values.append(score_value)
    return scores_values, candidate_pair.uri


def _compute_one_score(score: Score,
                       candidate_pair: CandidatePair) -> float:
    """
    Asynchronous method to compute a score without removing the
    candidate pair from the mapping at all.
    Use this for non-discriminant scores.

    Keyword arguments:
    candidate_pair -- the candidate pair
    scores -- the score to compute
    """
    if candidate_pair is None:
        return
    graph = Graph()
    score_value = score.compute(graph,
                                candidate_pair.member1,
                                candidate_pair.member2)
    return score_value


if __name__ == "__main__":
    pass
