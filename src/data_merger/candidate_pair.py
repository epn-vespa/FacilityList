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
from data_merger.graph import Graph
from data_merger.scorer.score import Score
from data_merger.scorer.scorer_lists import ScorerLists
from data_merger.synonym_set import SynonymSet, SynonymSetManager
from data_merger.entity import Entity
from data_updater.extractor.extractor import Extractor
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import Pool

from utils.performances import timeit



class CandidatePair():
    pass


class CandidatePair():
    def __init__(self,
                 first: Union[Entity, SynonymSet],
                 second: Union[Entity, SynonymSet],
                 uri: URIRef = None):
        """
        Instantiate a CandidatePair object. When reading a graph
        with some CandidatePair, use the node argument to save the
        URIRef of the CandidatePair. It will be used later if the
        CandidatePair can be removed.

        Keyword arguments:
        first -- first member of the pair
        second -- second member of the pair. Can be an entity or a synonym set.
        list1 -- list or namespace of the first member.
        list2 -- list or namespace of the second member.
        node -- the node of the candidate pair in the graph if exists
        """
        self._member1 = first
        self._member2 = second
        #self._list1 = list1
        #self._list2 = list2
        self._scores = dict()

        if not uri:
            self._uri = Graph._graph.OBS[str(uuid.uuid4())]
            # self._uri = str(uuid.uuid4())
        else:
            self._uri = uri

        self._list1 = str(first.uri).split('#')[0].split('/')[-1]
        self._list2 = str(second.uri).split('#')[0].split('/')[-1]


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


    def del_candidate_pairs(self,
                            entity: Union[Entity, SynonymSet]):
        """
        Remove all candidate pairs that contain an entity.

        Keyword arguments:
        entity -- the entity to remove pairs that contain it.
        """
        for pair in self.candidate_pairs:
            if (pair.member1 == entity or
                pair.member2 == entity):
                self.candidate_pairs.remove(pair)
        if entity in self.candidate_pairs_dict:
            del self.candidate_pairs_dict[entity]

    @timeit
    def save_all(self):
        """
        Save the remaining candidate pairs into the graph.
        This will add CandidatePair entities in the graph
        in case they were not all disambiguated.
        """
        graph = Graph._graph
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
    def disambiguate_candidates(self,
                                scores: List[Score] = None):
        """
        Disambiguate only entities that already have a candidate pair.
        This is useful for lists where we already have an idea of what to
        disambiguate (example: wikidata / naif with ambiguous identifiers).

        Keyword arguments:
        scores -- list of scores to perform. If None, perform on all scores.
        """
        for score in ScorerLists.DISCRIMINANT_SCORES:
            if score not in scores:
                continue
            for candidate_pair in tqdm(self.candidate_pairs.copy(),
                                       desc = f"Computing {score.NAME} on {self._list1}, {self._list2}"):
                # discriminant
                #self.compute(score = score,
                #             candidate_pair = candidate_pair)
                #"""
                with ThreadPoolExecutor() as executor:
                    executor.submit(self.compute,
                                    score = score,
                                    candidate_pair = candidate_pair)
               # """


    def compute(self,
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
        graph = Graph._graph
        score_value = score.compute(graph,
                                    candidate_pair.member1,
                                    candidate_pair.member2)
        candidate_pair.add_score(score.NAME, score_value)
        if ScorerLists.ELIMINATE.get(score, lambda x: False)(score_value):
            # self.candidate_pairs.remove(candidate_pair)
            self.del_candidate_pair(candidate_pair)
            return State.ELIMINATED
        elif ScorerLists.ADMIT.get(score, lambda x: False)(score_value):
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


    def __del__(self):
        del(self._mapping)
        del(self._list1_indexes)
        del(self._list2_indexes)


    def __len__(self):
        if not self._mapping:
            return 0
        return len(self._mapping) * len(self._mapping[0])


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
        for i in range(len(entities1)):
            self._mapping.append([None] * len(entities2))

        for i, (entity1, synset1) in enumerate(tqdm(entities1,
                                                    desc = f"Generating mapping for {self._list1}, {self._list2}")):
            if synset1 is not None:
                entity1 = SynonymSet(synset1)
            else:
                entity1 = Entity(entity1)
            self._list1_indexes.append(entity1)
            for j, (entity2, synset2) in enumerate(entities2):

                if synset2 is not None:
                    entity2 = SynonymSet(synset2)
                else:
                    entity2 = Entity(entity2)
                self._list2_indexes
                self._mapping[i][j] = CandidatePair(entity1, entity2)


    @timeit
    def old_generate_mapping(self,
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
        entities2 = graph.get_entities_from_list(self._list2,
                                                 no_equivalent_in = self._list1)
        for entity1, synset1 in tqdm(entities1,
                                     desc = f"Generating mapping for {self._list1}, {self._list2}"):
            if synset1 is not None:
                entity1 = SynonymSet(synset1)
            else:
                entity1 = Entity(entity1)
            mapping_list2 = []
            self._list1_indexes.append(entity1)
            for entity2, synset2 in entities2:
                if synset2 is not None:
                    entity2 = SynonymSet(synset2)
                else:
                    entity2 = Entity(entity2)
                self._list2_indexes.append(entity2)
                candidate_pair = CandidatePair(entity1, entity2)
                mapping_list2.append(candidate_pair)
            self._mapping.append(mapping_list2)


    def del_candidate_pairs(self,
                            entity: Union[Entity, SynonymSet]):
        """
        Remove all candidate pairs that contain an entity from the
        mapping and the CandidatePairManager.

        Keyword arguments:
        entity -- the entity to remove pairs.
        """
        if entity in self._list1_indexes:
            entity_index = self._list1_indexes.index(entity)
            del(self._mapping[entity_index])
            del(self._list1_indexes[entity_index])
        elif entity in self._list2_indexes:
            entity_index = self._list2_indexes.index(entity)
            for e1 in self._mapping:
                del(e1[entity_index])

        else:
            raise ValueError(f"{entity} not in mapping.")


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


    def compute(self,
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

        graph = Graph._graph
        score_value = score.compute(graph,
                                    candidate_pair.member1,
                                    candidate_pair.member2)

        candidate_pair.add_score(score.NAME, score_value)
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
        else:
            return State.UNCLEAR

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
            score_value = score.compute(Graph._graph,
                                        candidate_pair.member1,
                                        candidate_pair.member2)
            candidate_pair.add_score(score.NAME, score_value)


    def _compute_for_index(self,
                           index_score) -> None:
        """
        Used to compute a score on the nth candidate pair.
        """
        index, score = index_score
        i = index / len(self._mapping)
        j = index % len(self._mapping)
        candidate_pair = self._mapping[i][j]
        self.compute()


    @timeit
    def disambiguate(self,
                     scores: List[Score]):
        """
        Eliminate entities that are incompatible and select
        entities that are compatible.

        Keyword arguments:
        scores -- list of scores to perform. If None, perform on all scores.
        """
        print(f"Count of pairs to disambiguate: {len(self)}")# {len(self._mapping) * len(self._mapping[0])}")

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
                    state = self.compute(score = score,
                                         candidate_pair = candidate_pair)
                    if state == State.ADMITTED:
                        break
                if state == State.ADMITTED:
                    i += 0 # The mapping's size was reduced so
                    # if we increment i, it will jump over the next element.
                    print("admitted", len(self._mapping), i, candidate_pair.member1,  candidate_pair.member2)
                    # self._mapping[i][self._list2_indexes.index(candidate_pair.member2)]
                else:
                    i += 1
            """
            for candidate_pair_list in tqdm(self._mapping,
                                            desc = f"Computing {score.NAME} on {self._list1}, {self._list2}"):
                # discriminant
                for candidate_pair in candidate_pair_list:
                    state = self.compute(score = score,
                                         candidate_pair = candidate_pair)
                    if state == State.ADMITTED:
                        break
            """
            """
                with ThreadPoolExecutor() as executor:
                    {executor.submit(self.compute,
                                     score = score,
                                     candidate_pair = candidate_pair):
                                     candidate_pair for candidate_pair in candidate_pair_list}
            """
        for score in ScorerLists.OTHER_SCORES:
            if score not in scores:
                continue
            print(f"Computing {score.NAME} on {self._list1}, {self._list2} for {len(self)} candidate pairs.")

            """
            for candidate_pair_list in tqdm(self._mapping):
                for candidate_pair in candidate_pair_list:
                    self.compute(score = score,
                                 candidate_pair = candidate_pair)
                
            """
            ### TOO SLOW
            with ThreadPoolExecutor() as executor:
                futures = {executor.submit(self._compute_all,
                                           candidate_pair_list,
                                           score):
                                           candidate_pair_list for candidate_pair_list in self._mapping}
                for future in tqdm(as_completed(futures), total = len(futures)):
                    #data = future.result()
                    pass
                print(len(futures))

            """
            tasks = [(i, score) for i in range(0, len(self))]

            with Pool() as pool:
                # results = list(tqdm(pool.imap_unordered(self._compute_all, )))
                results = list(tqdm(pool.imap_unordered(self._compute_for_index,
                                                        tasks),
                                    total = len(self)))
            """
            """
            Example:
            futures = {executor.submit(self._extract_entity,
                                       wikidata_uri,
                                       result,
                                       True):
                    wikidata_uri for wikidata_uri in older}
            """


    def save_all(self):
        """
        Overrides save_all from CandidatePairsManager.
        We do not want to save a whole mapping into the Ontology."
        """

        graph = Graph._graph
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
                    print("add score for", score, value, candidate_pair.member1, candidate_pair.member2)
                    graph.add((candidate_pair_uri,
                               graph.OBS[score],
                               Literal(value, datatype = XSD.float)))


if __name__ == "__main__":
    pass