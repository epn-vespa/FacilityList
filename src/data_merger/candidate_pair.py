"""
CandidatePair class to store candidate pairs.

CandidatePairManager class can add candidate pairs to the graph
and has methods to keep track of the candidate pairs for each namespace
(one namespace corresponds to one facility list).

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from collections import defaultdict
from typing import Dict, List, Union
import uuid
import hashlib

from rdflib import RDF, XSD, Literal, URIRef
from data_merger.graph import Graph
from data_merger.scorer.score import Score
from data_merger.scorer.scorer_lists import ScorerLists
from data_merger.synonym_set import SynonymSet, SynonymSetManager
from data_merger.entity import Entity
from data_updater.extractor.extractor import Extractor
from concurrent.futures import ThreadPoolExecutor, as_completed



class CandidatePair():
    pass


class CandidatePair():
    def __init__(self,
                 first: Entity,
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
    def member1(self) -> Entity:
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


class CandidatePairsManager():
    """
    Save the candidate pairs inside a dictionary of sets. Useful to manage
    candidate pairs by facility lists. Every mapping of two lists should have
    its own CandidatePairsManager.
    """


    def __init__(self,
                 list1: str,
                 list2: str):
        # to get candidate pairs by entity
        self._candidate_pairs_dict = defaultdict(list)
        # to loop over candidate pairs
        self._candidate_pairs = list()
        self._list1 = list1
        self._list2 = list2


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
        Add a candidate pair to the candidate pairs manager.

        Keyword arguments:
        candidate_pairs -- the candidate pair(s) to add
        """
        if type(candidate_pairs) != list:
            candidate_pairs = [candidate_pairs]
        for candidate_pair in candidate_pairs:
            #self.candidate_pairs_dict[list1].add(candidate_pair)
            #self.candidate_pairs_dict[list2].add(candidate_pair)
            if candidate_pair not in self.candidate_pairs_dict[candidate_pair.member1]:
                self.candidate_pairs_dict[candidate_pair.member1].append(candidate_pair)
            if candidate_pair not in self.candidate_pairs_dict[candidate_pair.member2]:
                self.candidate_pairs_dict[candidate_pair.member2].append(candidate_pair)
            if candidate_pair not in self.candidate_pairs:
                self.candidate_pairs.append(candidate_pair)


    def remove_candidate_pair(self,
                              candidate_pair: CandidatePair):
        """
        Remove a candidate pair from the Candidate Pair manager
        and from the graph.

        Use after an entity has been disambiguated and is in a synset.
        This will remove all candidate pairs containing the entity
        that are between list1 & list2.

        Keyword arguments:
        graph -- the graph to remove the candidate pair from
        candidate_pair -- the candidate pair to remove
        """
        graph = Graph._graph
        self.candidate_pairs.remove(candidate_pair)
        # Remove the candidate pair from the graph.
        graph.remove((graph.OBS[candidate_pair.uri], None, None))
        graph.remove((None, None, graph.OBS[candidate_pair.uri]))

        # Remove pairs that contain the entity and point to the other list.
        del(candidate_pair)

        # Remove candidate pairs from the graph
        Graph._graph.remove((candidate_pair.node, None, None))
        Graph._graph.remove((None, None, candidate_pair.node))


    @DeprecationWarning
    def remove_all_pairs_with_entity(self,
                                     entity: Entity,
                                     list1: str,
                                     list2: str):
        """
        Use after an entity has been disambiguated and is in a synset.
        This will remove all candidate pairs containing the entity
        that are between list1 & list2.

        Keyword arguments:
        entity -- disambiguated entity to remove from candidate pair manager
        """
        for candidate_pair in self.candidate_pairs:
            if (entity == candidate_pair.member1 and list2 == candidate_pair.list2 or
                entity == candidate_pair.member2 and list1 == candidate_pair.list1):
                # If the pair is between the first member and second list or
                # between the second member and first list, then it should be removed.
                self.candidate_pairs.remove(candidate_pair)
                # Remove the candidate pair from the graph.
                Graph._graph.remove((candidate_pair.node, None, None))
                Graph._graph.remove((None, None, candidate_pair.node))
                del(candidate_pair)
                return # Only one candidate pair is allowed


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
                           entity1: Entity,
                           entity2: Entity) -> CandidatePair:
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


    def del_candidate_pairs(self,
                            entity: Union[Entity, SynonymSet]):
        """
        Remove all candidate pairs that contain an entity.

        Keyword arguments:
        entity -- the entity to remove pairs.
        """
        for pair in self.candidate_pairs:
            if (pair.member1 == entity or
                pair.member2 == entity):
                self.candidate_pairs.remove(pair)
        if entity in self.candidate_pairs_dict:
            del self.candidate_pairs_dict[entity]


    def save_all(self):
        """
        Save the remaining candidate pairs into the graph.
        This will add CandidatePair entities in the graph
        in case they were not all disambiguated.
        """
        graph = Graph._graph

        for candidate_pair in self.candidate_pairs:
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


    def disambiguate_candidates(self,
                                SSM: SynonymSetManager,
                                scores: List[Score] = None):
        """
        Disambiguate only entities that already have a candidate pair.
        This is useful for lists where we already have an idea of what to
        disambiguate (example: wikidata / naif with ambiguous identifiers).

        Keyword arguments:
        SSM - the Synonym Set Manager used to save synonym sets
        scores -- list of scores to perform. If None, perform on all scores.
        """
        for score in ScorerLists.DISCRIMINANT_SCORES:
            if score not in scores:
                continue
            print(f"Computing {score.NAME}")
            for candidate_pair in self.candidate_pairs.copy():
                # discriminant
                self.compute(score = score,
                             ssm = SSM,
                             candidate_pair = candidate_pair)
                """
                with ThreadPoolExecutor() as executor:
                    futures = {executor.submit(self.compute,
                                               score,
                                               SSM,
                                               candidate_pair)}
                """


    def compute(self,
                score: Score,
                ssm: SynonymSetManager,
                candidate_pair: CandidatePair):
        """
        Compute the score for a candidate pair. Add the candidate pair
        in a synset if the score was discriminant and remove the
        candidate pair from the candidate pairs list.
        """
        graph = Graph._graph
        score_value = score.compute(graph,
                                    candidate_pair.member1,
                                    candidate_pair.member2)
        candidate_pair.add_score(score.NAME, score_value)
        if ScorerLists.ELIMINATE.get(score, lambda x: False)(score_value):
            self.candidate_pairs.remove(candidate_pair)
        elif ScorerLists.ADMIT.get(score, lambda x: False)(score_value):
            # Remove from candidate pairs
            self.candidate_pairs.remove(candidate_pair)

            # Remove the candidate pairs that contain member1 & member2
            # (they do not need to be mapped anymore)
            self.del_candidate_pairs(candidate_pair.member1)
            self.del_candidate_pairs(candidate_pair.member2)

            # Add it to the graph
            ssm.add_synset(candidate_pair.member1,
                           candidate_pair.member2)
        else:
            # The candidate pair needs to be re-processed with other scores
            pass

    @DeprecationWarning
    def disambiguate(self,
                     graph: Graph,
                     SSM: SynonymSetManager,
                     list1: Extractor,
                     list2: Extractor,
                     scores: List[Score] = None):
        """
        Disambiguate all entities between two lists.
        Complexity is N*(M-N/2) / 4 if M is a longer list than N and N is included in M.
        This method will only compute the scores that are
        mentioned. If all, it will select scores that can be mentioned
        for each case (example, if both entities have a location, it will
        compute a location distance score). First start with
        scores that are discriminant.

        Keyword arguments:
        graph -- used to get the entities' informations to help disambiguate
        list1 -- the name of the first list
        list2 -- the name of the second list
        scores -- list of Score classes to use. If None, perform on all scores
        """
        if scores is None:
            scores = (ScorerLists.DISCRIMINANT_SCORES
                      + ScorerLists.AVAILABLE_SCORES)

        entities_list_1 = graph.get_entities_from_list(
            list1, no_equivalent_in = list2)

        entities_list_2 = graph.get_entities_from_list(
            list2, no_equivalent_in = list1)

        # Prevent looping for all the longer list
        if len(entities_list_1) > len(entities_list_2):
            entities_list_1, entities_list_2 = entities_list_2, entities_list_1

        # First compute discriminant scores.
        for score in ScorerLists.DISCRIMINANT_SCORES:
            if score in scores:
                # perform score computation & discriminate.

                # /!\ only for those that do not have a synSet /equivalentClass with
                # the other list yet !

                # create graph's table
                print(f"Computing {score.NAME} between {list1.URI} and {list2.URI}")
                with ThreadPoolExecutor() as executor:
                    futures = {executor.submit(self._compute_score(graph,
                                                                   score,
                                                                   SSM,
                                                                   entity1,
                                                                   synset1,
                                                                   entities_list_1,
                                                                   entities_list_2,
                                                                   list1,
                                                                   list2)):
                        (entity1, synset1) for entity1, synset1 in entities_list_1}
                    for futures in as_completed(futures):
                        pass # TODO no need to use as_completed

    @DeprecationWarning
    def _compute_score(self,
                       graph: Graph,
                       score: Score,
                       SSM: SynonymSetManager,
                       entity1: Entity,
                       synset1: SynonymSet,
                       entities_list_1: List[Entity],
                       entities_list_2: List,
                       list1: Extractor,
                       list2: Extractor):
        """
        Method to compute a score in a thread between
        one entity of the first list and all of the entities
        in the other list.

        Keyword arguments:
        graph -- to add triples
        """

        counter = 0
        entity1 = Entity(entity1)
        # synset1 & synset2 are only used to verify if they exist in the list.
        for entity2, synset2 in entities_list_2:
            counter += 1
            print(counter, "/", len(entities_list_2))
            entity2 = Entity(entity2)
            score_value = score.compute(graph, entity1, entity2)
            candidate_pair = self.get_candidate_pair(entity1,
                                                     entity2)
            candidate_pair.add_score(score.NAME, score_value)
            # If the score is discriminant, we link the entities
            # in the graph and remove them from the list of entities
            # to discriminate.
            if (score in ScorerLists.ELIMINATE and
                ScorerLists.ELIMINATE[score](score_value)):
                # remove the CandidatePair itself
                self.remove_candidate_pair(graph, candidate_pair)
            if (score in ScorerLists.ADMIT and
                ScorerLists.ADMIT[score](score_value)):
                # Do not loop again on those entities.
                if (entity1, synset1) in entities_list_1:
                    entities_list_1.remove((entity1, synset1))
                if (entity2, synset2) in entities_list_2:
                    entities_list_2.remove((entity2, synset2))

                SSM.add_synset(entity1, entity2) # SynSet Manager
                # Remove candidate pair from the manager & graph.
                # This has to be done every time we add a synset.
                # remove all other candidate pairs with the members that are linked
                self.remove_all_pairs_with_entity(candidate_pair.member1)
                self.remove_all_pairs_with_entity(candidate_pair.member2)
                break
            else:
                self.add_candidate_pairs(candidate_pair)


if __name__ == "__main__":
    pass