"""
CandidatePair class to store candidate pairs.

CandidatePairManager class can add candidate pairs to the graph
and has methods to keep track of the candidate pairs for each namespace
(one namespace corresponds to one facility list).

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from collections import defaultdict
from typing import List, Type, Union
import uuid
import hashlib

from rdflib import RDF, XSD, Literal, Node, URIRef
from tqdm import tqdm
from data_merger.graph import Graph
from data_merger.scorer.score import Score
from data_merger.scorer.scorer_lists import ScorerLists
from data_merger.synset import SynonymSet, SynonymSetManager
from data_updater.extractor.extractor import Extractor
from concurrent.futures import ThreadPoolExecutor, as_completed



class CandidatePair():
    pass


class CandidatePair():


    def __init__(self,
                 first: URIRef,
                 second: URIRef,
                 list1: str,
                 list2: str,
                 uri: str = ""):
        """
        Instantiate a CandidatePair object. When reading a graph
        with some CandidatePair, use the node argument to save the
        URIRef of the CandidatePair. It will be used later if the
        CandidatePair can be removed.

        Keyword arguments:
        first -- first member of the pair
        second -- second member of the pair
        node -- the node of the candidate pair in the graph if exists
        """
        self._member1 = first
        self._member2 = second
        self._list1 = list1
        self._list2 = list2
        self._scores = dict()

        if not uri:
            self._uri = str(uuid.uuid4())
        else:
            self._uri = uri


    @property
    def member1(self):
        return self._member1


    @property
    def member2(self):
        return self._member2


    @property
    def list1(self):
        return self._list1


    @property
    def list2(self):
        return self._list2


    @property
    def uri(self):
        return self._uri


    @property
    def scores(self):
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
               candidate_pair: CandidatePair):
        return (candidate_pair.member1 == self.member1 and candidate_pair.member2 == self.member2
                or candidate_pair.member1 == self.member2 and candidate_pair.member2 == self.member1)

    def __hash__(self):
        m1 = str(self.member1)
        m2 = str(self.member2)
        str_ver = m1 + '_' + m2 if m1 < m2 else m2 + '_' + m1
        return int(hashlib.sha256(str_ver.encode('utf-8')).hexdigest(), 16) % 10**8


class CandidatePairsManager():
    """
    Save the candidate pairs inside a dictionary of sets. Useful to manage
    candidate pairs by facility lists.
    """


    def __init__(self):
        # to get candidate pairs by namespace
        self._candidate_pairs_dict = defaultdict(lambda: defaultdict(list))
        # to loop over candidate pairs
        self._candidate_pairs = list()


    @property
    def candidate_pairs(self):
        return self._candidate_pairs


    @property
    def candidate_pairs_dict(self):
        return self._candidate_pairs_dict


    def add_candidate_pairs(self,
                            candidate_pairs: Union[CandidatePair,
                                                   List[CandidatePair]],
                            list1: str,
                            list2: str):
        """
        Add a candidate pair to the candidate pairs manager.

        Keyword arguments:
        candidate_pairs -- the candidate pair(s) to add
        list1 -- name of the 1st list (must be same as namespace of member1)
        list2 -- name of the 2nd list (must be same as namespace of member2)
        """
        if type(candidate_pairs) != list:
            candidate_pairs = [candidate_pairs]
        for candidate_pair in candidate_pairs:
            #self.candidate_pairs_dict[list1].add(candidate_pair)
            #self.candidate_pairs_dict[list2].add(candidate_pair)
            if candidate_pair not in self.candidate_pairs_dict[list1][candidate_pair.member1]:
                self.candidate_pairs_dict[list1][candidate_pair.member1].append(candidate_pair)
            if candidate_pair not in self.candidate_pairs_dict[list2][candidate_pair.member2]:
                self.candidate_pairs_dict[list2][candidate_pair.member2].append(candidate_pair)
            if candidate_pair not in self.candidate_pairs:
                self.candidate_pairs.append(candidate_pair)


    def remove_candidate_pair(self,
                              graph: Graph,
                              candidate_pair: CandidatePair,
                              list1: str,
                              list2: str):
        """
        Remove a candidate pair from the Candidate Pair manager
        and from the graph.

        Keyword arguments:
        graph -- the graph to remove the candidate pair from
        candidate_pair -- the candidate pair to remove
        list1 -- the list or namespace of the first pair's member
        list2 -- the list or namespace of the second pair's member
        """
        try:
            self.candidate_pairs_dict[list1][candidate_pair.member1].remove(candidate_pair)
        except ValueError:
            # print("candidate pair not saved in the dict:", candidate_pair)
            pass
        try:
            self.candidate_pairs_dict[list2][candidate_pair.member2].remove(candidate_pair)
        except ValueError:
            # print("candidate pair not saved in the dict:", candidate_pair)
            pass
        self.candidate_pairs.remove(candidate_pair)
        # Remove the candidate pair from the graph.
        graph.remove((graph.OBS[candidate_pair.uri], None, None))
        graph.remove((None, None, graph.OBS[candidate_pair.uri]))
        # Remove pairs that contain the entity and point to the other list.
        self._remove_all_pairs_with_entity(candidate_pair.member1, list1, list2)
        self._remove_all_pairs_with_entity(candidate_pair.member2, list1, list2)
        del(candidate_pair)


    def _remove_all_pairs_with_entity(self,
                                      entity: URIRef,
                                      list1: str,
                                      list2: str):
        """
        Use after an entity has been disambiguated and is in a synset.
        This will remove all candidate pairs containing the entity.

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


    def get_candidate_pairs(self,
                            list1: str,
                            list2: str = None) -> set:
        """
        Get the candidate pairs for a namespace or list or two namespaces or
        lists.

        Keword arguments:
        list1 -- the list or namespace
        list2 -- if set, get intersection of candidate pairs from both lists
        """
        candidate_pairs = self._candidate_pairs_dict[list1].values()
        if list2:
            candidate_pairs = candidate_pairs.intersection(
                self._candidate_pairs_dict[list2])
        # Only keep candidate pairs that still exist
        candidate_pairs = candidate_pairs.intersection(self.candidate_pairs)
        return candidate_pairs


    def get_candidate_pair(self,
                           entity1: URIRef,
                           entity2: URIRef,
                           list1: str,
                           list2: str,) -> CandidatePair:
        """
        Get a candidate pair object from the namespaces and entities.
        """
        if (list1 in self.candidate_pairs_dict.keys() and
            list2 in self.candidate_pairs_dict.keys()):
            candidate_pairs = self.candidate_pairs_dict[list1][list2]

            for pair in candidate_pairs:
                if (pair.member1.node == entity1 and pair.member2.node == entity2
                    or pair.member2.node == entity1 and pair.member1.node == entity2):
                    return pair
        # Absent from the Candidate Pair Manager.
        # We create a new candidate_pair, save it and return it.
        candidate_pair = CandidatePair(entity1, entity2,
                                       list1, list2)
        self.add_candidate_pairs(candidate_pair,
                                 list1,
                                 list2)
        return candidate_pair


    def save_all(self):
        """
        Save the candidate pairs.
        """
        graph = Graph._graph

        for candidate_pair in self.candidate_pairs:
            candidate_pair_uri = graph.OBS[candidate_pair.uri]

            # Add the candidate pair in the graph
            graph.add((candidate_pair_uri, RDF.type,
                       graph.OBS["CandidatePair"]))
            graph.add((candidate_pair_uri, graph.OBS["hasMember"],
                       candidate_pair.member1))
            graph.add((candidate_pair_uri, graph.OBS["hasMember"],
                       candidate_pair.member2))
            for score, value in candidate_pair.scores.items():
                graph.add((candidate_pair_uri,
                           graph.OBS[score],
                           Literal(value, datatype = XSD.float)))


    def disambiguate_only_candidates(self,
                                     graph: Graph,
                                     list1: Extractor,
                                     list2: Extractor,
                                     scores: List[Score] = None):
        """
        Disambiguate only entities that already have a candidate pair.
        This is useful for lists where we already have an idea of what to
        disambiguate (example: wikidata / naif with ambiguous identifiers).

        Keyword arguments:
        graph -- used to get the entities' informations to help disambiguate
        list1 -- the name of the first list
        list2 -- the name of the second list
        scores -- list of Score classes to use. If None, perform on all scores
        """
        # TODO for wikidata/naif


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
                print(len(entities_list_1), entities_list_1[0])
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
                for entity1, synset1 in entities_list_1: # No synset
                    print(len(entities_list_1))


    def _compute_score(self,
                       graph: Graph,
                       score: Score,
                       SSM: SynonymSetManager,
                       entity1: URIRef,
                       synset1: SynonymSet,
                       entities_list_1: List,
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
        for entity2, synset2 in entities_list_2:
            print(len(entities_list_2))
            score_value = score.compute(graph, entity1, entity2)
            candidate_pair = self.get_candidate_pair(entity1,
                                                     entity2,
                                                     list1.NAMESPACE,
                                                     list2.NAMESPACE)
            candidate_pair.add_score(score.NAME, score_value)
            # If the score is discriminant, we link the entities
            # in the graph and remove them from the list of entities
            # to discriminate.
            if ScorerLists.DISCRIMINATE[score](score_value):
                """
                if entity1 in entities_list_1:
                    print("INSIDE", entity1)
                    entities_list_1.remove(entity1)
                else:
                    print("OUTSIDE", entity1)"
                """
                # Do not loop again on those entities.
                entities_list_1.remove((entity1, synset1))
                entities_list_2.remove((entity2, synset2))

                SSM.add_synset(entity1, entity2) # SynSet Manager
                # Remove candidate pair from the manager & graph.
                # This has to be done every time we add a synset.
                self.remove_candidate_pair(graph, candidate_pair, list1, list2)
                break
            else:
                self.add_candidate_pairs(candidate_pair, list1, list2)


if __name__ == "__main__":
    pass