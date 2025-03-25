"""
CandidatePair class to store candidate pairs.

CandidatePairManager class can add candidate pairs to the graph
and has methods to keep track of the candidate pairs for each namespace
(one namespace corresponds to one facility list).

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from collections import defaultdict
from typing import List, Union
import uuid
import hashlib

from rdflib import RDF, URIRef
from data_merger.graph import Graph

class CandidatePair:
    pass

class CandidatePair():


    def __init__(self,
                 first: URIRef,
                 second: URIRef):
        self._member1 = first
        self._member2 = second


    @property
    def member1(self):
        return self._member1


    @property
    def member2(self):
        return self._member2


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
        self._candidate_pairs_dict = defaultdict(set)
        # to loop over candidate pairs
        self._candidate_pairs = list()


    @property
    def candidate_pairs(self):
        return self._candidate_pairs
    

    @property
    def candidate_pairs_dict(self):
        return self._candidate_pairs_dict


    def add_candidate_pairs(self,
                           candidate_pairs: Union[CandidatePair, List[CandidatePair]],
                           list1: str,
                           list2: str):
        """
        Add a candidate pair to the candidate pairs manager.

        Keyword arguments:
        candidate_pair -- the candidate pair or list of candidate pairs to add
        list1 -- the name of the first list or namespace
        list2 -- the name of the second list or namespace
        """
        if type(candidate_pairs) != list:
            candidate_pairs = [candidate_pairs]
        for candidate_pair in candidate_pairs:
            self.candidate_pairs_dict[list1].add(candidate_pair)
            self.candidate_pairs_dict[list2].add(candidate_pair)
            self.candidate_pairs.append(candidate_pair)


    def get_candidate_pairs(self,
                            list1: str,
                            list2: str = None) -> set:
        """
        Get the candidate pairs for a namespace or list

        Keword arguments:
        list1 -- the list or namespace
        list2 -- if set, get intersection of candidate pairs from both lists
        """
        candidate_pairs = self._candidate_pairs_dict[list1]
        if list2:
            candidate_pairs = candidate_pairs.intersection(
                self._candidate_pairs_dict[list2])
        return candidate_pairs

    def save_all(self):
        graph = Graph._graph

        for candidate_pair in self.candidate_pairs:
            unique_id = str(uuid.uuid4())
            candidate_pair_uri = URIRef(graph.OBS[unique_id])
            graph.add((candidate_pair_uri, RDF.type,
                       graph.OBS["CandidatePair"]))
            graph.add((candidate_pair_uri, graph.OBS["hasMember"],
                       candidate_pair.member1))
            graph.add((candidate_pair_uri, graph.OBS["hasMember"],
                       candidate_pair.member2))


if __name__ == "__main__":
    pass