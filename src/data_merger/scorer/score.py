"""
Define the superclass Score.
"""


from typing import Union
from data_merger.entity import Entity
from data_merger.graph import Graph
from data_merger.synonym_set import SynonymSet


class Score():



    def compute(graph: Graph,
                entity1: Union[Entity, SynonymSet],
                entity2: Union[Entity, SynonymSet]) -> float:
        """
        Return a score value between 0 and 1.
        This method is implemented in the different Scorers.
        """
        ...