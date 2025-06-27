"""
Define the superclass Score.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""


from typing import Union
from graph import Graph
from data_merger.entity import Entity
from data_merger.synonym_set import SynonymSet


class Score():


    NAME = "Generic Score (superclass)"


    def compute(graph: Graph,
                entity1: Union[Entity, SynonymSet],
                entity2: Union[Entity, SynonymSet]) -> float:
        """
        Return a score value between 0 and 1.
        This method is implemented in the different Scorers.
        """
        ...



    def __str__(self):
        return self.NAME