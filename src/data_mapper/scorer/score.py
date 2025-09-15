"""
Define the superclass Score.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""


from typing import Union
from graph import Graph
from data_mapper.entity import Entity
from data_mapper.synonym_set import SynonymSet


class Score():


    NAME = "Generic Score (superclass)"


    def compute(self,
                entity1: Union[Entity, SynonymSet],
                entity2: Union[Entity, SynonymSet]) -> float:
        """
        Return a score value between 0 and 1.
        This method is implemented in the different Scorers.
        """
        ...



    def __str__(self):
        return self.NAME