"""
Define the superclass Score.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import abc

from graph.entity import Entity
from data_mapper.tools.tool import Tool

class Score(Tool):
    """
    A score is a similarity that can be computed between two entities individually.
    The compute method returns a value between 0 and 1 (similarity value).
    """

    WEIGHT = 1.0

    NAME = "Generic Score (superclass)"

    @abc.abstractmethod
    def compute(self,
                entity1: Entity,
                entity2: Entity) -> float:
        """
        Return a score value between 0 and 1.
        This method is implemented in the different Scorers.
        """
        raise NotImplementedError("This method should be overridden by subclasses.")


    def __str__(self):
        return self.NAME