"""
Define the superclass Score.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import abc

from graph.entity import Entity

class Score(abc.ABC):


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