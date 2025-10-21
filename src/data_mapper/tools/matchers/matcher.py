"""
Define the superclass Matcher.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import abc

from graph.entity import Entity
from data_mapper.tools.tool import Tool

class Matcher(Tool):
    """
    A matcher merges two entities on criteria
    """

    WEIGHT = 1.0

    NAME = "Generic Matcher (superclass)"


    _instances = {}

    def __new__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__new__(cls)
            cls._instances[cls] = instance
        return cls._instances[cls]


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