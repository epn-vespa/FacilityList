"""
Define the superclass Matcher.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import abc

from typing import Tuple, Any
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
                entity2: Entity) -> Tuple[str, str, Any]:
        """
        This method is implemented in the different Scorers.
        Return:
            If matched, the two fields that matched and the matched value.
            None otherwise.
        """
        raise NotImplementedError("This method should be overridden by subclasses.")


    def __str__(self):
        return self.NAME