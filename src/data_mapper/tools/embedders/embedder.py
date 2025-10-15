"""
Define the superclass Embedder.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import abc
import numpy as np

from typing import List
from graph.entity import Entity
from data_mapper.tools.tool import Tool

class Embedder(Tool):


    NAME = "Generic Embedder (superclass)"


    @abc.abstractmethod
    def compute(self,
                entities: List[Entity]) -> np.ndarray:
        """
        Compute the embedding of for a list of entities.
        """
        raise NotImplementedError("This method should be overridden by subclasses.")


    def __str__(self):
        return self.NAME