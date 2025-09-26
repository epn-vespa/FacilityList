"""
Superclass for filters.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import abc
from typing import Union
from graph.entity import Entity
from graph.synonym_set import SynonymSet

class Filter(abc.ABC):
    """
    Superclass for filters.
    """
    
    NAME = "filter"

    @abc.abstractmethod

    def are_compatible(entity1: Union[Entity, SynonymSet],
                       entity2: Union[Entity, SynonymSet]) -> bool:
        """
        Return True if two entities are compatible according to
        a certain filter.

        Abstract method, to be implemented in subclasses.

        Args:
            entity1: reference entity or synonym set
            entity2: compared entity or synonym set
        """
        raise NotImplementedError("This method should be implemented in subclasses.")