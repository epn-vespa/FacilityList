"""
Superclass for filters.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import abc

from data_mapper.tools.tool import Tool
from graph.entity import Entity

# Used to generate mapping strategies
FILTERING_FIELDS = ["launch_date", "start_date", "end_date", "latitude", "longitude", "type", "aperture"]

class Filter(Tool):
    """
    Superclass for filters.
    """
    
    NAME = "Generic filter (superclass)"


    @abc.abstractmethod
    def are_compatible(entity1: Entity,
                       entity2: Entity) -> bool:
        """
        Return True if two entities are compatible according to
        a certain filter.

        Abstract method, to be implemented in subclasses.

        Args:
            entity1: reference entity or synonym set
            entity2: compared entity or synonym set
        """
        raise NotImplementedError("This method should be overridden by subclasses.")