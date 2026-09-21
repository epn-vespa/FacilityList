"""
A filter that detects numerical suffixes and
compares them together to prohibit Pioneer-Pioneer 10 mappings.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
from data_mapper.tools.filters.filter import Filter
from graph.entity import Entity
from utils.string_utilities import get_suffix_number
from utils.performances import timeall

class NumberFilter(Filter):

    NAME = "number"

    @timeall
    def are_compatible(self,
                       entity1: Entity,
                       entity2: Entity) -> bool:
        """
        Make sure that numbers in entity1's labels
        are in entity2's labels (ex: Pioneer VS Pioneer-10)
        and vice-versa.

        Args:
            entity1: reference entity
            entity2: compared entity
        """
        labels1 = entity1.get_values_for("label")
        labels2 = entity2.get_values_for("label")
        labels1.update(entity1.get_values_for("alt_label"))
        labels2.update(entity2.get_values_for("alt_label"))

        numbers1 = set()

        for label in labels1:
            n = get_suffix_number(label)
            if n:
                numbers1.add(n)

        for label in labels2:
            n = get_suffix_number(label)
            if n and n in numbers1:
                return True

        return not numbers1