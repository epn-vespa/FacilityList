"""
Verifies that two entities do not have a different
COSPAR ID, NSSDCA ID or NAIF ID. Return -2 if they are
incompatible, -1 if no incompatibility were found.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""



from typing import Union
from graph.entity import Entity
from data_mapper.filters.filter import Filter
from graph.synonym_set import SynonymSet
from utils.performances import timeall


class IdentifierFilter(Filter):

    NAME = "identifier"

    @timeall
    def are_compatible(entity1: Union[Entity, SynonymSet],
                       entity2: Union[Entity, SynonymSet]) -> bool:
        """
        Check if any of the entity's identifiers are different.

        If no incompatibility was found, return True.
        If an incompatibility was found, return False

        Args:
            entity1: reference entity
            entity2: compared entity
        """
        identifiers = [["COSPAR_ID", "NSSDCA_ID"], "NAIF_ID"]

        for attr in identifiers:
            if IdentifierFilter._compare_entity_identifiers(
                entity1,
                entity2,
                attr):
                return False
        return True


    @staticmethod
    def _compare_entity_identifiers(entity1: Union[Entity, SynonymSet],
                                    entity2: Union[Entity, SynonymSet],
                                    attrs: Union[str, list[str]]) -> bool:
        """
        Compare an identifier field in both entities.
        Return True if identifiers are compatible, False if not.
        For example, if entity1 has the identifier but not entity2, they
        are compatible.

        Args:
            entity1: reference entity
            entity2: compared entity
        """
        if type(attrs) == str:
            attrs = [attrs]
        identifiers1 = set()
        identifiers2 = set()
        for attr in attrs:
            identifiers1.update(entity1.get_values_for(attr))
            identifiers2.update(entity2.get_values_for(attr))
        if not identifiers1 or not identifiers2:
            return True
        return not identifiers1.isdisjoint(identifiers2)
