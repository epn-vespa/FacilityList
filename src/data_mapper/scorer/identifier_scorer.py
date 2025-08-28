"""
Verifies that two entities do not have a different
COSPAR ID, NSSDCA ID or NAIF ID. Return -2 if they are
incompatible, -1 if no incompatibility were found.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""



from typing import Union
from data_mapper.entity import Entity
from data_mapper.scorer.score import Score
from data_mapper.synonym_set import SynonymSet
from utils.performances import timeall


class IdentifierScorer(Score):

    # Name of the score computed by this class (as in score.py)
    NAME = "identifier"

    @timeall
    def compute(entity1: Union[Entity, SynonymSet],
                entity2: Union[Entity, SynonymSet]) -> float:
        """
        Check if any of the entity's identifiers are different.

        If no incompatibility was found, return -1.
        If an incompatibility was found, return -2.

        Keyword arguments:
        entity1 -- reference entity
        entity2 -- compared entity
        """
        identifiers = [["COSPAR_ID", "NSSDCA_ID"], "NAIF_ID"]

        for attr in identifiers:
            if IdentifierScorer._compare_entity_identifiers(entity1,
                                                            entity2,
                                                            attr):
                return -2 # Return -2 if any incompatibility is found
        return -1 # Return -1 if no incompatibility


    @staticmethod
    def _compare_entity_identifiers(entity1: Union[Entity, SynonymSet],
                                    entity2: Union[Entity, SynonymSet],
                                    attrs: Union[str, list[str]]) -> bool:
        """
        Compare an identifier field in both entities.
        Return True if identifiers are compatible, False if not.
        For example, if entity1 has the identifier but not entity2, they
        are compatible.

        Keyword arguments:
        entity1 -- reference entity
        entity2 -- compared entity
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
