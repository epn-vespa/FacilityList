"""
Special score that is used for compatibility on classes
(entity types). For example, a Telescope can be a Ground Observatory,
but a Ground Observatory cannot be a Spacecraft.

The incompatibilities reflect the LLM's entities categorization results:
the confusion matrix proved that Missions are often mistaken for Spacecrafts,
Observatory Network for Ground Observatory, Telescopes are also unclear as
they might belong to a ground observatory or a spacecraft.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from typing import Union
from graph import entity_types
from graph.entity import Entity
from graph.synonym_set import SynonymSet
from data_mapper.filters.filter import Filter


class TypeIncompatibilityScorer(Filter):

    NAME = "type"

    def are_compatible(entity1: Union[Entity, SynonymSet],
                       entity2: Union[Entity, SynonymSet]):
        """
        If any type of the entities is identical, then return True.
        Also return True if the type_confidence of any of the entities
        is not 1.

        Args:
            entity1: reference entity
            entity2: compared entity
        """
        type1 = entity1.get_values_for("type")
        type2 = entity2.get_values_for("type")
        confidence1 = entity1.get_values_for("type_confidence", unique = True)
        confidence2 = entity2.get_values_for("type_confidence", unique = True)
        if confidence1 != 1 or confidence2 != 1:
            # The type was determined by a LLM, cannot disambiguate
            # on the type (not enough control)
            return True

        if type1.intersection(type2):
            return True
        else:
            return False