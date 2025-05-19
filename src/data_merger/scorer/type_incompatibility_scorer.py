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
from data_merger.entity import Entity
from data_merger.scorer.score import Score
from data_merger.synonym_set import SynonymSet
from data_updater import entity_types
from graph import Graph


class TypeIncompatibilityScorer(Score):

    NAME = "type"


    ground_types = {entity_types.GROUND_OBSERVATORY,
                    entity_types.OBSERVATORY_NETWORK}

    space_types = {entity_types.AIRBORNE,
                   entity_types.SPACECRAFT}

    ambiguous_types = {entity_types.MISSION,
                       entity_types.TELESCOPE,
                       entity_types.UFO}

    def compute(graph: Graph,
                entity1: Union[Entity, SynonymSet],
                entity2: Union[Entity, SynonymSet]):
        """
        If any type of one entity (an Entity or Synonym Set) is a ground type
        and the other one is a space type, then they are incompatible.

        If no incompatibility was found, return -1.
        If an incompatibility was found, return -2.

        Keyword arguments:
        graph -- the graph
        entity1 -- reference entity
        entity2 -- compared entity
        """
        type1 = entity1.get_values_for("type")
        type2 = entity2.get_values_for("type")
        confidence1 = entity1.get_values_for("type_confidence")
        confidence2 = entity2.get_values_for("type_confidence")
        if confidence1 != 1 or confidence2 != 1:
            # The type was determined by a LLM, cannot disambiguate
            # on the type (not enough control)
            return -1

        if type1.union(type2):
            return -1
        else:
            return -2
        """
        if any(t1 in TypeIncompatibilityScorer.ground_types for t1 in types1):
            if any(t2 in TypeIncompatibilityScorer.space_types for t2 in types2):
                return -2 # Ground / Space
        
        if any(t1 in TypeIncompatibilityScorer.space_types for t1 in types1):
            if any(t2 in TypeIncompatibilityScorer.ground_types for t2 in types2):
                return -2 # Space / Ground
        return -1
        """