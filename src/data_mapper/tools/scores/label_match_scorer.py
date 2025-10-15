"""
True if labels are the same. Distinct from Fuzzy Levenshtein
because it is a discriminant score.

Author:
    Liza Fretel (liza.fretel@obsmp.fr)
"""

from unidecode import unidecode
from graph.entity import Entity
from data_mapper.tools.scores.score import Score
from utils.performances import timeall


class LabelMatchScorer(Score):

    # Name of the score computed by this class
    NAME = "label_match"


    @timeall
    def compute(entity1: Entity,
                entity2: Entity) -> float:
        """
        Return 1 if any of the labels match, else -1.
        -1 is a negative number so it will not influence the global score.

        Keyword arguments:
        entity1 -- reference entity
        entity2 -- compared entity
        """

        labels1 = entity1.get_values_for("label")
        labels1.update(entity1.get_values_for("alt_label"))
        labels2 = entity2.get_values_for("label")
        labels2.update(entity2.get_values_for("alt_label"))

        for label1 in labels1:
            if len(label1) < 5:
                # TNG can be Tangerang Geomagnetic Observatory & Telescopio Nazionale Galileo.
                # Need to ignore labels that are too short and thus likely to be the same
                # eventhough entities are distinct.
                continue
            for label2 in labels2:
                label1 = unidecode(label1).lower()
                label2 = unidecode(label2).lower()
                if label1 == label2:
                    return 1
        return -1