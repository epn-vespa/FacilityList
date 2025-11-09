"""
True if labels are the same. Distinct from Fuzzy Levenshtein
because it is a discriminant score.

Author:
    Liza Fretel (liza.fretel@obsmp.fr)
"""

from typing import Tuple, Any
from unidecode import unidecode
from graph.entity import Entity
from data_mapper.tools.matchers.matcher import Matcher
from utils.performances import timeall


class LabelMatcher(Matcher):

    # Name of the score computed by this class
    NAME = "label_match"


    @timeall
    def compute(self,
                entity1: Entity,
                entity2: Entity) -> Tuple[str, str, Any]:
        """
        Return True if any of the labels match. Labels that are shorter than
        5 characters are not taken into account.

        Args:
            entity1: reference entity
            entity2: compared entity
        """

        labels1 = entity1.get_values_for("label")
        alt_labels1 = entity1.get_values_for("alt_label")
        alt_labels1.update(labels1)
        labels2 = entity2.get_values_for("label")
        alt_labels2 = entity2.get_values_for("alt_label")
        alt_labels2.update(labels2)

        for label1 in alt_labels1:
            if len(label1) < 5:
                # TNG can be Tangerang Geomagnetic Observatory & Telescopio Nazionale Galileo.
                # Need to ignore labels that are too short and thus likely to be the same
                # eventhough entities are distinct.
                continue
            for label2 in alt_labels2:
                label1_l = unidecode(label1).lower()
                label2_l = unidecode(label2).lower()
                if label1_l == label2_l:
                    field1 = "label" if label1 in labels1 else "alt_label"
                    field2 = "label" if label2 in labels2 else "alt_label"
                    return field1, field2, label1
        return None, None, None