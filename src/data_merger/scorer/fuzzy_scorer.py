"""
Compute fuzzy scores between two entities. Take the main label
and alt label into consideration. If there is a perfect match,
return 1. Fuzzy score computes a similarity between two strings,
or an edit-distance. We use the Levenshtein distance here.

Author:
    Liza Fretel (liza.fretel@obsmp.fr)
"""


from typing import Union
from fuzzywuzzy import fuzz

from data_merger.entity import Entity
from data_merger.graph import Graph
from data_merger.scorer.score import Score
from data_merger.synonym_set import SynonymSet
from utils.utils import remove_punct


class FuzzyScorer(Score):

    # Name of the score computed by this class (as in score.py)
    NAME = "fuzzy_levenshtein"


    def compute(graph: Graph,
                entity1: Entity,
                entity2: Union[Entity, SynonymSet]) -> float:
        """
        Compute a fuzzy score between the two entities' labels and alt labels.
        Return the highest match between any labels.

        Keyword arguments:
        entity1 -- reference entity
        entity2 -- compared entity
        """
        highest_score = 0

        labels1 = entity1.get_values_for("alt_label")
        labels1.update(entity1.get_values_for("label"))
        labels2 = entity2.get_values_for("alt_label")
        labels2.update(entity2.get_values_for("label"))
        for label1 in labels1:
            for label2 in labels2:
                # partial: ignore punctuation?
                # token sort: different word order does not impact
                # ignore case (lower())
                score = fuzz.token_sort_ratio(remove_punct(label1.lower()),
                                              remove_punct(label2.lower()))
                if score > highest_score:
                    highest_score = score
        return highest_score / 100