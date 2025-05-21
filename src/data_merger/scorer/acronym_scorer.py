"""
Compute the probability of any label or alt label of an entity
to be the acronym of any label or alt label of the other entity.

Author:
    Liza Fretel (liza.fretel@obsmp.fr)
"""

from typing import Union
from graph import Graph
from data_merger.entity import Entity
from data_merger.scorer.score import Score
from data_merger.synonym_set import SynonymSet

from utils.acronymous import proba_acronym_of
from utils.performances import timeall


class AcronymScorer(Score):

    # Name of the score computed by this class (as in score.py)
    NAME = "acronym_probability"


    @timeall
    def compute(graph: Graph,
                entity1: Union[Entity, SynonymSet],
                entity2: Union[Entity, SynonymSet]) -> float:
        """
        Compute a fuzzy score between the two entities' labels and alt labels.
        Return the highest match between any labels.

        Keyword arguments:
        graph -- the graph
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
                if len(label2) < len(label1):
                    score = proba_acronym_of(label2, label1)
                else:
                    score = proba_acronym_of(label1, label2)
                if score == 1:
                    return 1
                if score > highest_score:
                    highest_score = score
        if score < 0.5:
            return -1 # Do not penalize
        return highest_score