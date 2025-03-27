"""
Compute fuzzy scores between two entities. Take the main label
and alt label into consideration. If there is a perfect match,
return 1. Fuzzy score computes a similarity between two strings,
or an edit-distance. We use the Levenshtein distance here.

Author:
    Liza Fretel (liza.fretel@obsmp.fr)
"""


from rdflib import URIRef
from fuzzywuzzy import fuzz

from data_merger.graph import Graph
from data_merger.scorer.score import Score


class FuzzyScorer(Score):

    # Name of the score computed by this class (as in score.py)
    NAME = "fuzzy_levenshtein"


    def compute(graph: Graph,
                entity1: URIRef,
                entity2: URIRef) -> float:
        """
        Compute a fuzzy score between the two entities' labels and alt labels.
        Return the highest match between any labels.

        Keyword arguments:
        entity1 -- reference entity
        entity2 -- compared entity
        """
        highest_score = 0

        labels1 = graph.get_labels_and_alt_labels(entity1)
        labels2 = graph.get_labels_and_alt_labels(entity2)
        for label1 in labels1:
            for label2 in labels2:
                # partial: ignore punctuation
                # token sort: different word order does not impact
                score = fuzz.partial_token_sort_ratio(label1, label2)
                if score > highest_score:
                    highest_score = score

        return highest_score / 100