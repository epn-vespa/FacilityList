"""
Compute fuzzy scores between two entities. Take the main label
and alt label into consideration. If there is a perfect match,
return 1. Fuzzy score computes a similarity between two strings,
or an edit-distance. We use the Levenshtein distance here.

Troubleshooting:
    fuzzywuzzy is a very slow library, and Wikidata has a lot of
    alt labels.
    1. Only keep one alt label
    2. Use rapidfuzz instead

Author:
    Liza Fretel (liza.fretel@obsmp.fr)
"""


from typing import List, Union
# from fuzzywuzzy import fuzz as wuzz
from rapidfuzz import fuzz, utils
from unidecode import unidecode

from data_mapper.entity import Entity
from data_mapper.scorer.score import Score
from data_mapper.synonym_set import SynonymSet
from utils.performances import timeall


class FuzzyScorer(Score):

    # Name of the score computed by this class
    NAME = "fuzzy_levenshtein"


    @timeall
    def compute(entity1: Union[Entity, SynonymSet],
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

        labels1 = entity1.get_values_for("label")
        labels1.update(entity1.get_values_for("alt_label"))
        labels2 = entity2.get_values_for("label")
        labels2.update(entity2.get_values_for("alt_label"))
        for label1 in labels1:
            for label2 in labels2:
                """
                # partial: ignore punctuation?
                # token sort: different word order does not impact
                # FuzzyWuzzy
                # Total: 273.05 seconds for aas/nssdc
                score = wuzz.token_sort_ratio(remove_punct(label1.lower()),
                                              remove_punct(label2.lower()))
                """

                # RapidFuzz
                label1 = unidecode(label1).lower()
                label2 = unidecode(label2).lower()

                score = fuzz.token_sort_ratio(label1,
                                              label2,
                                              # processor = utils.default_process
                                              )

                if score == 100:
                    return 1.0
                if score > highest_score:
                    highest_score = score
        return highest_score / 100