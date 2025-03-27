"""
Define the superclass Extractor.
"""

from enum import Enum
from data_merger.scorer import fuzzy_scorer


class ScorerLists():

    AVAILABLE_SCORES = []
    #                    ["cos_similarity",
    #                    "fuzzy_levenshtein",
    #                    "acronym_probability"]

    DISCRIMINANT_SCORES = [fuzzy_scorer.FuzzyScorer]
    #                      ["perfect_label_match", # fuzzy score is 1
    #                       "same_external_id",
    #                       "earth_position_distance",
    #                       "same_launch_date"]

    # Lambda functions that return a boolean for discriminant criteria.
    DISCRIMINATE = {"same_launch_date": lambda a, b: a == b,
                    "earth_position_distance": lambda dist: dist < 4, # kilometers
                    fuzzy_scorer.FuzzyScorer: lambda x: x == 1.0} # Perfect label match