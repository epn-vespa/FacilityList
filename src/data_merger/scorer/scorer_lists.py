"""
Define the superclass Extractor.
"""

from enum import Enum
from data_merger.scorer import fuzzy_scorer


class ScorerLists():

    AVAILABLE_SCORES = [fuzzy_scorer.FuzzyScorer]

    DISCRIMINANT_SCORES = [fuzzy_scorer.FuzzyScorer]

    # Lambda functions that return a boolean for discriminant criteria.

    # If the criteria is respected, then the candidate pair is admited.
    ADMIT = {fuzzy_scorer.FuzzyScorer: lambda x: x == 1.0 # Perfect label match
             }
    #ADMIT.setdefault(0, lambda x: False)

    # If the criteria is not respected, then the candidate pair is eliminated.
    ELIMINATE = {"launch_date": lambda dist: dist != 0, # time distance
                 "earth_distance": lambda dist: dist > 4 # kilometers
                 }
    #ELIMINATE.setdefault(0, lambda x: False)
