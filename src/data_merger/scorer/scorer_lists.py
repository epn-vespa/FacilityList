"""
Define the superclass Extractor.
"""

from enum import Enum
from data_merger.scorer import acronym_scorer, fuzzy_scorer, cosine_similarity_scorer


class ScorerLists():

    # Scores that might help reduce the amount of candidate pairs if they
    # are above or below a certain threshold. They are computed first.
    DISCRIMINANT_SCORES = [fuzzy_scorer.FuzzyScorer]

    # Scores that are computed for all of the candidate pairs.
    OTHER_SCORES = [acronym_scorer.AcronymScorer]

    # Scores that use CUDA and cannot be computed in a forked thread
    CUDA_SCORES = [cosine_similarity_scorer.CosineSimilarityScorer]

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
