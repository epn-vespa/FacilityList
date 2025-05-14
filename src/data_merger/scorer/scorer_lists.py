"""
List the available scorers and categorize them.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from data_merger.scorer import acronym_scorer, distance_scorer, fuzzy_scorer, cosine_similarity_scorer, llm_embedding_scorer, tfidf_scorer, type_incompatibility_scorer


class ScorerLists():

    # Scores that might help reduce the amount of candidate pairs if they
    # are above or below a certain threshold. They are computed first.
    DISCRIMINANT_SCORES = [type_incompatibility_scorer.TypeIncompatibilityScorer,
                           fuzzy_scorer.FuzzyScorer,
                           distance_scorer.DistanceScorer]

    # Scores that are computed for all of the candidate pairs.
    OTHER_SCORES = [acronym_scorer.AcronymScorer,
                    tfidf_scorer.TfIdfScorer]

    # Scores that use CUDA and cannot be computed in a forked thread
    CUDA_SCORES = [cosine_similarity_scorer.CosineSimilarityScorer,
                   llm_embedding_scorer.LlmEmbeddingScorer]


    ALL_SCORES = DISCRIMINANT_SCORES + OTHER_SCORES + CUDA_SCORES
    SCORES_BY_NAMES = {scorer.NAME: scorer for scorer in ALL_SCORES}


    # Lambda functions that return a boolean for discriminant criteria.

    # If the criteria is respected, then the candidate pair is admited.
    ADMIT = {fuzzy_scorer.FuzzyScorer: lambda x: x == 1.0, # Perfect label match
             acronym_scorer.AcronymScorer: lambda x: x == 1.0
            }
    #ADMIT.setdefault(0, lambda x: False)

    # If the criteria is not respected, then the candidate pair is eliminated.
    ELIMINATE = {"launch_date": lambda dist: dist != 0, # time distance
                 "distance": lambda dist: dist > 4 or dist == -2, # kilometers or incompatible
                 "type": lambda t: t == -2, # incompatible type
                 }
    #ELIMINATE.setdefault(0, lambda x: False)
