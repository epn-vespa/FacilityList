"""
List the available scorers and categorize them.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from data_merger.scorer import acronym_scorer, date_scorer, digit_scorer, distance_scorer, fuzzy_scorer, cosine_similarity_scorer, label_match_scorer, llm_embedding_scorer, tfidf_scorer, type_incompatibility_scorer, identifier_scorer


class ScorerLists():

    # Scores that might help reduce the amount of candidate pairs if they
    # are above or below a certain threshold. They are computed first.
    DISCRIMINANT_SCORES = [type_incompatibility_scorer.TypeIncompatibilityScorer,
                           date_scorer.DateScorer,
                           distance_scorer.DistanceScorer,
                           label_match_scorer.LabelMatchScorer,
                           identifier_scorer.IdentifierScorer]

    # Scores that are computed for all of the candidate pairs.
    OTHER_SCORES = [acronym_scorer.AcronymScorer,
                    tfidf_scorer.TfIdfScorer,
                    fuzzy_scorer.FuzzyScorer,
                    digit_scorer.DigitScorer,
                    ]

    # Scores that use CUDA and cannot be computed in a forked thread
    CUDA_SCORES = [cosine_similarity_scorer.CosineSimilarityScorer, # Too long without GPU
                   llm_embedding_scorer.LlmEmbeddingScorer]


    ALL_SCORES = DISCRIMINANT_SCORES + OTHER_SCORES + CUDA_SCORES
    SCORES_BY_NAMES = {scorer.NAME: scorer for scorer in ALL_SCORES}


    # Lambda functions that return a boolean for discriminant criteria.

    # If the criteria is respected, then the candidate pair is admited.
    ADMIT = {
             # acronym_scorer.AcronymScorer: lambda x: x == 1.0
             label_match_scorer.LabelMatchScorer: lambda x: x == 1.0,  # Perfect label match
            }
    #ADMIT.setdefault(0, lambda x: False)

    # If the criteria is not respected, then the candidate pair is eliminated.
    ELIMINATE = {
                 distance_scorer.DistanceScorer: lambda dist: dist == -2, # dist > 10km or incompatible
                 type_incompatibility_scorer.TypeIncompatibilityScorer: lambda t: t == -2, # incompatible type
                 date_scorer.DateScorer: lambda d: d == -2, # not the same year (as sources have different precision on months/days)
                 identifier_scorer.IdentifierScorer: lambda i: i == -2,
                }
    #ELIMINATE.setdefault(0, lambda x: False)
