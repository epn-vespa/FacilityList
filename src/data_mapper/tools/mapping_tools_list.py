"""
List the available scorers and categorize them.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import pkgutil
import inspect
import importlib

from data_mapper.tools.filters.filter import Filter
from data_mapper.tools.matchers.matcher import Matcher
from data_mapper.tools.scores.score import Score
from data_mapper.tools.embedders.embedder import Embedder
from data_mapper.tools import filters
from data_mapper.tools import matchers
from data_mapper.tools import scores
from data_mapper.tools import embedders

class MappingToolsList():

    FILTERS = []
    for loader, module_name, is_pkg in pkgutil.iter_modules(filters.__path__):
        module = importlib.import_module(f"{filters.__name__}.{module_name}")
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if obj.__module__ == module.__name__:
                if issubclass(obj, Filter) and obj is not Filter:
                    FILTERS.append(obj)

    MATCHERS = []
    for loader, module_name, is_pkg in pkgutil.iter_modules(matchers.__path__):
        module = importlib.import_module(f"{matchers.__name__}.{module_name}")
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if obj.__module__ == module.__name__:
                if issubclass(obj, Matcher) and obj is not Matcher:
                    MATCHERS.append(obj)

    SCORE_TOOLS = []
    for loader, module_name, is_pkg in pkgutil.iter_modules(scores.__path__):
        module = importlib.import_module(f"{scores.__name__}.{module_name}")
        
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if obj.__module__ == module.__name__:
                if issubclass(obj, Score) and obj is not Score:
                    SCORE_TOOLS.append(obj)

    EMBEDDER_TOOLS = []
    for loader, module_name, is_pkg in pkgutil.iter_modules(embedders.__path__):
        module = importlib.import_module(f"{embedders.__name__}.{module_name}")

        for name, obj in inspect.getmembers(module, inspect.isclass):
            if obj.__module__ == module.__name__:
                if issubclass(obj, Embedder) and obj is not Embedder:
                    EMBEDDER_TOOLS.append(obj)

    ALL_TOOLS = FILTERS + MATCHERS + SCORE_TOOLS + EMBEDDER_TOOLS
    TOOLS_BY_NAMES = {tool.NAME: tool for tool in ALL_TOOLS}

    """
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
                 aperture_scorer.ApertureScorer: lambda a: a == -2,
                }
    #ELIMINATE.setdefault(0, lambda x: False)
    """