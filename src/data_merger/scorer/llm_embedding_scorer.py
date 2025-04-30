"""
Compute the Cosine Similarity between two entities basing on their
textual data: labels, alt labels, descriptions, location, superclass.
Use a LLM's encoder to compute the entities' embeddings.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from typing import Union
from data_merger.entity import Entity
from data_merger.scorer.score import Score
from data_merger.synonym_set import SynonymSet
from graph import Graph
from utils.performances import timeall


class LlmEmbeddingScorer(Score):

    # Name of the score computed by this class (as in score.py)
    NAME = "llm_embedding"


    @timeall
    def compute(graph: Graph,
                entity1: Union[Entity, SynonymSet],
                entity2: Union[Entity, SynonymSet]) -> float:
        pass # TODO