"""
Compute the Cosine Similarity between two entities basing on their
textual data: labels, alt labels, descriptions, location, superclass.
Use a LLM's encoder to compute the entities' embeddings.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from typing import Union
from data_mapper.entity import Entity
from data_mapper.scorer.score import Score
from data_mapper.synonym_set import SynonymSet
from utils.performances import timeall
from utils import llm_connection
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np



class LlmEmbeddingScorer(Score):

    # Name of the score computed by this class (as in score.py)
    NAME = "llm_embedding"


    @timeall
    def compute(entity1: Union[Entity, SynonymSet],
                entity2: Union[Entity, SynonymSet]) -> float:
        """
        Compute the embeddings of both entities and a cosine
        distance.

        Keyword arguments:
        entity1 -- reference entity
        entity2 -- compared entity
        """
        embed1 = llm_connection.LLM().embed(entity1.to_string(exclude = ["url",
                                                                         "code",
                                                                         "alt_label"]),
                                            from_cache = True,
                                            cache_key = str(entity1.uri))
        embed2 = llm_connection.LLM().embed(entity2.to_string(exclude = ["url",
                                                                         "code",
                                                                         "alt_label"]),
                                            from_cache = True,
                                            cache_key = str(entity2.uri))
        embed1 = np.array(embed1).reshape(1, -1)
        embed2 = np.array(embed2).reshape(1, -1)
        """
        dot_product = np.dot(embed1, embed2)
        norm1 = np.linalg.norm(embed1)
        norm2 = np.linalg.norm(embed2)
        return dot_product / (norm1 * norm2)
        """
        similarity = cosine_similarity(embed1, embed2)
        return similarity[0][0]