"""
Compute tfidf scores between two entities. The tfidf scores compute a
similarity between the terms used within two expressions. It is often used
in Information Retrieval. It works as a bag of words and may not be optimized
for tasks such as paraphrasis.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from typing import Union
from data_merger.entity import Entity
from data_merger.graph import Graph
from data_merger.scorer.score import Score
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from data_merger.synonym_set import SynonymSet
from utils.performances import timeall


class TfIdfScorer(Score):

    # Name of the score computed by this class (as in score.py)
    NAME = "tfidf_cosine_similarity"


    vectorizer = None


    @timeall
    def compute(graph: Graph,
                entity1: Union[Entity, SynonymSet],
                entity2: Union[Entity, SynonymSet]) -> float:
        """
        Compute a cosine similarity score between the two entities.

        Keyword arguments:
        graph -- the graph
        entity1 -- reference entity
        entity2 -- compared entity
        """
        # Compute the embeddings of the documents
        if TfIdfScorer.vectorizer is None:
            TfIdfScorer.vectorizer = TfidfVectorizer().fit_transform(graph.get_descriptions(),
                                                                     lowercase = True)
        # TODO test with analyzer == 'char'
        # & ngram_range == (1, 3)

        description1 = ' '.join(entity1.get_values_for("description"))
        description2 = ' '.join(entity2.get_values_for("description"))
        
        
        return cosine_similarity(TfIdfScorer.vectorizer.transform(description1),
                                 TfIdfScorer.vectorizer.transform(description2))