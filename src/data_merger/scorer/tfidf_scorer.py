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
from graph import Graph
from data_merger.scorer.score import Score
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from data_merger.synonym_set import SynonymSet
from utils.performances import timeall
from nltk.corpus import stopwords

import re
import pickle


class TfIdfScorer(Score):

    # Name of the score computed by this class (as in score.py)
    NAME = "tfidf_cosine_similarity"

    tokenizer = None
    vectorizer = None

    # True if there were no definition or label in the ontology
    no_corpus = False


    @timeall
    def compute(entity1: Union[Entity, SynonymSet],
                entity2: Union[Entity, SynonymSet]) -> float:
        """
        Compute a cosine similarity score between the two entities.

        Keyword arguments:
        entity1 -- reference entity
        entity2 -- compared entity
        """
        if TfIdfScorer.no_corpus:
            return 0 # No corpus in the ontology.
        # Compute the embeddings of the documents
        if TfIdfScorer.vectorizer is None:
            stop_words = set()
            languages = ['english', 'french', 'spanish']
            for lang in languages:
                stop_words = stop_words.union(set(stopwords.words(lang)))
            TfIdfScorer.vectorizer = TfidfVectorizer(lowercase=True,
                                                     preprocessor=TfIdfScorer.preprocess,
                                                     stop_words=list(stop_words),
                                                     max_features=1000000)
            TfIdfScorer.tokenizer = TfIdfScorer.vectorizer.build_tokenizer()
            graph = Graph()
            definitions = graph.get_graph_semantic_fields(language = ["en", "ca", "fr", "es"])
            if not definitions:
                TfIdfScorer.no_corpus = True
                return 0
            TfIdfScorer.vectorizer.fit_transform(definitions)
        # TODO test with analyzer == 'char'
        # & ngram_range == (1, 3)

        repr1 = ' '.join(entity1.get_values_for("definition").
                         union(entity1.get_values_for("description")).
                         union(entity1.get_values_for("label", language = ["en", "ca", "fr", "es"])))
        repr2 = ' '.join(entity2.get_values_for("definition").
                         union(entity2.get_values_for("description")).
                         union(entity2.get_values_for("label", language = ["en", "ca", "fr", "es"])))

        if not repr1 or not repr2:
            # We need both entities to have a description to compute
            # a cosine similarity.
            return -1 # No score could be computed.

        repr1 = TfIdfScorer.vectorizer.transform([repr1])
        repr2 = TfIdfScorer.vectorizer.transform([repr2])
        sim = cosine_similarity(repr1, repr2)
        return float(sim[0][0])


    def preprocess(text: str) -> str:
        """
        Preprocessing operations for the TfidfVectorizer.
        - Add spaces around digits
        - Remove characters that are not alphabetic
        - Remove multiple spaces
        """
        text = re.sub(r'(?<=[a-zA-Z])(?=\d)', ' ', text)
        text = re.sub(r'(?<=\d)(?=[a-zA-Z])', ' ', text)
        text = re.sub(r"[^\w\W\d ]", " ", text)
        text = re.sub(r" +", " ", text)
        return text