"""
Compute tfidf scores between two entities. The tfidf scores compute a
similarity between the terms used within two expressions. It is often used
in Information Retrieval. It works as a bag of words and may not be optimized
for tasks such as paraphrasis.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import re
import string
import numpy as np
from typing import List, Set

from data_mapper.tools.embedders.embedder import Embedder
from graph.entity import Entity
from graph.graph import Graph
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from utils.performances import timeall
from nltk.corpus import stopwords


class TfIdfEmbedder(Embedder):
    """
    Only for one list of entities at a time.
    """

    # Name of the score computed by this class (as in score.py)
    NAME = "tfidf"

    tokenizer = None
    vectorizer = None

    # True if there were no definition or label in the ontology
    no_corpus = False

    # Document-term matrix
    dt_matrix = None


    TO_EXCLUDE = ["code", "url", "source", "location_confidence", "type_confidence"]

    ON_LANGUAGES = ["en", "ca", "fr", "es"]
    

    @timeall
    def compute(self, entities: List[Entity]) -> np.ndarray:
        """
        Compute a cosine similarity score between the two entities.

        Args
            entity1: reference entity
            entity2: compared entity
        """
        if self.no_corpus:
            return None # No corpus in the ontology.
        # Compute the embeddings of the documents
        if self.vectorizer is None:
            stop_words = set()
            self.add_stopwords(stop_words)

            # TODO test with analyzer == 'char'
            # & ngram_range == (1, 3)
            self.vectorizer = TfidfVectorizer(lowercase = True,
                                              # preprocessor=self._preprocess, # TODO remove the preprocessor
                                              tokenizer = self._custom_tokenizer,
                                              strip_accents='unicode',
                                              stop_words=list(stop_words),
                                              max_features=1000000)
            # self.tokenizer = self.vectorizer.build_tokenizer()
            #graph = Graph()
            #definitions = graph.get_graph_semantic_fields(language = ["en", "ca", "fr", "es"])
            entities_str_repr = []
            for entity in entities:
                entities_str_repr.append(entity.to_string(exclude = self.TO_EXCLUDE,
                                                          languages = self.ON_LANGUAGES))

            print(entities_str_repr)
            if not entities_str_repr:
                self.no_corpus = True
                return None # No textual data in the entities.

            self.dt_matrix = self.vectorizer.fit_transform(entities_str_repr).toarray()

            ### DEBUG ###
            vocab = self.vectorizer.vocabulary_
            print("vocab=", vocab)
            index_to_word = {v: k for k, v in self.vectorizer.vocabulary_.items()}
            row = self.dt_matrix[0]
            import numpy as np

            row_dense = row.ravel()
            nonzero = row_dense.nonzero()[0]
            for i in nonzero:
                print(index_to_word[i], row_dense[i])
            print(entities_str_repr[0])
            print(len(vocab), "mots dans le vocabulaire")
            print(list(vocab.items())[:50])

            print(self.dt_matrix.shape)
            ### END DEBUG ###
            return self.dt_matrix
        else:
            entities_str_repr = []
            if type(entities) == Entity:
                entities = [entities]
            for entity in entities:
                entities_str_repr.append(entity.to_string(exclude = self.TO_EXCLUDE,
                                                          languages = self.ON_LANGUAGES))
            return self.vectorizer.transform(entities_str_repr).toarray()

        """
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

        repr1 = TfIdfEmbedder.vectorizer.transform([repr1])
        repr2 = TfIdfEmbedder.vectorizer.transform([repr2])
        sim = cosine_similarity(repr1, repr2)
        sim = float(sim[0][0])
        if sim > 1:
            sim = 1
        return sim
        """


    def _preprocess(self,
                    text: str) -> str:
        """
        Preprocessing operations for the TfidfVectorizer.
        - Lowercase
        - Add spaces around digits
        - Remove characters that are not alphanumeric or spaces
        - Remove multiple spaces

        Args:
            text: input text to preprocess
        """
        text = text.lower()
        text = re.sub(r'(?<=[a-zA-Z])(?=\d)', ' ', text)
        text = re.sub(r'(?<=\d)(?=[a-zA-Z])', ' ', text)
        text = re.sub(r"[^\w\W\d ]", " ", text)
        text = re.sub(r" +", " ", text)
        return text
    

    punct_regex = re.compile("[" + re.escape(string.punctuation) + "]")
    def _custom_tokenizer(self,
                          text: str) -> str:
        
        """
        Tokenizer for the tfidf vectorizer
        - Lowercase
        - Add spaces around digits
        - Remove characters that are not alphanumeric or spaces
        - Remove multiple spaces

        Args:
            text: input text to preprocess
        """
        text = re.sub(r'(?<=[a-zA-Z])(?=\d)', ' ', text) # Separate text from digit
        text = re.sub(r'(?<=\d)(?=[a-zA-Z])', ' ', text) # Same
        text = re.sub(r"[^\w\d ]", " ", text) # Only one space
        text = re.sub(self.punct_regex, " ", text) # Remove punctuation
        tokens = re.findall(r'\b[a-zA-Z0-9]{1,5}', text.lower()) # 1 to 5 characters (pseudo-stemmization)
        print("tokens:", tokens)
        return tokens



    
    def add_stopwords(self,
                      stop_words: Set[str]) -> List[str]:
        """
        Add stopwords from nltk for several languages.

        Args:
            stopwords: set of stopwords to update
        """
        languages = ['english', 'french', 'spanish']
        for lang in languages:
            stop_words = stop_words.union(set(stopwords.words(lang)))