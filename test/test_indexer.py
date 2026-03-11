import setup_path
from data_mapper.indexer import Indexer
from graph.extractor.pds_extractor import PdsExtractor
from graph.extractor.aas_extractor import AasExtractor
from data_mapper.tools.embedders.tfidf_embedder import TfIdfEmbedder
import numpy as np
import unittest


class TestDigitScorer(unittest.TestCase):

    class Entity():

        def __init__(self, name):
            self._name = name

        def get_synonyms(self):
            return [0]

        def __eq__(self, ent):
            return self._name == ent._name

        def __hash__(self):
            return hash(self._name)


    def test_indexer(self):
        extractor1 = PdsExtractor()
        extractor2 = AasExtractor()
        embedders1 = [TfIdfEmbedder()]
        embedders2 = [TfIdfEmbedder()]
        entities1 = [self.Entity("ent1"), self.Entity("ent2")]
        entities2 = [self.Entity("ent3"), self.Entity("ent4")]
        embeddings1 = np.array([1, 0.5])
        embeddings2 = np.array([0.25, 1])
        entity_types = ["a"]
        
        indexer1 = Indexer(extractor1,
                           embedders1,
                           entities1,
                           embeddings1)

        indexer2 = Indexer(extractor2,
                           embedders2,
                           entities2,
                           embeddings2)

        assert len(Indexer._registry) == 2

        assert indexer1.indexes[entities1[0]] != indexer2.indexes[entities2[0]]

        indexer1.merge_embeddings(extractor2, entities1[0], entities2[0])
        # print(indexer1.indexes[entities1[0]])
        # print(indexer2.indexes[entities2[0]])

        assert indexer1.indexes[entities1[0]] == indexer2.indexes[entities2[0]]


if __name__ == "__main__":
    unittest.main()
