import setup_path
from graph.graph import Graph
from rdflib import RDF
from graph.extractor import extractor_lists
import unittest


class TestEntity(unittest.TestCase):

    graph = Graph()
    entity1 = graph.OBS["ent1"]
    graph.add((entity1, RDF.type, graph.OBS["observatory"]))
    graph.add((entity1, graph.OBS["source"], graph.OBS["aas_list"]))
    entity2 = graph.OBS["ent2"]
    graph.add((entity2, RDF.type, graph.OBS["observatory"]))
    aas_extractor = extractor_lists.AasExtractor()

    def test_entity(self):
        for e, in self.graph.get_entities_from_list(self.aas_extractor):
            assert e == self.entity1
        # assert self.entity1 in self.graph.get_entities_from_list(self.aas_extractor)

if __name__ == "__main__":
    unittest.main()