import setup_path
from graph.graph import Graph
from graph.entity import Entity
from graph.properties import Properties
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
    graph.add((entity2, graph.OBS["source"], graph.OBS["pds_list"]))
    aas_extractor = extractor_lists.AasExtractor()
    pds_extractor = extractor_lists.PdsExtractor()
    entity3 = graph.OBS["ent3"]

    def test_get_entities_from_list(self):
        for e, in self.graph.get_entities_from_list(self.aas_extractor):
            assert e == self.entity1

    def test_add_synonym(self):
        ent1 = Entity(self.entity1)
        ent2 = Entity(self.entity2)
        res = self.graph.get_entities_from_list(self.aas_extractor,
                                                no_equivalent_in = self.pds_extractor)
        assert len(res) == 1
        ent1.add_synonym(ent2,
                         extractor1 = self.pds_extractor,
                         extractor2 = self.aas_extractor)
        assert self.entity1 in ent2.get_synonyms()
        assert self.entity2 in ent1.get_synonyms()
        res = self.graph.get_entities_from_list(self.aas_extractor,
                                                no_equivalent_in = self.pds_extractor)
        assert len(res) == 0

    def test_add_synonym_twice(self):
        ent1 = Entity(self.entity1)
        ent2 = Entity(self.entity2)
        ent3 = Entity(self.entity3)
        ent1.add_synonym(ent2,
                         extractor1 = self.pds_extractor,
                         extractor2 = self.aas_extractor)
        ent1.add_synonym(ent3,
                         extractor1 = self.pds_extractor,
                         extractor2 = self.aas_extractor)
        assert(len(ent1.get_synonyms()) == 2)
        assert(len(ent2.get_synonyms()) == 2)
        assert(len(ent3.get_synonyms()) == 2)

if __name__ == "__main__":
    unittest.main()
