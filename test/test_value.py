import setup_path
from graph.entity import Entity
from rdflib import URIRef
from graph.value import Value, ValueSet
import unittest

class TestValue(unittest.TestCase):

    def test_add_value_with_diff_prov(self):
        ent1 = Entity(URIRef("values_test"))
        val1 = Value("a", provenance = "1")
        val2 = Value("a", provenance = "2")
        ent1.data = {"label": {val1, val2}}
        labels = ent1.get_values_for("label", return_raw_value = False)
        assert type(labels) == ValueSet
        assert len(labels) == 1
        assert len(list(labels)[0].provenance) == 2

if __name__ == "__main__":
    unittest.main()
