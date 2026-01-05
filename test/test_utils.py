import setup_path
from utils.string_utilities import get_aperture
from utils.dict_utilities import majority_voting_merge
from graph.properties import Properties
from datetime import datetime
import unittest


p = Properties()
class TestDigitScorer(unittest.TestCase):


    def test_get_aperture(self):
        test = [("0.4 inches, 5.0m", {'0.01m', '5.0m'}),
                ("65cm", {'0.65m'})]
        for string, expected in test:
            _, res = get_aperture(string)
            assert(res == expected)


    def test_majority_voting_merge(self):
        dict1 = {"label": "aas",
                 "aperture": 0.5,
                 "latitude": 3.692,
                 "longitude": 20.5,
                 "location_confidence": 0.5,
                 "address": "Californie",
                 "state": "California",
                 "description":  "dict1",
                 "url": "dict1.html",
                 "source": "aas"}
        dict2 = {"label": "wiki",
                 "aperture": 0.51,
                 "start_date": datetime(2014, 1, 1),
                 "latitude": 3.6921,
                 "longitude": 20.5,
                 "location_confidence": 1,
                 "address": "Californie",
                 "state": "California",
                 "description": "dict2",
                 "url": "dict2.html",
                 "source": "wikidata"}
        dict3 = {"label": "spasee",
                 "aperture": 0.8,
                 "start_date": datetime(2014, 9, 25),
                 "latitude": 3.69219,
                 "longitude": 20.53,
                 "location_confidence": 1,
                 "address": "Californie_2",
                 "state": "California_2",
                 "description": "dict3",
                 "url": "dict3.html",
                 "source": "spase"}
        res = majority_voting_merge([dict1, dict2, dict3])
        assert(res[p.aperture] == 0.51)
        assert(len(res[p.description]) == 3)
        assert(len(res[p.url]) == 3)
        assert(res[p.start_date] == [datetime(2014, 9, 25)])


    def test_integrity_of_majority_voting_merge(self):
        dict4 = {"label": "aas_",
                 "aperture": 0.5,
                 "latitude": 3.692,
                 "longitude": 20.5,
                 "location_confidence": 0.5,
                 "address": "Californie",
                 "state": "California",
                 "description":  "dict1",
                 "url": "dict1.html",
                 "source": "aas"}
        res = majority_voting_merge([dict4])
        assert(res[p.source] == ["aas"])

if __name__ == "__main__":
    unittest.main()
