import unittest
import setup_path

from data_updater.extractor.aas_extractor import AasExtractor
from data_updater.extractor.pds_extractor import PdsExtractor
from data_mapper.candidate_pair import CandidatePairsMapping, CandidatePair
from data_mapper.entity import Entity


class TestCandidatePairMethods(unittest.TestCase):

    def test_del_candidate_pairs(self):

        CPM = CandidatePairsMapping(AasExtractor(), PdsExtractor())

        e1 = Entity("E1")
        e2 = Entity("E2")
        e3 = Entity("E3")
        f1 = Entity("F1")
        f2 = Entity("F2")

        CPM._list1_indexes = [e1, e2, e3]
        CPM._list2_indexes = [f1, f2]
        CPM._mapping = [
            [CandidatePair(e1, f1), CandidatePair(e1, f2)],
            [CandidatePair(e2, f1), CandidatePair(e2, f2)],
            [CandidatePair(e3, f1), CandidatePair(e3, f2)],
        ]

        CPM.del_candidate_pairs(e2)
        assert CPM._list1_indexes == [e1, e3]
        assert len(CPM._mapping) == 2
        assert all(len(row) == 2 for row in CPM._mapping)

        CPM.del_candidate_pairs(f1)
        assert CPM._list2_indexes == [f2]
        assert all(len(row) == 1 for row in CPM._mapping)

        # Already removed
        CPM.del_candidate_pairs(e2)
        CPM.del_candidate_pairs(f1)

    def test_iter(self):
        CPM = CandidatePairsMapping(AasExtractor(), PdsExtractor())

        e1 = Entity("E1")
        e2 = Entity("E2")
        e3 = Entity("E3")
        f1 = Entity("F1")
        f2 = Entity("F2")

        CPM._list1_indexes = [e1, e2, e3]
        CPM._list2_indexes = [f1, f2]
        CPM._mapping = [
            [CandidatePair(e1, f1), CandidatePair(e1, f2)],
            [CandidatePair(e2, f1), CandidatePair(e2, f2)],
            [None, CandidatePair(e3, f2)],
        ]

        for i, (_, _, cp) in enumerate(CPM.iter_mapping()):
            if i == 1:
                assert cp.member1 == e2 and cp.member2 == f2
        assert i == 4

if __name__ == "__main__":
    unittest.main()