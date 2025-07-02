
"""
Acronymous test
"""
import setup_path
from data_merger.scorer.digit_scorer import DigitScorer
import unittest


class TestDigitScorer(unittest.TestCase):


    def test_compare_numbers(self):
        test = [(1.10, 1.099, 1),
                (0.3, 0.375, 1),
                (4, 40, 0),
                (3, 3.3, 1),
                (3.3, 3.4, 0.5),
                (3.9, 3.104, 0.25),
                (0.4, 0.04, 1/3),
                (38, 37.9999, 1)]
        for number1, number2, expected in test:
            res = DigitScorer.compare_numbers(number1, number2)
            print("test for", number1, number2, expected)
            print("res = ", res)
            assert(res == expected)


    def test_compute_(self):
        res = DigitScorer._inclusion_ratio([0.0, 0.5], [0.0, 0])
        print("RES:", res)
        assert(res == 1)


if __name__ == "__main__":
    unittest.main()