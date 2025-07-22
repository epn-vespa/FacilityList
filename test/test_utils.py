
import setup_path
from utils.utils import get_aperture
import unittest


class TestDigitScorer(unittest.TestCase):


    def test_get_aperture(self):
        test = [("0.4 inches, 5.0m", {'0.01m', '5.0m'})]
        for string, expected in test:
            _, res = get_aperture(string)
            assert(res == expected)


if __name__ == "__main__":
    unittest.main()