"""
Compare two entities' aperture's similarity by rounding
the values and converting units to meters if necessary.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from typing import Union
from graph.entity import Entity
from graph.synonym_set import SynonymSet
from data_mapper.filters.filter import Filter
from utils.performances import timeall
from utils.string_utilities import convert_to_meters, extract_number

class ApertureFilter(Filter):


    NAME = "aperture"

    @timeall
    def are_compatible(entity1: Union[Entity, SynonymSet],
                       entity2: Union[Entity, SynonymSet]) -> float:
        """
        Check if the two entities have the same aperture.
        They are compatible if one of them do not have any information
        about its aperture.

        Args:
            entity1: reference entity
            entity2: compared entity
        """
        apertures1 = entity1.get_values_for("aperture", unique = False)
        apertures2 = entity2.get_values_for("aperture", unique = False)
        if not apertures1 or not apertures2:
            return True
        apertures1 = [convert_to_meters(extract_number(a)) for a in apertures1]
        apertures2 = [convert_to_meters(extract_number(a)) for a in apertures2]

        inclusion_ratio = ApertureFilter._inclusion_ratio(apertures1, apertures2)
        if inclusion_ratio != 1:
            # If there is one or more aperture(s) that mismatch
            return False
        else:
            return True


    def compare_numbers(number1: float,
                        number2: float) -> bool:
        """
        Compare numbers from left to right. Return a string matching
        ratio until the digits are different.

        Args:
            number1: the aperture (float) in meters of the first entity.
            number2: the aperture (float) in meters of the second entity.
        """
        if number1 == number2:
            return True
        if number1.__trunc__() != number2.__trunc__():
            if round(number1) != round(number2):
                return False
        number1_str = str(number1)
        number2_str = str(number2)
        len_number1_str = len(number1_str)
        len_number2_str = len(number2_str)
        if len_number1_str > len_number2_str:
            number1_str, number2_str = number2_str, number1_str
            len_number1_str, len_number2_str = len_number2_str, len_number1_str
        has_comma = False
        n_after_comma = 0
        for i, digit1 in enumerate(number1_str):
            digit2 = number2_str[i]
            if has_comma:
                n_after_comma += 1
            if digit2 != digit1:
                # if has_comma:
                if i == len_number1_str - 1:
                    # Last number after comma is different. Rounded
                    if round(number1, ndigits = n_after_comma) == round(number2, ndigits = n_after_comma):
                        return True
                return (i - has_comma) / (len_number2_str - has_comma) # digits difference in decimals
            if digit1 == '.':
                has_comma = True

        # No digit divergence
        return False


    def _inclusion_ratio(numbers_e1: list[float],
                         numbers_e2: list[float]) -> float:
        """
        Check that numbers from the first entity are included into
        the numbers from the other entity. Returns an inclusion ratio.

        Args:
            numbers_e1: the apertures in meters of the first entity.
            numbers_e2: the apertures in meters of the second entity.
        """
        # Map every number from e1 to e2
        scores_e1 = 0
        numbers_e1 = set(numbers_e1)
        numbers_e2 = set(numbers_e2)
        if len(numbers_e1) == 0 or len(numbers_e2) == 0:
            return -2
        if len(numbers_e2) < len(numbers_e1):
            numbers_e1, numbers_e2 = numbers_e2, numbers_e1
        for number_e1 in numbers_e1:
            found = False
            for number_e2 in numbers_e2:
                score = ApertureFilter.compare_numbers(number_e1, number_e2)
                if score == 1:
                    scores_e1 += 1
                    found = True
                    break
            if found:
                continue
        return scores_e1 / len(numbers_e1)