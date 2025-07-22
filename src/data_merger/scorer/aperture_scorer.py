"""
Compare two entities' aperture's similarity by rounding
the values and converting units to meters if necessary.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from numbers import Number
from typing import Union
from data_merger.entity import Entity
from data_merger.scorer.score import Score
from data_merger.synonym_set import SynonymSet
from utils.performances import timeall
from utils.utils import convert_to_meters, extract_number

import re

class ApertureScorer(Score):


    NAME = "aperture"

    @timeall
    def compute(entity1: Union[Entity, SynonymSet],
                entity2: Union[Entity, SynonymSet]) -> float:
        """
        Check if the two entities have the same aperture.
        Return -1 if they do, -2 otherwise.
        Return -1 if one of them do not have any information
        about its aperture.

        Keyword arguments:
        entity1 -- reference entity
        entity2 -- compared entity
        """
        apertures1 = entity1.get_values_for("aperture", unique = False)
        apertures2 = entity2.get_values_for("aperture", unique = False)
        apertures1 = [convert_to_meters(extract_number(a)) for a in apertures1]
        apertures2 = [convert_to_meters(extract_number(a)) for a in apertures2]

        inclusion_ratio = ApertureScorer._inclusion_ratio(apertures1, apertures2)
        if inclusion_ratio != 1:
            return -2
        else:
            return -1



    def _get_numbers(entity: Entity,
                     identifiers: list[str]) -> list:
        """
        Return a list of float from the entity.

        Keyword arguments:
        entity -- Entity to get numbers from
        identifiers -- ignore substrings or numbers that are in identifiers
        """
        result = []

        for key, values in entity._data.items():
            if "id" in key.lower() or "uri" in key.lower():
                continue # Ignore identifiers
            for value in values:
                if isinstance(value, Number):
                    if str(value) in identifiers:
                        continue
                    else:
                        result.append(float(value))
                elif type(value) == str:
                    # True for Literal
                    for number in re.findall(r"\d+(?:\.\d+)?", value):
                        result.append(float(number))
                else:
                    try:
                        v = float(value)
                        result.append(v)
                    except:
                        continue
        return result


    def compare_numbers(number1: float,
                        number2: float) -> float:
        """
        Compare numbers from left to right. Return a string matching
        ratio until the digits are different.
        """
        if number1 == number2:
            return 1
        if number1.__trunc__() != number2.__trunc__():
            if round(number1) != round(number2):
                return 0
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
                        return 1
                return (i - has_comma) / (len_number2_str - has_comma) # digits difference in decimals
            if digit1 == '.':
                has_comma = True

        # No digit divergence
        return 1


    def _inclusion_ratio(numbers_e1: list[float],
                         numbers_e2: list[float]) -> float:
        """
        Check that numbers from the first entity are included into
        the numbers from the other entity. Returns an inclusion ratio.
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
                score = ApertureScorer.compare_numbers(number_e1, number_e2)
                if score == 1:
                    scores_e1 += 1
                    found = True
                    break
            if found:
                continue
        return scores_e1 / len(numbers_e1)