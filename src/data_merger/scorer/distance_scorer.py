"""
Compute a distance between two entities.
If there are no latitude or longitude on one of them, it will test
their incompatibalities instead (not on the same continent / country / city)

Author:
    Liza Fretel (liza.fretel@obsmp.fr)
"""


from typing import Union
from data_merger.entity import Entity
from data_merger.scorer.score import Score
from data_merger.synonym_set import SynonymSet
from utils.performances import timeall
from utils.utils import distance


class DistanceScorer(Score):

    # Name of the score computed by this class (as in score.py)
    NAME = "distance"

    @timeall
    def compute(entity1: Union[Entity, SynonymSet],
                entity2: Union[Entity, SynonymSet]) -> float:
        """
        If the entities are more than 10km away from each other, the can
        not be the same (incompatible). If they do not have geolocation,
        they might be compatible.

        If one of them does not have coordinates, test their compatibility:
        - continent is the same;
        - country is the same;
        - city is the same.
        If no incompatibility was found, return -1.
        If an incompatibility was found, return -2.

        Keyword arguments:
        entity1 -- reference entity
        entity2 -- compared entity
        """
        lat1 = entity1.get_values_for("latitude", unique=True)
        lat2 = entity2.get_values_for("latitude", unique=True)
        long1 = entity1.get_values_for("longitude", unique=True)
        long2 = entity2.get_values_for("longitude", unique=True)

        if not (lat1 is None or lat2 is None or long1 is None or long2 is None):
            if (lat1 != 0 or long1 != 0) and (lat2 != 0 or long2 != 0):
                if distance((lat1, long1), (lat2, long2)) > 10:
                    return -2 # More than 10km away
                else:
                    return -1
                # return 1 - distance((lat1, long1), (lat2, long2)) / 20004 # perimeter / 2

        continent1 = entity1.get_values_for("continent", unique=True)
        continent2 = entity2.get_values_for("continent", unique=True)

        if (continent1 != continent2 and
            continent1 is not None and continent2 is not None):
            return -2 # Incompatible

        country1 = entity1.get_values_for("country", unique=True)
        country2 = entity2.get_values_for("country", unique=True)

        if (country1 != country2 and
            country1 is not None and country2 is not None):
            return -2

        city1 = entity1.get_values_for("city", unique=True)
        city2 = entity2.get_values_for("city", unique=True)
        if (city1 != city2 and
            city1 is not None and city2 is not None):
            return -2

        return -1 # No incompatibility found.
        # Might be due to the absence of location information or
        # the entities' compatibility.