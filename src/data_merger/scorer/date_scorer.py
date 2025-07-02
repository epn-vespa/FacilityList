from typing import Union
from data_merger.entity import Entity
from data_merger.scorer.score import Score
from data_merger.synonym_set import SynonymSet
from graph import Graph
from utils.performances import timeall


class DateScorer(Score):

    # Name of the score computed by this class (as in score.py)
    NAME = "date"

    @timeall
    def compute(graph: Graph,
                entity1: Union[Entity, SynonymSet],
                entity2: Union[Entity, SynonymSet]) -> float:
        """
        Check if any of the entity's date are incompatible (launch_date,
        start_date, end_date).

        If no incompatibility was found, return -1.
        If an incompatibility was found, return -2.
        """
        # Check all relevant date fields
        date_attrs = ["launch_date", "start_date", "end_date"]
        for attr in date_attrs:
            if not DateScorer._compare_entity_dates(entity1,
                                                    entity2,
                                                    attr):
                return -2 # Return -2 if any incompatibility is found
        return -1 # Return -1 if no incompatibility

    @staticmethod
    def _compare_entity_dates(entity1: Union[Entity, SynonymSet],
                              entity2: Union[Entity, SynonymSet],
                              attr: str) -> bool:
        """
        Compare the year of the dates for a given field in both entities.
        Return True if dates are compatible, False if not.
        """
        dates1 = entity1.get_values_for(attr)
        dates2 = entity2.get_values_for(attr)

        # Only compare if both sets have dates
        if dates1 and dates2:
            return DateScorer._compare_years(dates1, dates2)
        return True  # If one or both sets are empty, we assume they are compatible.

    @staticmethod
    def _compare_years(dates1: set,
                       dates2: set) -> bool:
        """
        If any date from dates1 & dates2 are the same year,
        return True. Dates can be isoformat string or date type,
        as turtle does not support negative dates
        (example: Chankillo observatory from Wikidata)

        Keyword arguments:
        dates1 -- set of dates or isoformat str date of entity 1
        dates2 -- set of dates or isoformat str date of entity 2
        """
        years1 = set()
        for date in dates1:
            if date is None:
                continue
            elif type(date) == str:
                year = int(date.split('-')[0])
            else:
                year = date.year
            years1.add(year)

        years2 = set()
        for date in dates2:
            if date is None:
                continue
            elif type(date) == str:
                year = int(date.split('-')[0])
            else:
                year = date.year
            years2.add(year)
        #years1 = {date1.year for date1 in dates1 if date1 is not None}
        #years2 = {date2.year for date2 in dates2 if date2 is not None}
        return not years1.isdisjoint(years2)
