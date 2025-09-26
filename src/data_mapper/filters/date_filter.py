from typing import Union
from graph.entity import Entity
from graph.synonym_set import SynonymSet
from data_mapper.filters.filter import Filter
from utils.performances import timeall


class DateFilter(Filter):

    # Name of the score computed by this class (as in score.py)
    NAME = "date"

    @timeall
    def are_compatible(entity1: Union[Entity, SynonymSet],
                       entity2: Union[Entity, SynonymSet]) -> float:
        """
        Check if any of the entity's date are incompatible (launch_date,
        start_date, end_date).

        Args:
            entity1: reference entity
            entity2: compared entity
        """
        # Check all relevant date fields
        date_attrs = ["launch_date", "start_date", "end_date"]
        for attr in date_attrs:
            if not DateFilter._compare_entity_dates(entity1,
                                                    entity2,
                                                    attr):
                return False
        return True

    @staticmethod
    def _compare_entity_dates(entity1: Union[Entity, SynonymSet],
                              entity2: Union[Entity, SynonymSet],
                              attrs: Union[str, list[str]]) -> bool:
        """
        Compare the year of the dates for a given field in both entities.
        Return True if dates are compatible, False if not.

        Args:
            entity1: reference entity
            entity2: compared entity
        """
        dates1 = set()
        dates2 = set()
        if type(attrs) == str:
            attrs = [attrs]
        for attr in attrs:
            dates1.update(entity1.get_values_for(attr))
            dates2.update(entity2.get_values_for(attr))

        # Only compare if both sets have dates
        if dates1 and dates2:
            return DateFilter._compare_years(dates1, dates2)
        return True  # If one or both sets are empty, we assume they are compatible.


    @staticmethod
    def _compare_years(dates1: set,
                       dates2: set) -> bool:
        """
        If any date from dates1 & dates2 are the same year,
        return True. Dates can be isoformat string or date type,
        as turtle does not support negative dates
        (example: Chankillo observatory from Wikidata)

        Args:
            dates1: set of dates or isoformat str date of entity 1
            dates2: set of dates or isoformat str date of entity 2
        """
        years1 = set()
        for date in dates1:
            if date is None:
                continue
            elif type(date) == str:
                # Negative date
                year = int('-' + date.split('-')[1])
            else:
                year = date.year
            years1.add(year)

        years2 = set()
        for date in dates2:
            if date is None:
                continue
            elif type(date) == str:
                # Negative date
                year = int('-' + date.split('-')[1])
            else:
                year = date.year
            years2.add(year)
        # True if not disjoint, False if disjoint.
        return not years1.isdisjoint(years2)