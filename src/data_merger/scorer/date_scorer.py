"""
Verify that dates are the same between two entities.
"""

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
        ld_e1 = entity1.get_values_for("launch_date")
        ld_e2 = entity2.get_values_for("launch_date")
        #ld_e1 = {x.date() for x in ld_e1}
        #ld_e2 = {x.date() for x in ld_e2}
        if ld_e1 and ld_e2 and not DateScorer._compare_years(ld_e1, ld_e2):#ld_e1.intersection(ld_e2):
            return -2
        sd_e1 = entity1.get_values_for("start_date")
        sd_e2 = entity2.get_values_for("start_date")
        if sd_e1 and sd_e2 and not DateScorer._compare_years(sd_e1, sd_e2):# sd_e1.intersection(sd_e2):
            return -2
        ed_e1 = entity1.get_values_for("end_date")
        ed_e2 = entity2.get_values_for("end_date")
        if ed_e1 and ed_e2 and not DateScorer._compare_years(ed_e1, ed_e2):#ed_e1.intersection(ed_e2):
            return -2
        return -1

    def _compare_years(dates1: set,
                       dates2: set) -> bool:
        """
        If any date from dates1 & dates2 are the same year,
        return True
        """
        for date1 in dates1:
            if date1 is None:
                continue
            year1 = date1.year
            for date2 in dates2:
                if date2 is None:
                    continue
                year2 = date2.year
                if year1 == year2:
                    # Same year.
                    # TODO: compare months (sometimes 1st of January is for an
                    # empty date)
                    return True
        return False