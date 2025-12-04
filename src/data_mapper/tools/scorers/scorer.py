"""
Define the superclass Scorer.
Scorers can hold a threshold.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import abc

from graph.entity import Entity
from data_mapper.tools.tool import Tool

class Scorer(Tool):
    """
    A score is a similarity value that can be computed between two entities individually.
    The compute method returns a value between 0 and 1 (similarity value).
    """

    WEIGHT = 1.0

    NAME = "Generic Scorer (superclass)"

    threshold_func = lambda self, score: False

    threshold = None

    symbol = None

    @abc.abstractmethod
    def compute(self,
                entity1: Entity,
                entity2: Entity) -> float:
        """
        Return a score value between 0 and 1.
        This method is implemented in the different Scorers.
        """
        raise NotImplementedError("This method should be overridden by subclasses.")


    def __str__(self):
        return self.NAME


    def set_threshold(self,
                      threshold: float,
                      symbol: str = '>='):
        if type(threshold) == str:
            threshold = float(threshold)
        if symbol == '>=':
            self.threshold_func = lambda self, score: score >= threshold
        elif symbol == '==':
            self.threshold_func = lambda self, score: score == threshold
        elif symbol == '>':
            self.threshold_func = lambda self, score: score > threshold
        elif symbol == '<':
            self.threshold_func = lambda self, score: score < threshold
        elif symbol == '<=':
            self.threshold_func = lambda self, score: score <= threshold


    def apply_threshold(self,
                        score: float) -> bool:
        """
        Method to accept or reject a mapping if a certain threshold
        is reached.
        """
        return self.threshold_func(score)


    def threshold_str(self) -> str:
        """
        String representation of the threshold function
        """
        if not self.symbol and not self.threshold:
            return ""
        return self.symbol + ' ' + self.threshold