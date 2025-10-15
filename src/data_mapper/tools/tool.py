"""
Define the superclass Tool.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import abc

from graph.entity import Entity

class Tool(abc.ABC):


    NAME = "Generic Tool (superclass)"


    def __str__(self):
        return self.NAME