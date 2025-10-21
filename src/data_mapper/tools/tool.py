"""
Define the superclass Tool.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import abc

from graph.entity import Entity

class Tool(abc.ABC):


    NAME = "Generic Tool (superclass)"


    _instances = {}

    def __new__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__new__(cls)
            cls._instances[cls] = instance
        return cls._instances[cls]



    def __str__(self):
        return self.NAME


    def __eq__(self, tool):
        return self.NAME == tool.NAME


    def __hash__(self):
        return hash(self.NAME)