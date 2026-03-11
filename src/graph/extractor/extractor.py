"""
Define the superclass Extractor.
Each extractor has its own namespace.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from graph import entity_types


class Extractor():
    """
    Superclass defining an extractor.
    """

    # Define some default constants to be overlapped by subclasses
    NAMESPACE = "extractor"
    TYPE_KNOWN = 1
    POSSIBLE_TYPES = entity_types.ALL_TYPES
    DEFAULT_TYPE = entity_types.OBSERVATION_FACILITY


    _instances = {}

    def __new__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__new__(cls)
            cls._instances[cls] = instance
        return cls._instances[cls]


    def __repr__(self):
        return self.NAMESPACE


    def __str__(self):
        return self.NAMESPACE