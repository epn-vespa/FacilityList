"""
Define the superclass Extractor.
"""

from data_updater import entity_types


class Extractor:


    AVAILABLE_NAMESPACES = ["aas",
                            "iaumpc",
                            "naif",
                            "nssdc",
                            "pds",
                            "spase",
                            "wikidata"]

    # Define some default constants to be overlapped by subclasses
    NAMESPACE = "extractor"
    TYPE_KNOWN = 1
    POSSIBLE_TYPES = entity_types.ALL_TYPES
    DEFAULT_TYPE = entity_types.OBSERVATION_FACILITY


    def __repr__(self):
        return self.NAMESPACE


    def __str__(self):
        return self.NAMESPACE