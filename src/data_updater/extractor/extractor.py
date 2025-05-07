"""
Define the superclass Extractor.
"""

class Extractor:


    AVAILABLE_NAMESPACES = ["aas",
                            "iaumpc",
                            "naif",
                            "nssdc",
                            "pds",
                            "spase",
                            "wikidata"]


    NAMESPACE = "extractor"


    def __repr__(self):
        return self.NAMESPACE


    def __str__(self):
        return self.NAMESPACE