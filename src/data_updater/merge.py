#!/bin/python3

"""Perform extraction for all lists. Add data to the VO ontology without
checking if it already exists from another list. This is a preliminary
step before we filter out the duplicate entities.

Arguments:
-- VO ontology file (turtle)
-- objects type (ex: Observatory if it is a list of observatories).
   The object type must be in the taxonomy of the ontology.
   Add it to the concepts' taxonomy otherwise.
"""

from typing import Type
from argparse import ArgumentParser
from graph import Graph # rdflib.Graph singleton with OBS namespace
from typing import List
from aas_extractor import AasExtractor

class Merger():
    def __init__(self,
            ontology_file: str = ""):
        self._graph = Graph(ontology_file)
        if not ontology_file:
            self.init_graph() # Create basic classes

    @property
    def graph(self):
        return self._graph

    def merge(self,
            data: List,
            source: Type = None,
            cat: str = "ufo"):
        """
        Adds the data from the dict to the Ontology.

        Keyword arguments:
        data -- a list of dictionaries like [{"uri":"a", "Label":"b"}]
        source -- the class of the extractor of the source (ex: AasExtractor)
        cat -- the category of the objects in the list if they are
        not already in the dictionary's features.
        """
        for identifier, features in data.items():
            # TODO Identifier is the identifier according to the source list.
            # We want to save it with the source. Maybe use reification.
            # get label
            if "label" in features:
                label = features["label"]
                subj = label # label_to_uri(label, source = source)
            else:
                subj = identifier
            for predicate, obj in features.items():
                self.graph.add((subj, predicate, obj), source = source)
            if "type" not in features: 
                self.graph.add((subj, "type", cat), source = source)
            # Create the OBS uri

    def init_graph(self):
        """
        Create the basic classes (like the list of sources of the project)
        """
        COMMUNITIES = {
                "A": {"label": "celestial astronomy", "alliance": "IVOA"},
                "H": {"label": "heliophysics", "alliance": "IHDEA"},
                "G": {"label": "geology", "alliance": "OGC"},
                "P": {"label": "planetary sciences", "alliance": "IPDA"},
                "O": {"label": "other, generic"}}

        self.merge(COMMUNITIES, cat = "community")

        SOURCES = {AasExtractor.URI: {"url": AasExtractor.URL,
            "community": COMMUNITIES["A"]["label"]}}
        # TODO add other sources (can have more than one community)
        # every time we create an extraction script for the source.

        self.merge(SOURCES, cat = "facility list")

def main(input_ontology: str = ""):
    aas_extractor = AasExtractor()
    data_aas = aas_extractor.extract()
    merger = Merger(input_ontology)
    # merger.merge(data_aas, source = aas_extractor.get_source_uri())
    merger.merge(data_aas, source = AasExtractor)
    print(merger.graph.serialize())

if __name__ == "__main__":
    parser = ArgumentParser(
        prog = "merge.py",
        description = "Merge data from different lists to ontology.")
    parser.add_argument("-i",
            "--input-ontology",
            dest = "input_ontology",
            default = "",
            type = str,
            required = False,
            help = "Input ontology that will be merged with new data.")
    args = parser.parse_args()
    main(args.input_ontology)
