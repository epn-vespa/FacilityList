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
from extractor.aas_extractor import AasExtractor
from extractor.iaumpc_extractor import IauMpcExtractor
from extractor.naif_extractor import NaifExtractor
from extractor.pds_extractor import PdsExtractor
from extractor.spase_extractor import SpaseExtractor


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
            data: dict,
            source: Type = None,
            cat: str = "ufo"):
        """
        Adds the data from the dict to the Ontology.

        Keyword arguments:
        data -- a dictionary like {"uri1": {"uri":"a", "label":"b"}}
        source -- the class of the extractor of the source (ex: AasExtractor)
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
                if cat == "ufo" and hasattr(source, "DEFAULT_TYPE"):
                    cat = source.DEFAULT_TYPE
                self.graph.add((subj, "type", cat), source = source)
            # Create the OBS uri


    def init_graph(self):
        """
        Create the basic classes (like the list of sources of the project)
        """
        # Labels
        A = "celestial astronomy"
        H = "heliophysics"
        G = "geology"
        P = "planetary sciences"
        O = "other, generic"

        COMMUNITIES = {
                "A": {"label": A, "alliance": "IVOA"},
                "H": {"label": H, "alliance": "IHDEA"},
                "G": {"label": G, "alliance": "OGC"},
                "P": {"label": P, "alliance": "IPDA"},
                "O": {"label": O}}

        SOURCES = {
            AasExtractor.URI: {"url": AasExtractor.URL,
                               "community": A,
                               "is_authoritative_for": A},
            IauMpcExtractor.URI: {"url": IauMpcExtractor.URL,
                                  "community": [A, P],
                                  "is_authoritative_for": [A, P]},
            NaifExtractor.URI: {"url": NaifExtractor.URL,
                                "community": [A, H, P],
                                "is_authoritative_for": [H, P]},
            PdsExtractor.URI: {"url": PdsExtractor.URL,
                               "community": [H, P, G],
                               "is_authoritative_for": [P]},
            SpaseExtractor.URI: {"url": SpaseExtractor.URL,
                                 "community": [H, P],
                                 "is_autoritative_for": [H]}}
        # TODO add other sources (can have more than one community)
        # every time we create an extraction script for the source.

        self.merge(COMMUNITIES, cat = "community")
        self.merge(SOURCES, cat = "facility list")


def main(input_ontology: str = ""):
    merger = Merger(input_ontology)

    # Extract for those sources:
    extractors = [
        #AasExtractor,
        #IauMpcExtractor,
        #NaifExtractor,
        PdsExtractor,
        #SpaseExtractor,
    ]

    for Extractor in extractors:
        extractor = Extractor()
        data = extractor.extract()
        merger.merge(data, source = extractor)

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