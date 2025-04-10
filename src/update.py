#!/bin/python3

"""Perform extraction for all lists. Add data to the VO ontology without
checking if it already exists from another list. This is a preliminary
step before we filter out the duplicate entities.

Arguments:
-- list names to extract (optional, default "all")
-- input ontology to merge data with (optional, default None)
-- output ontology (optional, default "output.ttl")

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from typing import List
from argparse import ArgumentParser
from graph import Graph
#from data_updater.graph import Graph # rdflib.Graph singleton with OBS namespace
from data_updater.extractor.extractor import Extractor
from data_updater.extractor.aas_extractor import AasExtractor
from data_updater.extractor.iaumpc_extractor import IauMpcExtractor
from data_updater.extractor.naif_extractor import NaifExtractor
from data_updater.extractor.pds_extractor import PdsExtractor
from data_updater.extractor.spase_extractor import SpaseExtractor
from data_updater.extractor.wikidata_extractor import WikidataExtractor


# Extract for those sources:
all_extractors = [
    AasExtractor,
    IauMpcExtractor,
    NaifExtractor,
    PdsExtractor,
    SpaseExtractor,
    WikidataExtractor
]


class Updater():


    def __init__(self,
                 ontology_file: str = ""):
        self._graph = Graph(ontology_file)
        if not ontology_file:
            self.init_graph() # Create basic classes


    @property
    def graph(self):
        return self._graph


    def update(self,
               data: dict,
               extractor: Extractor = None,
               cat: str = "ufo"):
        """
        Adds the data from the dict to the Ontology.

        Keyword arguments:
        data -- a dictionary like {"uri1": {"uri":"a", "label":"b"}}
        source -- the class of the extractor of the source (ex: AasExtractor)
        not already in the dictionary's features.
        """
        for identifier, features in data.items():
            # get label
            if "label" in features:
                label = features["label"]
                subj = label # label_to_uri(label, source = source)
            else:
                subj = identifier
            for predicate, obj in features.items():
                self.graph.add((subj, predicate, obj),
                                extractor = extractor)
            if "type" not in features:
                if (not hasattr(extractor, "IS_ONTOLOGICAL") or
                    not extractor.IS_ONTOLOGICAL):
                    if cat == "ufo" and hasattr(extractor, "DEFAULT_TYPE"):
                        cat = extractor.DEFAULT_TYPE
                    self.graph.add((subj, "type", cat),
                                    extractor = extractor)
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
                                 "is_autoritative_for": [H]},
            WikidataExtractor.URI: {"url": WikidataExtractor.URL,
                                    "community": [A, H, G, P, O]}} # Not authoritative

        # TODO add other sources (can have more than one community)
        # every time we create an extraction script for the source.

        self.update(COMMUNITIES, cat = "community")
        self.update(SOURCES, cat = "facility list")


def main(lists: List[str],
         input_ontology: str = "",
         output_ontology: str = "output.ttl",
         format: str = "turtle"):
    updater = Updater(input_ontology)

    extractors = []

    if "all" in lists:
        extractors = all_extractors
    else:
        for list_to_extract in lists:
            for extractor in all_extractors:
                if list_to_extract == extractor.NAMESPACE:
                    extractors.append(extractor)

    for Extractor in extractors:
        extractor = Extractor()
        data = extractor.extract()
        updater.update(data, extractor = extractor)

    with open(output_ontology, 'w') as file:
        file.write(updater.graph.serialize(format = format))


if __name__ == "__main__":
    parser = ArgumentParser(
        prog = "updater.py",
        description = "Download data from different lists and merge them into\
              an output ontology.")

    parser.add_argument("-l",
                        "--lists",
                        dest = "lists",
                        default = ["all"],
                        choices = ["all"] + [e.NAMESPACE for e in all_extractors],
                        nargs = '*',
                        type = str,
                        required = False,
                        help = "Name of the lists to extract. 'all' will" + ""
                        "extract from all of the lists.")
    parser.add_argument("-i",
                        "--input-ontology",
                        dest = "input_ontology",
                        default = "",
                        type = str,
                        required = False,
                        help = "Input ontology. The triples in this ontology will be " +
                        "added in the output ontology with new data. Use to split " +
                        "the data extraction into different steps.")
    parser.add_argument("-o",
                        "--output-ontology",
                        dest = "output_ontology",
                        default = "output.ttl",
                        type = str,
                        required = False,
                        help = "Output ontology file to save the merged data.")
    parser.add_argument("-f",
                        "--output-format",
                        type = str,
                        default = "turtle",
                        choices = ["turtle", "rdf", "xml"],
                        required = False)
    args = parser.parse_args()
    main(args.lists, args.input_ontology, args.output_ontology)