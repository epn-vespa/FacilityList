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

import setup_path
import atexit
from typing import List
from argparse import ArgumentParser

from tqdm import tqdm
from graph import Graph
from data_updater import entity_types
from data_updater.extractor.extractor import Extractor
from data_updater.extractor.extractor_lists import ExtractorLists
from data_updater.extractor.aas_extractor import AasExtractor
from data_updater.extractor.iaumpc_extractor import IauMpcExtractor
from data_updater.extractor.naif_extractor import NaifExtractor
from data_updater.extractor.pds_extractor import PdsExtractor
from data_updater.extractor.spase_extractor import SpaseExtractor
from data_updater.extractor.wikidata_extractor import WikidataExtractor

from utils.utils import get_location_info


class Updater():


    def __init__(self,
                 ontology_file: str = "",
                 output_ontology: str = ""):
        self._graph = Graph(ontology_file)
        if not ontology_file:
            self.init_graph() # Create basic classes
        self._output_ontology = output_ontology


    @property
    def graph(self):
        return self._graph


    @property
    def output_ontology(self):
        return self._output_ontology


    def update(self,
               data: dict,
               extractor: Extractor = None,
               cat: str = "ufo"):
        """
        Add the data from the dict to the Ontology.

        Keyword arguments:
        data -- a dictionary like {"uri1": {"uri":"a", "label":"b"}}
        source -- the class of the extractor of the source (ex: AasExtractor)
        not already in the dictionary's features.
        """

        for identifier, features in tqdm(data.items(), desc = f"Add entities to ontology"):
            # Get complete location information and add them to the features
            if extractor: # Only for extracted entities
                cat = features.get("type", None)
                if (cat == entity_types.GROUND_OBSERVATORY or
                    cat in entity_types.MAY_HAVE_ADDR and (
                     "latitude" in features and "longitude" in features or
                     "location" in features or
                     "is_part_of" in features and cat == entity_types.TELESCOPE and
                     data[features["is_part_of"]].get("type", None) in entity_types.GROUND_TYPES
                    )):
                    location_info = get_location_info(label = features.get("label", None),
                                                      latitude = features.get("latitude", None),
                                                      longitude = features.get("longitude", None),
                                                      address = features.get("address", None),
                                                      location = features.get("location", None),
                                                      part_of = features.get("is_part_of", None))
                    # Retrieved information include country, continent. We also set location to
                    # Earth or Space.
                    for key, value in location_info.items():
                        if key not in features or features[key] is None:
                            features[key] = value
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
                # For non-ontological entities, if it has a default type and
                # does not have a type, we use the extractor's default type as
                # the entity's superclass.
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


    def write(self):
        """
        """
        with open(self.output_ontology, 'wb') as file:
            file.write(self.graph.serialize(format = "turtle",
                                            encoding = "utf-8"))

def main(lists: List[str],
         input_ontology: str = "",
         output_ontology: str = "output.ttl"):

    updater = Updater(input_ontology,
                      output_ontology)
    atexit.register(updater.write)

    extractors = []

    if "all" in lists:
        extractors = ExtractorLists.AVAILABLE_EXTRACTORS
    else:
        for list_to_extract in lists:
            for extractor in ExtractorLists.AVAILABLE_EXTRACTORS:
                if list_to_extract == extractor.NAMESPACE:
                    extractors.append(extractor)

    for Extractor in extractors:
        extractor = Extractor()
        data = extractor.extract()
        updater.update(data, extractor = extractor)


if __name__ == "__main__":
    parser = ArgumentParser(
        prog = "updater.py",
        description = "Download data from different lists and merge them into\
              an output ontology.")

    parser.add_argument("-l",
                        "--lists",
                        dest = "lists",
                        default = ["all"],
                        choices = ["all"] + list(ExtractorLists.EXTRACTORS_BY_NAMES.keys()),
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
