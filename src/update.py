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
import setup_path # Import first

import atexit
from rdflib import Namespace
from typing import List
from argparse import ArgumentParser
import os
import sys

from tqdm import tqdm
from graph import Graph
from data_updater import entity_types
from data_updater.extractor.cache import VersionManager
from data_updater.extractor.extractor import Extractor
from data_updater.extractor.extractor_lists import ExtractorLists
from data_updater.extractor.aas_extractor import AasExtractor
from data_updater.extractor.iaumpc_extractor import IauMpcExtractor
from data_updater.extractor.imcce_extractor import ImcceExtractor
from data_updater.extractor.naif_extractor import NaifExtractor
from data_updater.extractor.nssdc_extractor import NssdcExtractor
from data_updater.extractor.pds_extractor import PdsExtractor
from data_updater.extractor.spase_extractor import SpaseExtractor
from data_updater.extractor.wikidata_extractor import WikidataExtractor

from config import CACHE_DIR # type: ignore

from utils.utils import get_location_info


class Updater():


    def __init__(self,
                 ontology_file: str = "",
                 output_ontology: str = "",
                 lists: list[str] = []):
        """
        ontology_file -- input ontology (will be merged with this ontology)
        output_ontology -- filename of the output ontology
        lists -- lists to extract
        """
        self._graph = Graph(ontology_file)
        if not ontology_file:
            self.init_graph() # Create basic classes
        self._output_ontology = output_ontology
        self._description = f"script: {os.path.basename(sys.argv[0])} {' '.join(sys.argv[1:])}\n" + \
            f"input ontology: {ontology_file}\n" + \
            f"filename: {output_ontology}\n" + \
            f"lists: {' '.join(lists)}"


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
        # Remove the old entities for the extractor in order to update the source.
        if extractor:
            namespace_uri = Namespace(str(self.graph.OM.OBS)[:-1] + "/" + extractor.NAMESPACE + "#")
            for triple in self.graph:
                if triple[0].startswith(namespace_uri):
                    self.graph.remove(triple)

        for identifier, features in tqdm(data.items(), desc = f"Add entities to ontology"):
            # Get complete location information and add them to the features
            if extractor: # Only for extracted entities
                location_info = dict()
                ent_type = features.get("type", None)
                part_of = None

                if ent_type is not None and "is_part_of" in features:
                    if type(features["is_part_of"]) == list:
                        part_of_uri = None
                        for p in features["is_part_of"]:
                            if p:
                                part_of_uri = p
                                if part_of_uri not in data:
                                    continue
                                part_of_type = data[part_of_uri].get("type", [])
                                if type(part_of_type) == str:
                                    part_of_type = [part_of_type]
                                for po_t in part_of_type:
                                    if po_t in entity_types.MAY_HAVE_ADDR:
                                        part_of = data[part_of_uri]
                                        break
                            if part_of:
                                break # Keep the first non-none part-of only
                    elif type(features["is_part_of"]) == str:
                        # Wikidata
                        part_of = features["is_part_of"]

                if type(ent_type) == str:
                    ent_type = {ent_type}
                if ent_type is not None:
                    for cat in ent_type:
                        if (# cat == entity_types.GROUND_OBSERVATORY or
                            cat in entity_types.MAY_HAVE_ADDR and (
                            "latitude" in features and "longitude" in features or
                            "location" in features # or
                            #part_of is not None or
                            )):

                            location_info = get_location_info(label = features.get("label", None),
                                                              latitude = features.get("latitude", None),
                                                              longitude = features.get("longitude", None),
                                                              address = features.get("address", None),
                                                              location = features.get("location", None),
                                                              part_of = part_of)
                            break
                # Retrieved information include country, continent. We also set location to
                # Earth or Space.
                for key, value in location_info.items():
                    if key not in features or features[key] is None:
                        features[key] = value

            # Add triple <subj, pred, obj>
            subj = identifier

            for predicate, obj in features.items():
                self.graph.add((subj, predicate, obj),
                                extractor = extractor)

            # Add type on non-typed entities
            if "type" not in features:
                # If an entity has a default type, use it as the entity's type
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
                A: {"label": A, "alliance": "IVOA"},
                H: {"label": H, "alliance": "IHDEA"},
                G: {"label": G, "alliance": "OGC"},
                P: {"label": P, "alliance": "IPDA"},
                O: {"label": O}}

        SOURCES = {
            AasExtractor.URI: {"url": AasExtractor.URL,
                               "community": A,
                               "is_authoritative_for": A},
            IauMpcExtractor.URI: {"url": IauMpcExtractor.URL,
                                  "community": [A, P],
                                  "is_authoritative_for": [A, P]},
            ImcceExtractor.URI: {"url": ImcceExtractor.URL,
                                 "community": [A, H, P],
                                 "is_authoritative_for": [A, H, P]},
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
                                    "community": [A, H, G, P, O]},
            NssdcExtractor.URI: {"url": NssdcExtractor.URL,
                                 "community": [A, H, P, O],
                                 "is_authoritative_for": [A, H, P, O]}} # Not authoritative

        # TODO add other sources (can have more than one community)
        # every time we create an extraction script for the source.

        self.update(COMMUNITIES, cat = "community")
        self.update(SOURCES, cat = "facility list")


    def write(self):
        """
        Serialize the updated graph into the output ontology file.
        """
        self.graph.add_metadata(description = self._description)
        with open(self.output_ontology, 'wb') as file:
            file.write(self.graph.serialize(format = "turtle",
                                            encoding = "utf-8"))
        print(f"Ontology saved in {self.output_ontology}")


def main(lists: List[str],
         input_ontology: str = "",
         output_ontology: str = "output.ttl",
         from_cache: bool = True):
    updater = Updater(input_ontology,
                      output_ontology,
                      lists)
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
        data = extractor.extract(from_cache = from_cache)
        VersionManager.compare_versions(data, extractor)
        updater.update(data, extractor = extractor)


if __name__ == "__main__":
    parser = ArgumentParser(
        prog = "update.py",
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
    parser.add_argument("-c",
                        "--no-cache",
                        dest = "no_cache",
                        action = "store_true",
                        help = "If set, will download cache again and compare versions.")

    args = parser.parse_args()
    main(args.lists,
         args.input_ontology,
         args.output_ontology,
         not args.no_cache)
