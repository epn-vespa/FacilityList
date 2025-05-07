"""
SpaseExtractor scraps the SPASE github and stores data into a dictionary.
The output dictionary is compatible with the ontology mapping (see graph.py).

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from pathlib import Path
from typing import Set
from data_updater import entity_types
from data_updater.extractor.cache import CacheManager
from data_updater.extractor.extractor import Extractor
from utils.llm_connection import LLM
from utils.utils import clean_string, extract_items, merge_into
import json
import re
import os

from config import CACHE_DIR # type: ignore


class SpaseExtractor(Extractor):

    # Other URLs
    # URL = "https://heliophysicsdata.gsfc.nasa.gov/websearch/dispatcher?action=CDAW_ELEMENT_LIST_PANE_ACTION&element="
    # URL = "https://hpde.io/SMWG/Observatory/index.html" # first version of the code (2025.03.14)
    # URL = "https://github.com/spase-group/spase-info/tree/master/SMWG/Observatory/"
    # URL = "https://github.com/spase-group/spase-info/tree/master/" # 2nd version of the code: without git pull (2025.03.14)
    # URL = "https://github.com/spase-group/spase-info"
    # URL = "https://github.com/hpde/hpde.io/tree/master/"
    URL = "https://github.com/hpde/hpde.io" # 3rd version of the code: with git pull (2025.03.18)

    # Name of the folder after git clone
    GIT_REPO = "hpde.io"

    # URI to save this source as an entity
    URI = "SPASE_list"

    # Folder name to save cache/ and data/
    CACHE = "SPASE/"

    # URI to save entities from this source
    NAMESPACE = "spase"

    # Default type used for all unknown types in this resource
    DEFAULT_TYPE = entity_types.GROUND_OBSERVATORY

    # List's types.
    # For merging strategies. Prevent merging data from lists
    # that do not have types in common
    POSSIBLE_TYPES = {entity_types.GROUND_OBSERVATORY,
                      entity_types.MISSION,
                      entity_types.OBSERVATORY_NETWORK,
                      entity_types.TELESCOPE,
                      entity_types.AIRBORNE,
                      entity_types.SPACECRAFT}

    # Folders that we want to keep must contain
    KEEP_FOLDER = "Observatory"

    # Mapping between PDS xml files and our dictionary format
    FACILITY_ATTRS = {"ResourceID": "code",
                      "ResourceName": "label",
                      "Description": "description",
                      #"URL": "url",
                      "AlternateName": "alt_label",
                      "ObservatoryRegion": "location",
                      "ObservatoryGroupID": "is_part_of",
                      "Agency": "funding_agency",
                      #"Aknowledgement": "",
                      "Latitude": "latitude",
                      "Longitude": "longitude",
                      "StartDate": "start_date",
                      "EndDate": "end_date",
                      "PriorIDs": "prior_id",
                      "PriorID": "prior_id"}


    def __init__(self):
        pass


    def __str__(self):
        return self.NAMESPACE


    def extract(self) -> dict:
        """
        Extract the github content into a dictionary.
        """
        # pull if not exist
        CacheManager.git_pull(self.URL,
                              self.GIT_REPO,
                              list_name = self.CACHE)

        # get files from the git repo that are Observatory .json
        files = self._list_files(str(CACHE_DIR / self.CACHE / self.GIT_REPO))

        result = dict()

        # Dictionary to save the internal references and replace
        # them by our ontology's ID in the result dict
        # (used with hasPart & isPartOf)
        spase_references_by_id = dict()

        # Dictionary to save the internal PriorIDs and merge the entities
        # when there are the same.
        data_by_prior_id = dict()

        for file in files:
            with open(file, "r") as f:
                content = f.read()

            if not content:
                continue

            dict_content = json.loads(content)

            data = dict()
            alt_labels = set()

            prior_id = ""
            parent_rel = ""
            for rel, values in extract_items(dict_content):
                if rel not in self.FACILITY_ATTRS:
                    parent_rel = rel
                    continue
                key = self.FACILITY_ATTRS.get(rel)
                if type(values) == str:
                    values = [values]
                for value in values:
                    value = clean_string(value)
                    if value == "None":
                        continue
                    if "PriorID" in rel:
                        if "\n" in value or len(value) > 150:
                            key = self.FACILITY_ATTRS["Description"]
                        else:
                            if value[-1] == '.':
                                value = value[:-1] # Remove final '.'
                            prior_id = value
                    if key == "longitude":
                        # Remove final 'E' (East) to have a valid float value
                        if value[-1] == 'E':
                            value = value[:-1]
                        value = float(value)
                    if key == "latitude":
                        value = float(value)
                    if key == "description":
                        if value.startswith("includes observatory/station name,"):
                            continue # Ignore this description

                    if key == "label":
                        data[key] = value
                    elif key == "alt_label":
                        value = value.replace("Observatory Station Code: ", "")
                        alt_labels.add(value)
                    #elif key == "description" and "description" in data:
                    #    continue # Only one description per entity
                    elif key in data:
                        data[key].append(value)
                    else:
                        data[key] = [value]
            if "code" in data:
                spase_references_by_id[data["code"][0]] = data["label"]

            # alt labels
            if alt_labels:
                data["alt_label"] = alt_labels

            # url
            href = self.URL + "/tree/master" + file.split(self.GIT_REPO)[1]
            data["url"] = href

            # label
            if "label" not in data or not data["label"]:
                data["label"] = href.split('/')[-1]

            if prior_id:
                data_by_prior_id[prior_id] = data["label"]

            if data["label"] not in result:
                result[data["label"]] = data
            else:
                merge_into(result[data["label"]], data)

        # Merge entities on prior_id
        for prior_id, newer_id in data_by_prior_id.items():
            # Find in results the data for which id is the prior id (to remove)
            for label, data in result.copy().items():
                if "code" in data and prior_id in data["code"]:
                    merge_into(result[newer_id], data)
                    del result[label]
        # If the SPASE id of a part does not exists in the
        # extracted data, create a new entity with this
        # identifier.
        spase_missing_ids = dict()
        for key, value in result.items():
            if "has_part" in value:
                for i, part in enumerate(value["has_part"]):
                    if part in spase_references_by_id:
                        value["has_part"][i] = spase_references_by_id[part]
                    else:
                        # Create the entity from the missing id.
                        if part not in spase_missing_ids:
                            data = self._create_entity_from_missing_id(part)
                            if data["label"] == "individual.none":
                                # Some entities refer to None
                                value["has_part"][i] = None
                                continue
                            spase_missing_ids[part] = data
                        value["has_part"][i] = spase_missing_ids[part]["label"]
            if "is_part_of" in value:
                deleted = 0
                for i, part in enumerate(value["is_part_of"].copy()):
                    if part in spase_references_by_id:
                        reference_label = spase_references_by_id[part]
                        if reference_label not in result:
                            del value["is_part_of"][i-deleted]
                            deleted += 1
                        else:
                            value["is_part_of"][i-deleted] = spase_references_by_id[part]
                    else:
                        # Create the entity from the missing id.
                        if part not in spase_missing_ids:
                            data = self._create_entity_from_missing_id(part)
                            if data["label"] == "individual.none":
                                # Remove entities that refer to None
                                del value["is_part_of"][i-deleted]
                                deleted += 1
                                continue
                            spase_missing_ids[part] = data
                        value["is_part_of"][i-deleted] = spase_missing_ids[part]["label"]

        # If a SPASE id is missing, add an artificial entity for this code
        for key, value in spase_missing_ids.items():
            result[value["label"]] = value

        # Get types
        for data in result.values():
            self._get_type(data)
        return result


    def _create_entity_from_missing_id(self,
                                       identifier: str) -> dict:
        """
        Creates a data dictionary for an identifier that does not exist in the
        SPASE database but should still be added to the ontology.

        Keyword arguments:
        identifier -- the ID of the entity in SPASE
        """
        data = dict()
        # Remove .json & add "/"
        label = identifier.split(self.KEEP_FOLDER)[1]
        label = label.split(".json")[0]
        label = re.sub(r"[_/]", " ", label).strip()
        data = {"label": label,
                "code": identifier}
        return data


    def _list_files(self,
                    folder: str,
                    visited_folders: Set = None) -> Set:
        """
        Get the list of paths recursively in the folder.
        Only return .json files located in any Observatory folder.

        Keyword arguments:
        folder -- the root folder
        visited_folders -- prevent looping in the tree
        """
        result = set()
        if visited_folders is None:
            visited_folders = set()
        for root, dirs, files in os.walk(folder):
            for file in files:
                if self.KEEP_FOLDER in folder and file.endswith(".json"):
                    result.add(str(Path(root) /  file))
            for dir in dirs:
                # TODO ignore the Deprecated folder ?
                dir = str(Path(root) / dir)
                if dir not in visited_folders:
                    visited_folders.add(dir)
                    result.update(self._list_files(dir,
                                  visited_folders = visited_folders))
            # return dict()
        return result

    def _get_type(self,
                  data: dict):
        """
        Add the type of the entity to the data dictionary
        """
        location_space = [
            "Earth.Magnetosphere",
            "Earth.Magnetosphere.Polar",
            "Earth.Magnetosphere.Magnetotail"
            "Earth.Magnetosphere.Main",
            "Earth.Magnetosheath",
            "Earth.NearSurface",
            "Earth.NearSurface.AuroralRegion",
            "Earth.NearSurface.EquatorialRegion",
            "Earth.NearSurface.Ionosphere",
            "Earth.NearSurface.PolarCap",
            "Heliosphere.NearEarth",
        ]
        location_ground = [
            "Earth.Surface",
            "Earth"
        ]
        choices = None # None choices will disambiguate for all categories.
        if "latitude" in data and "longitude" in data:
            choices = [entity_types.GROUND_OBSERVATORY, entity_types.MISSION,
                       entity_types.OBSERVATORY_NETWORK, entity_types.TELESCOPE]
        elif "location" in data:
            for l in data["location"]:
                if l in location_space:
                    choices = [entity_types.AIRBORNE, entity_types.MISSION,
                               entity_types.SPACECRAFT]
                    break
                if l in location_ground:
                    choices = [entity_types.GROUND_OBSERVATORY, entity_types.MISSION,
                               entity_types.OBSERVATORY_NETWORK, entity_types.TELESCOPE]
                    break

        repr = entity_types.to_string(data, exclude = ["start_date",
                                                       "end_date",
                                                       "code",
                                                       "end_date",
                                                       "url",
                                                       "uri",
                                                       "prior_id"])
        data["type"] = LLM().classify(repr,
                                      choices = choices,
                                      from_cache = True,
                                      cache_key = self.NAMESPACE + '#' + data["label"])

if __name__ == "__main__":
    pass