"""
SpaseExtractor scraps the SPASE github and stores data into a dictionary.
The output dictionary is compatible with the ontology mapping (see graph.py).

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from pathlib import Path
from typing import Set
from collections import defaultdict
from graph import entity_types
from graph.extractor.cache import CacheManager
from graph.extractor.extractor import Extractor
from graph.extractor import data_fixer
from llm.llm_connection import LLMConnection
from utils.performances import timeall
from utils.string_utilities import clean_string, has_cospar_nssdc_id
from utils.dict_utilities import extract_items, merge_into, UnionFind
from utils.location_utilities import distance
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
    URI = "spase_list"

    # Folder name to save cache/ and data/
    CACHE = "SPASE/"

    # URI to save entities from this source
    NAMESPACE = "spase"

    # The Deprecated attribute will not be added
    # to SPASE when merging old and new versions of entities
    # together.
    MULTI_VERSIONING = True

    # Default type used for all unknown types in this resource
    DEFAULT_TYPE = entity_types.OBSERVATION_FACILITY

    # List's types.
    # For merging strategies. Prevent merging data from lists
    # that do not have types in common
    POSSIBLE_TYPES = {entity_types.GROUND_OBSERVATORY,
                      entity_types.INVESTIGATION,
                      entity_types.TELESCOPE,
                      entity_types.AIRBORNE,
                      entity_types.SPACECRAFT}

    # No need to disambiguate the type with LLM.
    # Useful for merging strategy: when the type is ambiguous,
    # it is recommanded to not discriminate on types.
    # 1: always known.
    # 0.5: partially known (see individuals)
    # 0: never known.
    TYPE_KNOWN = 0

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
                      "Elevation": "altitude",
                      "StartDate": "start_date",
                      "EndDate": "end_date",
                      "PriorIDs": "prior_id",
                      "PriorID": "prior_id",
                      "InstrumentType": "instrument_type"}



    # The /Observatory's parent folders names associated with the type
    # of their entries (this might not be accurate. Use to fasten
    # mapping to have typed entities.)
    TYPES_BY_FOLDER = {"ASWS/Ground": entity_types.GROUND_OBSERVATORY,
                       "ASWS/Satellite": entity_types.SPACECRAFT,
                       "CNES": [entity_types.INVESTIGATION, entity_types.SPACECRAFT],
                       "ESA": entity_types.SPACECRAFT,
                       "HamSCI": entity_types.GROUND_OBSERVATORY, # OBSERVATORY_NETWORK
                       "ISWI": entity_types.GROUND_OBSERVATORY, # GIRO.json: should be OBSERVATORY NETWORK
                       "IUGONET": entity_types.GROUND_OBSERVATORY,
                       "NASA": entity_types.SPACECRAFT,
                       "NOAA": entity_types.SPACECRAFT,
                       "NSF": entity_types.GROUND_OBSERVATORY,
                       "SMWG/Observatory/AUGSBURG": entity_types.GROUND_OBSERVATORY,
                       "SMWG/Observatory/BARREL": entity_types.AIRBORNE, # TODO SMWG has many different objects
                       }


    def __init__(self):
        pass


    def __str__(self):
        return self.NAMESPACE


    def extract(self,
                from_cache: bool = True) -> dict:
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

        # Same entities with different sources (only for the 210MM cases)
        to_merge = defaultdict(list) # {"short-id": ["long-id1", "long-id2"]}

        for file in files:
            with open(file, "r") as f:
                content = f.read()

            if not content:
                continue

            dict_content = json.loads(content)

            data = dict()
            alt_labels = set()

            prior_id = ""
            launch_year = ""

            for rel, values in extract_items(dict_content):
                if rel not in self.FACILITY_ATTRS:
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
                    if key in ["start_date", "end_date"]:
                        if value.startswith("2000-01-01"):
                            continue # Default date, ignore
                    if key == "label":
                        source = file.split('/')[-2]
                        if (value.startswith("210MM") and "MM210" == source or
                            value.startswith(source)):
                            if "station" in value:
                                alt_labels.add(value)
                                to_merge[value[len(source):].strip()].append(value)
                        data[key] = value
                    elif key == "alt_label":
                        ok, nssdc_id, launch_date = has_cospar_nssdc_id(value)
                        if ok:
                            data["NSSDCA_ID"] = nssdc_id
                            data["COSPAR_ID"] = nssdc_id
                            launch_year = launch_date
                        else:
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

            # launch date
            if launch_year and "launch_date" not in data:
                data["launch_date"] = launch_year

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
            #data["type"] = self.DEFAULT_TYPE
            #data["type_confidence"] = 0
            self._get_type(data)


        # Merge entities on label
        for short_label, long_labels in to_merge.items():
            i = 0
            ll = long_labels[i]
            while ll not in result:
                i += 1
                if i >= len(long_labels) + 1:
                    break
                ll = long_labels[i]
            if ll not in result:
                continue
            for long_label in long_labels[i+1:]:
                if long_label in result and ll in result:
                    merge_into(result[ll], result[long_label])
                    del result[long_label]
            if ll in result and short_label != ll:
                result[short_label] = result[ll]
                result[short_label]["label"] = short_label
                if ll in result:
                    del result[ll]

        data_fixer.fix(result, self)

        # Merge duplicate entities
        new_references = self.self_merge(result)

        # Restore hasPart & isPartOf references with the new references
        for label, data in result.items():
            parts = data.get("has_part", [])
            for i, part in parts:
                if part in new_references:
                    parts[i] = new_references[part]
            parts = data.get("is_part_of", [])
            for i, part in enumerate(parts):
                if part in new_references:
                    parts[i] = new_references[part]

        # Set the inverse relations of hasPart & isPartOf
        for label, data in result.items():
            parts = data.get("has_part", [])
            for part in parts:
                if "is_part_of" in result[part]:
                    result[part]["is_part_of"].append(label)
                else:
                    result[part]["is_part_of"] = [label]
            parts = data.get("is_part_of", [])
            for part in parts:
                if "has_part" in result[part]:
                    result[part]["has_part"].append(label)
                else:
                    result[part]["has_part"] = [label]

        return result


    def _create_entity_from_missing_id(self,
                                       identifier: str) -> dict:
        """
        Creates a data dictionary for an identifier that does not exist in the
        SPASE database but should still be added to the ontology.

        Args:
            identifier: the ID of the entity in SPASE
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

        Args:
            folder: the root folder
            visited_folders: prevent looping in the tree
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


    @timeall
    def _get_type(self,
                  data: dict):
        """
        Add the type of the entity to the data dictionary
        """
        location_space = [
            "Asteroid",
            "Comet",
            "Earth.Magnetosheath",
            "Earth.Magnetosphere",
            "Earth.Magnetosphere.Magnetotail",
            "Earth.Magnetosphere.Main",
            "Earth.Magnetosphere.Polar",
            "Earth.Magnetosphere.RadiationBelt",
            "Earth.Moon",
            "Earth.NearSurface",
            "Earth.NearSurface.Atmosphere",
            "Earth.NearSurface.AuroralRegion",
            "Earth.NearSurface.EquatorialRegion",
            "Earth.NearSurface.Ionosphere",
            "Earth.NearSurface.Ionosphere.DRegion",
            "Earth.NearSurface.Ionosphere.ERegion",
            "Earth.NearSurface.Ionosphere.FRegion",
            "Earth.NearSurface.Ionosphere.Topside",
            "Earth.NearSurface.Mesosphere",
            "Earth.NearSurface.Plasmasphere",
            "Earth.NearSurface.PolarCap",
            "Earth.NearSurface.Stratosphere",
            "Earth.NearSurface.Thermosphere",
            "Heliosphere",
            "Heliosphere.Inner",
            "Heliosphere.NearEarth",
            "Heliosphere.Outer",
            "Heliosphere.Remote1AU",
            "Jupiter",
            "Jupiter.Magnetosphere",
            "Jupiter.Magnetosphere.Magnetotail",
            "Mars",
            "Mars.Phobos",
            "Mercury",
            "Pluto",
            "Saturn",
            "Saturn.Magnetosphere",
            "Sun",
            "Sun.Chromosphere",
            "Sun.Corona",
            "Sun.TransitionRegion",
            "Venus"
        ]
        location_ground = [
            "Earth.Surface",
            "Earth"
        ]
        if data.get("location", None) is None:
            if "NumericalData" in data["code"]:
                data["type"] = entity_types.SURVEY
            else:
                data["type"] = entity_types.INSTRUMENT
            # TODO elif "Instrument" in data["code"]: else: default type (obs facility) & type_confidence to 0
            # (right now only one case has nor Instrument neither NumericalData, but it is an instrument so
            # it will be covered by 'else'.)
            return
        for l in data["location"]:
            if l in location_ground:
                data["type"] = entity_types.GROUND_FACILITY
                break
            elif l in location_space:
                data["type"] = entity_types.SPACE_FACILITY
                break
            else:
                # Still SPACE_FACILITY because there is a location.
                print("Warning: new unspecified location in spase:", data["location"])
                print("Please add it to space_extractor")
                data["type"] = self.DEFAULT_TYPE
        data["type_confidence"] = 1


    def self_merge(self,
                   result: dict) -> dict:
        """
        Merge spase entities that are the same but come from
        different catalogs.

        Returns:
            the old label with the new label to use instead
        """
        replacement_labels = dict()
        uf = UnionFind()
        groups = defaultdict(set)
        for label1, data1 in result.items():
            for label2, data2 in result.items():
                if label1 >= label2:
                    continue
                # Filters
                word1 = label1.replace('-', ' ').split(' ')[0]
                word2 = label2.replace('-', ' ').split(' ')[0]
                if word1 != word2:
                    continue
                if "NSSDCA_ID" in data1 and "NSSDCA_ID" in data2:
                    if data1["NSSDCA_ID"] != data2["NSSDCA_ID"]:
                        continue
                if "latitude" in data1 and "latitude" in data2 and "longitude" in data1 and "longitude" in data2:
                    latitude1 = data1["latitude"]
                    longitude1 = data1["longitude"]
                    latitude2 = data2["latitude"]
                    longitude2 = data2["longitude"]
                    if type(latitude1) == list:
                        latitude1 = latitude1[0]
                        longitude1 = longitude1[0]
                    if type(latitude2) == list:
                        latitude2 = latitude2[0]
                        longitude2 = longitude2[0]
                    dist = distance((latitude1, longitude1), (latitude2, longitude2))
                    if dist < 0.3:
                        uf.union(label1, label2)
                elif "start_date" in data1 and "start_date" in data2:
                    if data1["start_date"] == data2["start_date"]:
                        if label1.lower().replace('-', ' ') == label2.lower().replace('-', ' '): # Only if equal alphanum values
                            uf.union(label1, label2)

        for label in result:
            groups[uf.find(label)].add(label)
            # Also merge on alt labels
            for alt_label in result[label].get("alt_label", []):
                if alt_label in result:
                    # groups[uf.find(label)].add(alt_label)
                    uf.union(label, alt_label)
        for synset in groups.values():
            if len(synset) <= 1:
                continue

            # keep the longest label
            synset = sorted(synset, key=len, reverse=True)
            longest = synset[0]
            data1 = result[longest]

            for label2 in synset[1:]:
                merge_into(data1, result[label2])
                del result[label2]
                replacement_labels[label2] = longest
        return replacement_labels


if __name__ == "__main__":
    pass
