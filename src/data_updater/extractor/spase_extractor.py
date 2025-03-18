"""
SpaseExtractor scraps the SPASE webpage and stores data into a dictionary.
The output dictionary is compatible with the ontology mapping (see graph.py).

Troubleshooting:
    SPASE does not allow scrapping (even with selenium).
    We extract data from https://hpde.io/SMWG/Observatory/index.html
    instead. ** SMWG: SPASE Metadata Working Group **

    For SPASE original data, we can use the previously downloaded data
    from the /data/SPASE folder.

    In some descriptions, there are tables (like Orbital parameters, etc)
    that could be extracted as entity's attributes or as hasPart relation.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from typing import List, Set
from bs4 import BeautifulSoup
from extractor.cache import CacheManager
import json
from utils import clean_string, extract_items
import re
import os


class SpaseExtractor():

    # URL = "https://heliophysicsdata.gsfc.nasa.gov/websearch/dispatcher?action=CDAW_ELEMENT_LIST_PANE_ACTION&element="
    # URL = "https://hpde.io/SMWG/Observatory/index.html" # first version of the code (2025.03.14)
    # URL = "https://github.com/spase-group/spase-info/tree/master/SMWG/Observatory/"
    # URL = "https://github.com/spase-group/spase-info/tree/master/" # SMWG/Observatory/
    # URL = "https://github.com/spase-group/spase-info"
    # URL = "https://github.com/hpde/hpde.io/tree/master/"
    URL = "https://github.com/hpde/hpde.io"

    # Name of the folder after git clone
    GIT_REPO = "hpde.io"

    # URI to save this source as an entity
    URI = "SPASE_list"

    # URI to save entities from this source
    NAMESPACE = "spase"

    # Default type used for all unknown types in this resource
    DEFAULT_TYPE = "observation facility"

    # Mapping between PDS xml files and our dictionary format
    FACILITY_ATTRS = {"ResourceID": "code",
                      "ResourceName": "label",
                      "Description": "description",
                      #"URL": "url",
                      "AlternateName": "alt_label",
                      "ObservatoryRegion": "location",
                      "ObservatoryGroupID": "is_part_of",
                      #"Aknowledgement": "",
                      "Latitude": "latitude",
                      "Longitude": "longitude",
                      "StartDate": "start_date",
                      "EndDate": "end_date"}


    def extract(self) -> dict:
        """
        Extract the github content into a dictionary.
        """
        # pull if not exist
        CacheManager.git_pull(self.URL, self.GIT_REPO)

        # get files from the git repo
        files = self._list_files(CacheManager.CACHE + self.GIT_REPO)

        result = dict()

        for file in files:
            with open(file, "r") as f:
                content = f.read()

            if not content:
                continue

            dict_content = json.loads(content)

            data = dict()
            alt_labels = set()

            for key, values in extract_items(dict_content):
                if key not in SpaseExtractor.FACILITY_ATTRS:
                    continue
                key = SpaseExtractor.FACILITY_ATTRS.get(key)
                if type(values) == str:
                    values = [values]
                for value in values:
                    value = clean_string(value)
                    if key == "label":
                        data[key] = value
                    elif key == "alt_label":
                        value = value.replace("Observatory Station Code: ", "")
                        alt_labels.add(value)
                    elif key == "description" and "description" in data:
                            continue # Only one description per entity
                    elif key == "is_part_of":
                        # Save the observatory group
                        observatory_group = value.split('/')[-1]
                        if observatory_group not in result:
                            result[observatory_group] = dict()
                        result[observatory_group]["label"] = observatory_group
                        result[observatory_group]["code"] = value
                        data[key] = observatory_group
                    elif key in data:
                        data[key].append(value)
                    else:
                        data[key] = [value]

            # alt labels
            if alt_labels:
                data["alt_label"] = alt_labels

            # url
            href = self.URL + "/tree/master" + file.split(self.GIT_REPO)[1]
            data["url"] = href

            # label
            if not data["label"]:
                data["label"] = href.split('/')[-1]

            result[data["label"]] = data
        return result


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
                if "Observatory" in folder and file.endswith(".json"):
                    result.add(root + '/' + file)
            for dir in dirs:
                # TODO ignore the Deprecated folder ?
                dir = root + '/' + dir
                if dir not in visited_folders:
                    visited_folders.add(dir)
                    result.update(self._list_files(dir,
                                  visited_folders = visited_folders))
            # return dict()
        return result

if __name__ == "__main__":
    ex = SpaseExtractor()
    ex.extract()