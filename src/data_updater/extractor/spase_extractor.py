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

from typing import List
from bs4 import BeautifulSoup
from extractor.cache import CacheManager
import json
from utils import clean_string, extract_items

class SpaseExtractor():

    # URL = "https://heliophysicsdata.gsfc.nasa.gov/websearch/dispatcher?action=CDAW_ELEMENT_LIST_PANE_ACTION&element="
    # URL = "https://hpde.io/SMWG/Observatory/index.html" # old version of the code (2025.03.14)
    URL = "https://github.com/spase-group/spase-info/tree/master/SMWG/Observatory/"

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
                      "ObservatoryGroupId": "part_of",
                      #"Aknowledgement": "",
                      "Latitude": "latitude",
                      "Longitude": "longitude",
                      "StartDate": "start_date",
                      "EndDate": "end_date"}

    def _get_all_links(self,
                      url: str) -> List:
        """
        Get a list of all the content links (json files) from the
        SPASE github repository, recursively.
        """
        links = set()
        content = CacheManager.get_page(url)
        if not content:
            return links
        soup = BeautifulSoup(content, "html.parser")
        a = soup.find_all("a", attrs = {"class":"Link--primary"})
        hrefs = list(set([href.attrs["href"] for href in a]))
        for href in hrefs:
            if href.endswith(".json"):
                href = href.replace("blob", "refs/heads")
                href = "https://raw.githubusercontent.com" + href
                links.add(href)
            elif href.endswith(".html"):
                continue # Ignore HTML pages to take json instead
            elif href.endswith(".xml"):
                continue
            else:
                # Is an index page
                href = "https://github.com" + href
                links.update(self._get_all_links(href))
        return links

    def extract(self) -> dict:
        """
        Extract the page content into a dictionary.
        """
        result = dict()
        hrefs = self._get_all_links(SpaseExtractor.URL)
        if not hrefs:
            return dict()

        for href in hrefs:
            content = CacheManager.get_page(href)
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
                    elif key in data:
                        data[key].append(value)
                    else:
                        data[key] = [value]

            # alt labels
            if alt_labels:
                data["alt_label"] = alt_labels

            # label
            if not data["label"]:
                data["label"] = href.split('/')[-1]
            result[data["label"]] = data

            # url
            data["url"] = href
        return result

if __name__ == "__main__":
    ex = SpaseExtractor()
    ex.extract()
