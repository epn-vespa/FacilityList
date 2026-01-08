"""
n2yo scraps the n2yo webpage (space category) and stores data into a dictionary.
The output dictionary is compatible with the ontology mapping (see graph.py).
This list provides a more precise launch date than IMCCE and most lists.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import re

from bs4 import BeautifulSoup
from datetime import datetime
from graph import entity_types
from graph.extractor.cache import CacheManager
from graph.extractor.data_fixer import fix
from graph.extractor.extractor import Extractor
from rdflib import Graph
from utils.dict_utilities import merge_into

from config import DATA_DIR


class N2yoExtractor(Extractor):
    
    URL = "https://www.n2yo.com/satellites/"#?c=26&p=" # 0 (min)

    # URI to save this source as an entity
    URI = "n2yo_list"

    # URI to save entities from this source
    NAMESPACE = "n2yo"

    # Folder name to save cache/ and data/
    CACHE = "N2YO/"

    # Default type used for all unknown types in this resource
    DEFAULT_TYPE = entity_types.SPACECRAFT



    # List's types.
    # For merging strategies. Prevent merging data from lists
    # that do not have types in common
    POSSIBLE_TYPES = {entity_types.SPACECRAFT}

    # No need to disambiguate the type with LLM.
    # Useful for merging strategy: when the type is ambiguous,
    # it is recommanded to not discriminate on types.
    # 1: always known.
    # 0.5: partially known (see individuals)
    # 0: never known.
    TYPE_KNOWN = 1


    FACILITY_ATTRS = {"Name": "label",
                      "NORAD ID": "NORAD_ID",
                      "Int'l Code": "NSSDCA_ID",
                      "Launch date": "launch_date",
                      "Period": "satellite_period" # minutes
    }

    def __init__(self):
        pass


    def __str__(self):
        return self.NAMESPACE


    def extract(self,
                from_cache: bool = True) -> dict:
        """
        Extract the page content into a dictionary.
        """
        params = {
            "c": "26",
            "p": 0
        }

        results = dict()

        # The first label that is encountered is the pref label
        labels_by_nssdca = dict()

        while True:
            data_str =  "c" + params["c"] + '_p' + str(params["p"])
            response = CacheManager.get_page(self.URL,
                                             from_cache = from_cache,
                                             list_name = self.CACHE,
                                             params = params,
                                             data_str = data_str)
            params['p'] += 1
            #if params['p'] == 4:
            #    break # For dev

            # Fix response format
            response = response.replace("</thead></tr>", "</tr></thead>")

            soup = BeautifulSoup(response, "html.parser")
            table = soup.find("table", id = "categoriestab")
            titles = table.find("thead").find_all("a")
            titles = [title.text for title in titles]
            lines = table.find_all("tr")[1:]
            if not lines:
                break
            empty = True
            label = None
            nssdca = None
            for line in lines:
                data = dict()
                values = line.find_all("td")
                if not values:
                    continue
                empty = False
                data["type"] = self.DEFAULT_TYPE
                values = [value.text for value in values]
                for attr, value in zip(titles, values):
                    if value == '-':
                        continue
                    to_merge_with = None
                    attr = self.FACILITY_ATTRS[attr]
                    if attr == "NSSDCA_ID":
                        nssdca = value
                        if value in labels_by_nssdca:
                            to_merge_with = labels_by_nssdca[value]
                    elif attr == "label":
                        label = value
                    elif attr == "launch_date":
                        dt = datetime.strptime(value, "%B %d, %Y")
                        value = dt.date().isoformat()
                    elif attr == "satellite_period":
                        value += "min"
                    data[attr] = value
                if to_merge_with:
                    # They have the same NSSDCA_ID.
                    merge_into(results[to_merge_with], data)
                else:
                    # First encounter
                    labels_by_nssdca[nssdca] = label
                    results[label] = data

            if empty:
                break
        return results