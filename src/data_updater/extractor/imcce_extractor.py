"""
ImcceExtractor scraps the IMCCE API and stores data into a dictionary.
The output dictionary is compatible with the ontology mapping (see graph.py).

Acknowledgement:
    The Sso name search is powered by LTE's SsODNet.quaero REST API.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from config import DATA_DIR # type: ignore
from data_updater import entity_types
from data_updater.extractor.cache import CacheManager
from data_updater.extractor.extractor import Extractor
from utils.utils import has_cospar_nssdc_id
import time
import json
from urllib.parse import urlencode


class ImcceExtractor(Extractor):
    URL = "https://api.ssodnet.imcce.fr/quaero/1/sso/search?"

    # URI to save this source as an entity
    URI = "IMCCE_list"

    # URI to save entities from this source
    NAMESPACE = "imcce"

    # Folder name to save cache/ and data/
    CACHE = "IMCCE/"

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


    ATTRS = {"id": "code",
             "name": "label",
             "type": "type",
             "parent": "parent",
             "updated": "modified",
             "aliases": "alt_label",
             "links": {"self": "uri"}}


    def extract(self,
                from_cache: bool = True) -> dict:
        """
        Extract the page content into a dictionary.
        """
        params = {
            "q": "type:Spacecraft",
            "limit": 100,
            "offset": 0
        }

        results = []

        while True:
            data_str =  params["q"] + '_' + str(params["offset"]) + '-' + str(params["offset"] + params["limit"])
            response = CacheManager.get_page(self.URL,
                                             from_cache = from_cache,
                                             list_name = self.CACHE,
                                             params = params,
                                             data_str = data_str)#url=self.URL, params=params
            if not response:
                break # last page
            response = json.loads(response)
            if len(response) == 0:
                break # last page
            results.extend(response["data"])
            params["offset"] += params["limit"]

            # if parent == "Neptune":
            #   ignore (because Neptune is default)

            # ephemeris : whether the position in space is known
            # updated: for the VersionManager

        result = dict()
        for elem in results:
            data = dict()
            for key, value in elem.items():
                if key == "parent" and value == "Neptune":
                    # Neptune is used as a default value currently
                    continue
                elif key == "links":
                    for key2, value2 in value.items():
                        data[self.ATTRS["links"][key2]] = value2
                    continue
                elif key == "aliases":
                    for v in value:
                        ok, cospar_ids, launch_dates = has_cospar_nssdc_id(v)
                        if ok:
                            for cospar_id, launch_date in zip(cospar_ids, launch_dates):
                                data["launch_date"] = launch_date
                                data["COSPAR_ID"] = cospar_id
                                data["NSSDCA_ID"] = cospar_id
                   
                if key in self.ATTRS:
                    data[self.ATTRS[key]] = value
            result[data["label"]] = data
        return result