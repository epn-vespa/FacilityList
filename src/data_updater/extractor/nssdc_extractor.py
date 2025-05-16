"""
NssdcExtractor scraps the NASA's NSSDC webpage and stores data into a dictionary.
The output dictionary is compatible with the ontology mapping (see graph.py).
We make sure to get only entities that are in "ASNO" (Astronomy),
"PSNO" (Earth Science), "SONO" (Solar Physics), "SPNO" (Space Physics).

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from bs4 import BeautifulSoup
from data_updater import entity_types
from data_updater.extractor.cache import CacheManager
from data_updater.extractor.extractor import Extractor
from utils.utils import clean_string


class NssdcExtractor(Extractor):

    # Base URL for queries
    URL = "https://nssdc.gsfc.nasa.gov/nmc/spacecraft/query"

    # URI to save this source as an entity
    URI = "NSSDC_list"

    # URI to save entities from this source
    NAMESPACE = "nssdc"

    # Folder name to save cache/ and data/
    CACHE = "NSSDC/"

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

    # Extracted categories
    DISCIPLINES = {"ASNO": "Astronomy",
                   "PSNO": "Planetary Science",
                   "SONO": "Solar Physics",
                   "SPNO": "Space Physics"}

    # Mapping between NSSDC page and our dictionary format
    FACILITY_ATTRS = {"label": "label",
                      "description":"description",
                      "alternate names": "alt_label",
                      "launch date": "launch_date",
                      "launch site": "launch_place", # launch_place in Wikidata
                      "mass": "mass",
                      "additional information": "ext_ref",
                      "url": "url",
                      "launch vehicle": "launch_vehicle",
                      "funding agency": "funding_agency",
                      "funding agencies": "funding_agency",
                      "disciplines": "discipline",
                      "discipline": "discipline", # Keep disciplines
                      #"nominal power": "nominal_power" # watts (maximum power required by a facility)
                      }

    def extract(self) -> dict:
        """
        Extract the page content into a dictionary.
        """
        result = dict()

        # 1. Get the entities' URL pages
        entities_url = set()

        for discipline in self.DISCIPLINES.keys():
            content = CacheManager.get_page(NssdcExtractor.URL,
                                            self.CACHE,
                                            from_cache = True,
                                            data = {"name": "",
                                                    "discipline": discipline,
                                                    "launch": ""},
                                            data_str = discipline)

            soup = BeautifulSoup(content, "html.parser")
            table = soup.find('table')
            for a_tag in table.find_all('a', href=True):
                entities_url.add(a_tag['href'])

        # 2. Extract pages data
        for entity_url in entities_url:
            entity_url = "https://nssdc.gsfc.nasa.gov" + entity_url
            content = CacheManager.get_page(entity_url,
                                            self.CACHE)
            soup = BeautifulSoup(content, "html.parser")
            maindiv = soup.find("div", {"class": "twocol"})
            left = maindiv.find("div", {"class": "urone"})
            right = maindiv.find("div", {"class": "urtwo"})

            # Get data dict
            data = self._to_dict(left)
            data.update(self._to_dict(right))

            # label
            h1 = soup.find("h1")
            label = clean_string(h1.get_text().replace('\xa0', ' '))
            data["label"] = label

            # code
            p_code = h1.find_next("p")
            code = p_code.find("strong")
            data["code"] = clean_string(code.next_sibling.text.replace('\xa0', ' '))

            # url
            data["url"] = entity_url

            # type
            data["type"] = self.DEFAULT_TYPE
            data["type_confidence"] = 1

            # Ignore entities that have one or more uncompatible discipline
            """
            ignore = False
            for key, value in data.items():
                if key == "disciplines":
                    for discipline in value:
                        if discipline not in self.CAN_INCLUDE:
                            ignore = True
                            break
                    if ignore:
                        break
            if ignore:
                continue # do not add in result
            """
            result[label] = data
        return result

    def _to_dict(self,
                 div) -> dict:
        """
        Transform the page's div into a dictionary.
        """
        key = ""
        data = dict()
        for d in div:
            if d.name == "h2":
                key = d.get_text().replace('\xa0', ' ').rstrip(':').lower()
                elements = []
            else:
                strongs = d.find_all("strong")
                for strong in strongs:
                    key = strong.get_text().replace('\xa0', ' ').rstrip(':').lower()
                    if key not in self.FACILITY_ATTRS:
                        continue
                    value = strong.next_sibling.replace('\xa0', ' ')
                    if value:
                        key = self.FACILITY_ATTRS[key]#.get(key, key)
                        data[key] = clean_string(value)
                        continue
                if strongs:
                    continue
                #if len(d) <= 1:
                #    data[key] = d.get_text().replace('\xa0', ' ')
                if key not in self.FACILITY_ATTRS:
                    continue
                for d2 in d:
                    if d2.name == "li":
                        a = d2.find("a")
                        if a:
                            href = "https://nssdc.gsfc.nasa.gov" + a["href"]
                            elements.append(href)
                        else:
                            value = d2.get_text().replace('\xa0', ' ')
                            elements.append(clean_string(value))
                            continue
                if key and (not key in data or not data[key]):
                    key = self.FACILITY_ATTRS[key]#.get(key, key)
                    if elements:
                        data[key] = elements
                    else:
                        data[key] = clean_string(d.get_text().replace('\xa0', ' '))
        return data


if __name__ == "__main__":
    pass
