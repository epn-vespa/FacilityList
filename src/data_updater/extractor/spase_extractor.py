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

from bs4 import BeautifulSoup
from extractor.cache import CacheManager
import json
from utils import clean_string, extract_items

class SpaseExtractor():

    # URL = "https://heliophysicsdata.gsfc.nasa.gov/websearch/dispatcher?action=CDAW_ELEMENT_LIST_PANE_ACTION&element="
    URL = "https://hpde.io/SMWG/Observatory/index.html"

    BASE_URL = "https://hpde.io/SMWG/Observatory/"

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

    def extract(self) -> dict:
        """
        Extract the page content into a dictionary.
        """

        # content0 = CacheManager.get_page(SpaseExtractor.URL)
        content = CacheManager.get_page(SpaseExtractor.URL)

        if not content:
            return dict()

        soup = BeautifulSoup(content, "html.parser")
        # [1:] to ignore the title's link
        a = soup.find("ul", attrs = {"class":"listview"}).find_all("a")[1:]
        a = [aa.text.strip() for aa in a]
        result = dict()
        base_url = SpaseExtractor.BASE_URL
        for href in a:
            data = dict()
            if not href:
                continue

            # First test if there is a json page
            url = base_url + href + ".json"
            
            content = CacheManager.get_page(url)

            if not content:
                # Try with the html version of the page
                if self._get_data_from_html(base_url + href + ".html", data):
                    # HTML could be scrapped.
                    # We first try to scrap json instead of
                    # HTML due to HTML errors in some pages.
                    result[href] = data
                    continue

                # The page is an index page with new links
                url = base_url + href  + "/index.html"
                content = CacheManager.get_page(url)
                if not content:
                    continue # No json, no html, not an index.html
                soup_index = BeautifulSoup(content, "html.parser")
                a2 = soup_index.find("ul", attrs = {"class":"listview"}).find_all("a")[1:]
                a2 = [href + "/" + aa.text for aa in a2]
                a.extend([aa for aa in a2 if aa not in a])
                continue

            dict_content = json.loads(content)
            # This dictionary is recursive. Get a flat list of (key, value)
            dict_content = extract_items(dict_content)

            alt_labels = set()

            for key, values in dict_content:
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
            if alt_labels:
                data["alt_label"] = alt_labels
            if not data["label"]:
                data["label"] = href.split('/')[-1]
            result[data["label"]] = data
            data["url"] = url
        return result
    
    def _get_data_from_html(self,
                            url: str,
                            data: dict):
        """
        Extract data from the html version of the page.
        Save data into the data dict.
        If the page does not exist, will return False.

        Keyword arguments:
        url -- the url to extract from
        data -- the dict to save the extracted data
        """
        content = CacheManager.get_page(url)
        if not content:
            return False
        soup = BeautifulSoup(content, features = "xml")
        div = soup.find("div", attrs = {"class": "product"})
        cat = div.find("h1").text # Observatory
        key = ""
        value = ""
        alt_labels = set()
        for subdiv in div.find_all("div"):
            if subdiv["class"] == "term":
                key = subdiv.text
                if key not in SpaseExtractor.FACILITY_ATTRS:
                    key = ""
                    continue
                else:
                    key = SpaseExtractor.FACILITY_ATTRS.get(key)
            elif subdiv["class"] == "definition":
                if not key:
                    continue
                value = subdiv.text
                value = clean_string(value)

                if key == "label":
                    data[key] = value
                elif key == "alt_label":
                    value = value.replace("Observatory Station Code: ", "")
                    alt_labels.add(value)
                elif key == "description":
                    if "description" in data:
                        key = ""
                        continue # Only one description per entity
                    else:
                        # Sometimes the <div> for Description is not closed properly.
                        value = value.split("Acknowledgement")[0]
                        value = value.split("Contacts")[0].strip()
                        data[key] = value
                elif key not in data:
                    data[key] = [value]
                else:
                    data[key].append(value)
                key = ""
        if alt_labels:
            data["alt_label"] = alt_labels
        if cat:
            data["type"] = cat
        data["url"] = url
        return True

if __name__ == "__main__":
    ex = SpaseExtractor()
    ex.extract()