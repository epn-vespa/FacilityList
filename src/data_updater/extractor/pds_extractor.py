"""
PdsExtractor scraps the NASA's PDS webpage and stores data into a dictionary.
The output dictionary is compatible with the ontology mapping (see graph.py).

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
from bs4 import BeautifulSoup
from extractor.cache import CacheManager
from xml.etree import ElementTree as ET
import re

class PdsExtractor():
    # List of documents to scrap
    URL = "https://pds.nasa.gov/data/pds4/context-pds4/facility/"

    # URI to save this source as an entity
    URI = "NASA-PDS_list"

    # URI to save entities from this source
    NAMESPACE = "pds"

    # Default type used for all unknown types in this resource
    DEFAULT_TYPE = "observation facility"

    # Mapping between PDS xml files and our dictionary format
    FACILITY_ATTRS = {"logical_identifier": "code",
                      "name": "label"}

    if __name__ == "__main__":
        pass

    def extract(self) -> dict:
        """
        Extract the page content into a dictionary.
        """
        content = CacheManager.get_page(PdsExtractor.URL)

        if not content:
            return dict()

        soup = BeautifulSoup(content, "html.parser")
        links = soup.find("div", id = "files").find("table").find_all("a", href = True)

        result = dict()
        for link in links:
            data = dict()
            href = link["href"]
            # laboratory & observatory
            cat = ""
            if href.startswith("laboratory."):
                cat = "laboratory"
            elif href.startswith("observatory."):
                cat = "observatory"
            if not cat:
                continue

            # Download XML file for href
            resource_url = PdsExtractor.URL + href
            content = CacheManager.get_page(resource_url)

            # Remove <Product_Context> attributes from XML
            content = re.sub(r"<Product_Context[^>]*>",
                             "<Product_Context>",
                             content,
                             flags = re.DOTALL)

            # Parse XML file
            root = ET.fromstring(content)

            # Internal references
            codes = [x.text for x in root.findall(".//lid_reference")]
            data["code"] = codes

            # Other tags
            facility = root.find(".//Facility")
            tags = facility.findall('*')
            for tag in tags:
                tag_str = tag.tag
                tag_str = PdsExtractor.FACILITY_ATTRS.get(tag_str, tag_str)
                data[tag_str] = re.sub("[\n ]+", " ", tag.text.strip())

            data["url"] = resource_url

            # label
            label = root.find(".//title").text.strip()
            if "label" not in data:
                data["label"] = label

            result[label] = data
        return result

if __name__ == "__main__":
    pass
