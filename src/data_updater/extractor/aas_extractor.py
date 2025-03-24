"""
AasExtractor scraps the AAS webpage and stores data into a dictionary.
The output dictionary is compatible with the ontology mapping (see graph.py).

Troubleshooting:
    Some lines in the data source contain more than one information
    with an "and" or "or" in the label. This is not taken into account yet.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from bs4 import BeautifulSoup
from utils import cut_location
from extractor.cache import CacheManager
from extractor.extractor import Extractor

class AasExtractor(Extractor):
    URL = "https://journals.aas.org/author-resources/aastex-package-for-manuscript-preparation/facility-keywords/"

    # URI to save this source as an entity (obs:AAS_list)
    URI = "AAS_list"

    # URI to save entities from this source
    NAMESPACE = "aas"

    # Folder name to save cache/ and data/
    CACHE = "AAS/"

    # Default type used for all unknown types in this resource
    DEFAULT_TYPE = "observation facility"

    # Mapping to IVOA's messenger
    # https://www.ivoa.net/rdf/messenger/
    # All Waveband categories in AAS:
    # Gamma-ray
    # Infared
    # Infrared
    # Millimeter
    # Optical
    # Radio
    # Ultraviolet
    # X-ray
    _WAVEBAND_MAPPING = {
        "Ultraviolet": "UV",
        "Infared": "Infrared",
    }

    def __init__(self):
        pass


    def get_community(self) -> str:
        return "" # TODO (heliophysics / astronomy / planetology)


    def extract(self) -> dict:
        """
        Extract the page content into a dictionary.
        """
        content = CacheManager.get_page(AasExtractor.URL,
                                        list_name = self.CACHE)

        if not content:
            return dict()

        soup = BeautifulSoup(content, "html.parser")
        rows = soup.find('tbody').find_all('tr')
        headers = soup.find('thead').find('tr')
        headers = [header.text.strip().lower()
                for header
                in headers.find_all('th')]

        # wavebands and facility types column indexes
        WB = 3
        FT = 10
        wavebands = headers[WB:FT]
        facility_types = headers[FT:]

        # Process page's data into a dictionary.
        # This dictionary can then be processed by the ontology merger.
        result = dict()
        for row in rows:
            cols = row.find_all('td')
            cols = [col.text.strip() for col in cols]
            row_data = dict(zip(headers, cols)) # {"h1": "col1", "h2": "col2"}

            data = dict() # Dictionary to save the row's data

            # Add location to data dict
            location = row_data["location"]
            if location:
                data["location"] = [location]
                # TODO get latitude & longitude from location
            else:
                continue # TGCC is a computer and has no location.

            alt_labels = set()

            # Extract data from the full facility name
            # The full facility name contains a location (Observatory etc)
            # We can use a part-of between the facility and location
            facility_name = row_data["full facility name"].strip()
            # Origin observatory
            location = cut_location(facility_name,
                                               delimiter = " at ",
                                               alt_labels = alt_labels)
            if location:
                data["is_part_of"] = location

            # Add label to row dict
            data["label"] = facility_name

            # Add and filter out facility types
            for facility_type, col in zip(facility_types, cols[FT:]):
                if col:
                    # Can have more than one type
                    if "type" in data:
                        data["type"].append(facility_type)
                    else:
                        data["type"] = [facility_type]
            if "type" in data and data["type"] in ["computational center", "archive/database"]:
                continue # Filter out computational center & archive/database.
            elif "type" not in data:
                data["type"] = AasExtractor.DEFAULT_TYPE # telescope

            # waveband
            for waveband_length, waveband in zip(wavebands, cols[WB:FT]):
                if waveband:
                    waveband = waveband.capitalize()
                    if waveband in AasExtractor._WAVEBAND_MAPPING:
                        # Get the IVOA Messenger class name
                        waveband = AasExtractor._WAVEBAND_MAPPING[waveband]
                    # There can be more than one waveband per facility
                    if "waveband" not in data:
                        data["waveband"] = [waveband]
                    else:
                        data["waveband"].append(waveband)

            # alt labels
            if alt_labels:
                data["alt_label"] = alt_labels

            # Internal reference
            keyword = row_data["keyword"] # code
            if keyword:
                data["code"] = keyword
            else:
                # If there is no keyword (id), find it between ().
                keyword = facility_name[facility_name.find('(')+1:facility_name.find(')')]

            # Save the row's dict into the result dict
            result[keyword] = data
        return result


if __name__ == "__main__":
    pass
