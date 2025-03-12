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
from utils import del_aka, cut_acronyms
import re
from extractor.cache import CacheManager

class AasExtractor():
    URL = "https://journals.aas.org/author-resources/aastex-package-for-manuscript-preparation/facility-keywords/"

    # URI to save this source as an entity (obs:AAS_list)
    URI = "AAS_list"

    # URI to save entities from this source
    NAMESPACE = "aas"

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

    def get_community(self) -> str:
        return "" # TODO (heliophysics / astronomy / planetology)

    def extract(self) -> dict:
        """
        Extract the page content into a dictionary.
        """
        content = CacheManager.get_page(AasExtractor.URL)

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

            keyword = row_data["keyword"] # code
            if keyword:
                data["code"] = keyword

            # Add location to data dict
            location = row_data["location"]
            if location:
                data["location"] = location
            else:
                continue # TGCC is a computer and has no location.

            alt_labels = set()

            # Extract data from the full facility name
            # The full facility name contains a location (Observatory etc)
            # We can use a part-of between the facility and location
            # and acronyms that need to be treated as alternate labels
            label = del_aka(row_data["full facility name"].strip())
            #full_facility_name = row_data["full facility name"].strip()
            #full_facility_name = del_aka(full_facility_name)
            # Origin observatory
            at = re.search(" at ", label)
            facility_name = ""
            if at:
                # Facility name is before the first " at "
                facility_name = label[0:at.start()].strip() # TODO do not use facility name as pref label, but label instead
                alt_labels.add(facility_name)
                facility_location = label[at.end():].strip()
            else:
                facility_name = label
                facility_location = ""
            # Take the first acronym and remove parenthesis
            facility_name, facility_acronym = cut_acronyms(facility_name)
            if facility_acronym:
                alt_labels.add(facility_acronym)
            if facility_name:
                alt_labels.add(facility_name)
            if facility_location:
                facility_location_name, facility_location_acronym = cut_acronyms(facility_location)
                # TODO if there is more than 1 acronym,
                # there might be more than 1 location
                result[facility_location_name] = {
                        "type": "FacilityLocation",
                        "label": facility_location,
                        }
                alt_labels_location = []
                if facility_location != facility_location_name:
                    alt_labels_location.append(facility_location_name)
                if facility_location_acronym:
                    alt_labels_location.append(facility_location_acronym)
                if alt_labels_location:
                    result[facility_location_name]["alt_label"] = alt_labels_location

                data["part_of"] = facility_location_name


            # Add and filter out facility types
            for facility_type, col in zip(facility_types, cols[FT:]):
                if col:
                    data["type"] = facility_type
            if "type" in data and data["type"] in ["computational center", "archive/database"]:
                continue # Filter out computational center & archive/database.
            elif "type" not in data:
                data["type"] = AasExtractor.DEFAULT_TYPE # telescope

            # Add label to row dict
            data["label"] = label #facility_name

            # Add waveband to row dict
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

            if not keyword:
                # If there is no keyword (id), find it between ().
                keyword = label[label.find('(')+1:label.find(')')]

            if alt_labels:
                data["alt_label"] = alt_labels

            # Save the row's dict into the result dict
            # keyword = identifier of the data in the source
            result[keyword] = data
        return result

if __name__ == "__main__":
    pass
