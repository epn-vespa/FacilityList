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
from tqdm import tqdm
from data_updater import entity_types
from utils.utils import clean_string, cut_location, cut_acronyms, cut_part_of, get_size, proba_acronym_of, merge_into, cut_aka
from data_updater.extractor.cache import CacheManager
from data_updater.extractor.extractor import Extractor

class AasExtractor(Extractor):
    URL = "https://journals.aas.org/author-resources/aastex-package-for-manuscript-preparation/facility-keywords/"

    # URI to save this source as an entity (obs:AAS_list)
    URI = "AAS_list"

    # URI to save entities from this source
    NAMESPACE = "aas"

    # Folder name to save cache/ and data/
    CACHE = "AAS/"

    # Default type used for all unknown types in this resource
    DEFAULT_TYPE = entity_types.OBSERVATION_FACILITY

    POSSIBLE_CATEGORES = {entity_types.GROUND_OBSERVATORY,
                          entity_types.MISSION,
                          entity_types.OBSERVATORY_NETWORK,
                          entity_types.TELESCOPE}
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

    # Used to split the label into entity / location
    LOCATION_DELIMITER = " at "

    def __init__(self):
        pass


    def __str__(self):
        return self.NAMESPACE


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

        # To merge duplicate entities
        duplicate_codes = dict() # code1, duplicate of, code2 (code1, code2)

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

            # Internal reference
            keyword = row_data["keyword"] # code
            if keyword:
                data["code"] = keyword
            else:
                # If there is no keyword (id), find it between ().
                keyword = facility_name[facility_name.find('(')+1:facility_name.find(')')]

            # Extract data from the full facility name
            # The full facility name contains a location (Observatory etc)
            # We can use a part-of between the facility and location
            facility_name = row_data["full facility name"].strip()

            # Akas
            facility_name, aka = cut_aka(facility_name)
            if aka:
                alt_labels.add(aka)

            # Duplicate identifiers
            duplicate_idx = facility_name.find("[Duplicate of ")
            duplicate_of = ""
            if duplicate_idx >= 0:
                facility_name, duplicate_of = facility_name.split("[Duplicate of ")
                duplicate_of = duplicate_of.strip()[:-1]
            facility_name = clean_string(facility_name)
            alt_labels.add(facility_name)

            # Origin observatory
            label_without_location, location = cut_location(facility_name,
                                                            delimiter = self.LOCATION_DELIMITER)

            if location.lower().startswith("the "):
                location = location[4:] # often location starts with " at the ..."

            # If the entity is a part of some other entity
            label_without_part_of, part_of_label = cut_part_of(label_without_location)

            # Add label to row dict
            facility_name = label_without_part_of
            data["label"] = facility_name

            if part_of_label:
                data["is_part_of"] = [part_of_label]
                if part_of_label in result:
                    if "has_part" in result[part_of_label]:
                        result[part_of_label]["has_part"].append(facility_name)
                    else:
                        result[part_of_label]["has_part"] = [facility_name]
                else:
                    result[part_of_label] = {"label": part_of_label,
                                             "has_part": [facility_name]}
                label_without_location = label_without_part_of


            # Get the size of the facility
            label_without_size, size = get_size(label_without_part_of)
            if size:
                data["size"] = size

            # Get the acronym
            label_without_acronyms, label_acronym = cut_acronyms(label_without_size)
            if label_acronym:
                label_without_acronyms, _ = cut_acronyms(facility_name)
                alt_labels.add(label_without_acronyms)
                label_without_acronyms, _ = cut_acronyms(label_without_location)
                alt_labels.add(label_without_acronyms)
                if label_acronym in keyword:
                    alt_labels.add(keyword)

            if location:
                alt_labels_location = set()
                location, location_aka = cut_aka(location)
                if location_aka:
                    alt_labels_location.add(location_aka)
                location_without_part_of, part_of_location = cut_part_of(location)
                # If the location is a part of something else

                location_without_acronym, location_acronym = cut_acronyms(location_without_part_of)

                if location_acronym and proba_acronym_of(location_without_acronym,
                                                         location_acronym) == 1:
                    alt_labels_location.update((location_acronym, location))

                if location_without_acronym in result:
                    if "has_part" in result[location_without_acronym]:
                        result[location_without_acronym]["has_part"].append(facility_name)
                    else:
                        result[location_without_acronym]["has_part"] = [facility_name]
                else:
                    result[location_without_acronym] = {"label": location_without_acronym,
                                                        "has_part": [facility_name]}

                if part_of_location:
                    alt_labels_location.add(location_without_part_of)
                    if "is_part_of" not in result[location_without_acronym]:
                        result[location_without_acronym]["is_part_of"] = [part_of_location]
                    else:
                        result[location_without_acronym]["is_part_of"].append(part_of_location)
                    # add the location's part of to the result
                    result[part_of_location] = {"label": part_of_location}

                result[location_without_acronym]["label"] = location
                alt_labels_location = alt_labels_location - {location_without_acronym}
                # Location's alt labels
                if alt_labels_location:
                    if "alt_label" in result[location_without_acronym]:
                        result[location_without_acronym]["alt_label"].update(alt_labels_location)
                    else:
                        result[location_without_acronym]["alt_label"] = alt_labels_location
                # Add location's info to data
                alt_labels.add(label_without_location)
                if "is_part_of" in data:
                    data["is_part_of"].append(location_without_acronym)
                else:
                    data["is_part_of"] = [location_without_acronym]

            # Alt labels
            alt_labels = alt_labels - {facility_name}

            # Add and filter out observed objects (previously facility types)
            for facility_type, col in zip(facility_types, cols[FT:]):
                if col:
                    # Can have more than one type
                    if "observed_object" in data:
                        data["observed_object"].append(facility_type)
                    else:
                        data["observed_object"] = [facility_type]
            if "observed_object" in data:
                if any([x in ["computational center", "archive/database"] for x in data["observed_object"]]):
                    continue # Filter out computational center & archive/database.
            #elif "type" not in data:
            #    data["type"] = AasExtractor.DEFAULT_TYPE # telescope

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
            alt_labels = alt_labels - set(facility_name)
            if "The Instituto de Astrofísica" in facility_name:
                print("Alt labels:::", alt_labels)
                print(facility_name)
            if alt_labels:
                data["alt_label"] = alt_labels

            # Save the row's dict into the result dict
            result[keyword] = data

            # Add duplicate entities code pairs
            if duplicate_of:
                duplicate_codes[keyword] = duplicate_of

        # Merge the duplicate entities
        for code1, code2 in duplicate_codes.items():
            if code1 in result and code2 in result:
                merge_into(result[code1], result[code2])
                del(result[code2])

        # Add a type to the entities
        for code, data in tqdm(result.items(), desc = f"Classify {self.NAMESPACE}"):
            choices = [entity_types.TELESCOPE, entity_types.MISSION, entity_types.GROUND_OBSERVATORY]
            label = data["label"]# + ' ' + ' '.join(data.get("alt_label", []))
            # label = label.strip()
            description = ""
            if label.lower().endswith("telescopes"):
                data["type"] = entity_types.OBSERVATORY_NETWORK
                continue
            elif label.lower().endswith("telescope"):
                data["type"] = entity_types.TELESCOPE

            if "location" in data:
                for l in data["location"]:
                    l = l.lower()
                    if "space" in l:
                        data["type"] = entity_types.SPACECRAFT
                        break
                    if "airborne" in l:
                        data["type"] = entity_types.AIRBORNE
                        break
                    if l == "earth":
                        data["type"] = entity_types.OBSERVATORY_NETWORK
                        break
            if "type" in data:
                continue

            description = entity_types.to_string(data, exclude = ("has_part", "is_part_of", "alt_label", "code"))
            if "has_part" in data:
                choices = [entity_types.MISSION,
                           entity_types.GROUND_OBSERVATORY,
                           entity_types.OBSERVATORY_NETWORK]
            elif "waveband" in data:
                choices = [entity_types.MISSION,
                           entity_types.GROUND_OBSERVATORY,
                           entity_types.OBSERVATORY_NETWORK,
                           entity_types.TELESCOPE]
            elif "observed_object" in data:
                choices = [entity_types.MISSION,
                           entity_types.GROUND_OBSERVATORY,
                           entity_types.TELESCOPE,
                           entity_types.OBSERVATORY_NETWORK]
            choices += entity_types.UFO # La Villa
            category = entity_types.classify(description,
                                             choices = choices,
                                             from_cache = False)
            data["type"] = category

        return result


if __name__ == "__main__":
    pass
