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
from data_updater.extractor.data_fixer import fix
from utils.utils import clean_string, cut_location, cut_acronyms, cut_part_of, get_size, merge_into, cut_aka
from data_updater.extractor.cache import CacheManager
from data_updater.extractor.extractor import Extractor
from utils.llm_connection import LLM

class AasExtractor(Extractor):
    URL = "https://journals.aas.org/author-resources/aastex-package-for-manuscript-preparation/facility-keywords/"
    # "https://journals.aas.org/facility-keywords/"

    URI = "AAS_list"

    # URI to save entities from this source
    NAMESPACE = "aas"

    # Folder name to save cache/ and data/
    CACHE = "AAS/"

    # Default type used for all unknown types in this resource
    DEFAULT_TYPE = entity_types.TELESCOPE

    # List's types.
    # For merging strategies. Prevent merging data from lists
    # that do not have types in common
    POSSIBLE_TYPES = {entity_types.GROUND_OBSERVATORY,
                      entity_types.MISSION,
                      entity_types.TELESCOPE,
                      entity_types.AIRBORNE,
                      entity_types.SPACECRAFT}

    # No need to disambiguate the type with LLM.
    # Useful for merging strategy: when the type is ambiguous,
    # it is recommanded to not discriminate on types.
    # 1: always known.
    # 0.5: partially known (see individuals)
    # 0: never known.
    TYPE_KNOWN = 0.5


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


    def extract(self,
                from_cache: bool = True) -> dict:
        """
        Extract the page content into a dictionary.
        """
        content = CacheManager.get_page(AasExtractor.URL,
                                        list_name = self.CACHE,
                                        from_cache = from_cache)

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
        # Used to find duplicates by labels instead of codes after processing
        # all the entities
        labels_by_code = dict()

        for row in rows:
            data = dict() # Dictionary to save the row's data

            cols = row.find_all('td')
            cols = [col.text.strip() for col in cols]
            row_data = dict(zip(headers, cols)) # {"h1": "col1", "h2": "col2"}

            alt_labels = set()

            # Get facility name
            facility_name = row_data["full facility name"].strip()
            if facility_name.startswith("Deprecated"):
                continue
            split_labels = facility_name.split('/')
            facility_name = split_labels[0]
            alt_labels.update(split_labels[1:])

            # Add location to data dict
            location = row_data["location"]
            if location:
                if ' & ' in location:
                    data["location"] = [l.strip() for l in location.split(' & ')]
                elif ' and 'in location:
                    data["location"] = [l.strip() for l in location.split(' and ')]
                else:
                    data["location"] = [location]
                # TODO get latitude & longitude from location
            else:
                continue # TGCC is a computer and has no location.

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

            # Akas
            facility_name, aka = cut_aka(facility_name)
            if aka:
                alt_labels.add(aka)


            # Duplicate identifiers
            duplicate_idx = facility_name.find("[Duplicate of ")
            duplicate_of = ""
            if duplicate_idx >= 0:
                facility_name, duplicate_of = facility_name.split("[Duplicate of ")
                duplicate_of = duplicate_of.strip()[:-1] # remove ']'

            # Add label to row dict
            facility_name = clean_string(facility_name)
            data["label"] = facility_name


            # Origin observatory
            label_without_location, location = cut_location(facility_name,
                                                            delimiter = self.LOCATION_DELIMITER)

            if location.lower().startswith("the "):
                location = location[4:] # often location starts with " at the ..."

            # If the entity is a part of some other entity
            label_without_part_of, part_of_label = cut_part_of(label_without_location)

            if part_of_label:
                alt_labels.add(label_without_part_of)
                data["is_part_of"] = [part_of_label]
                # The host observatory might already exist in the result dict
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
                # get other labels without any acronyms too
                label_without_acronyms, _ = cut_acronyms(facility_name)
                alt_labels.add(label_without_acronyms)
                label_without_acronyms, _ = cut_acronyms(label_without_location)
                alt_labels.add(label_without_acronyms)
                if label_acronym in keyword:
                    alt_labels.add(keyword) # keyword often is acronym:size

            if location:
                alt_labels_location = set()
                location, location_aka = cut_aka(location)
                if location_aka:
                    alt_labels_location.add(location_aka)
                location_without_part_of, part_of_location = cut_part_of(location)
                # If the location is a part of something else

                location_without_acronym, location_acronym = cut_acronyms(location_without_part_of)

                if location_acronym:
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
            alt_labels.pop(facility_name)
            if alt_labels:
                data["alt_label"] = alt_labels

            # Save the row's dict into the result dict
            result[facility_name] = data
            labels_by_code[keyword] = facility_name


            # Add duplicate entities code pairs
            if duplicate_of:
                duplicate_codes[keyword] = duplicate_of

        # Merge the duplicate entities
        for code1, code2 in duplicate_codes.items():
            label1 = labels_by_code[code1]
            label2 = labels_by_code[code2]
            if label1 in result and label2 in result:
                merge_into(result[label1], result[label2])
                del(result[label2])

        # Fix errors in source page
        fix(result, self)

        # Add a type to the entities
        for code, data in tqdm(result.items(), desc = f"Classify {self.NAMESPACE}"):
            choices = [entity_types.TELESCOPE, entity_types.MISSION, entity_types.GROUND_OBSERVATORY]
            label = data["label"]# + ' ' + ' '.join(data.get("alt_label", []))
            # label = label.strip()
            description = ""
            data["type_confidence"] = 1

            if ("telescopes" in label.lower() or
               "twin telescope" in label.lower() or
               " array" in label.lower() or
               "telescope network" in label.lower() or
               label == "La Palma" in label and "Siding Spring" in label):
                data["type"] = entity_types.GROUND_OBSERVATORY
                continue
            elif label.lower().endswith("telescope"):
                data["type"] = entity_types.TELESCOPE
                continue
            elif label.lower().endswith("mission"):
                data["type"] = entity_types.MISSION
                continue

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
                        data["type"] = entity_types.GROUND_OBSERVATORY
                        break
            if "type" in data:
                # No need to disambiguate type with LLM
                continue

            description = entity_types.to_string(data, exclude = ("has_part", "is_part_of", "alt_label", "code"))
            data["type_confidence"] = 0
            if "has_part" in data:
                choices = [entity_types.MISSION,
                           entity_types.GROUND_OBSERVATORY,
                           ]
            elif "waveband" in data:
                choices = [entity_types.MISSION,
                           entity_types.GROUND_OBSERVATORY,
                           entity_types.TELESCOPE
                           ]
            elif "observed_object" in data:
                choices = [entity_types.MISSION,
                           entity_types.GROUND_OBSERVATORY,
                           entity_types.TELESCOPE
                           ]
            choices.append(entity_types.UFO) # La Villa, Madrid...
            category = LLM().classify(description,
                                      choices = choices,
                                      from_cache = True,
                                      cache_key = self.NAMESPACE + '#' + data["label"])
            data["type"] = category

        return result


if __name__ == "__main__":
    pass
