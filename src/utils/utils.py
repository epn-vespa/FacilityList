"""
Define utility functions to manipulate data.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import json
import re
import time
from typing import Optional, Tuple, List
from urllib.parse import quote
import geopy
import pycountry_convert
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
import atexit

from config import CACHE_DIR # type: ignore
from utils.acronymous import proba_acronym_of
from utils.performances import timeall

geolocator = Nominatim(user_agent="obspm.fr")

def standardize_uri(label: str) -> str:
    """
    Creates a valid uri string from a label using lowercase and hyphens
    between words.

    Keyword arguments:
    label -- the label of the entity.
    """
    label = label.lower()
    label = re.sub(r"[^\w\s\.]", ' ', label)
    label = re.sub(r"\s+", ' ', label) # Remove multiple spaces
    label = label.split(' ')
    label = '-'.join([l for l in label if l])
    label = quote(label)
    return label


def cut_acronyms(label: str) -> Tuple[str]:
    """
    Acronyms are alternate names that are between ().
    Returns:
        (name without acronyms, last acronym*)
        *if the last acronym is at the end of the label.

    Keyword arguments:
    label -- the label containing acronyms
    """
    label = label.strip()
    # label, acronym_aka = cut_aka(label)
    acronyms = list(re.finditer(r"\([^(]+?\)", label))
    if not acronyms:
        return label, ""
    full_name_without_acronyms = ""
    acronym_str = ""
    prev_acronym_idx = 0
    for acronym in acronyms:
        name = label[prev_acronym_idx:acronym.start()-1].strip()
        prev_acronym_idx = acronym.end()+1
        acronym_str = acronym.group()[1:-1].strip() # remove ()
        full_name_without_acronyms += name + " "

    full_name_without_acronyms += label[prev_acronym_idx:]
    if label[-1] != ')':
        acronym_str = "" # Acronym for the whole string (last word)
    if len(acronyms) > 1:
        # If there are more than one acronym, impossible to detect which
        # acronym is the right one
        acronym_str = ""
    # Return full name without acronyms + the last acronym
    # Compute the probability of the acronym string to be an acronym
    # of the label without its acronyms.

    if proba_acronym_of(acronym_str, full_name_without_acronyms) != 1:
        acronym_str = ""
    return clean_string(full_name_without_acronyms), acronym_str


def cut_aka(label: str) -> Tuple[str]:
    """
    Delete stopwords like 'aka' from the label.
    Return the label without the aka and its aka.

    Keyword arguments:
    label -- the label to delete akas and get the alternate name from.
    """
    stopwords = '|'.join(["aka",
                          "a.k.a.",
                          "also known as",
                          "formerly the",
                          "formerly"])
    exp = re.compile(f"\\b({stopwords})\\b")

    alt_label = "" # Alt label for the whole entity
    for match in re.finditer(exp, label.lower()):
        start_index = match.start()
        end_index = match.end()
        if label[start_index-1] != '(': # The aka is not between ()
            after_aka = label[end_index:]
            # The aka end after a ','
            comma = after_aka.find(',')
            if comma > 0: # not the last substring
                aka = after_aka[0:comma]
                after_aka = after_aka[comma:]
            else:
                aka = after_aka
                after_aka = "" # end
                alt_label = aka # if end of string, is alt label of entity
            # label = f"{label[:start_index]} ({aka}) {after_aka}"
            label = f"{label[:start_index]} {after_aka}"

        else:
            # Find the aka for alt_label (if at the end of the string)
            if label[end_index:].count(')') == 1 and label.endswith(')'):
                alt_label = label[end_index:-1]
                label = label[:start_index-1]

    label = re.sub(exp, "", label)
    label = re.sub(r" +", " ", label)
    label = re.sub(r"\( ", "(", label)
    label = clean_string(label)

    # If the altLabel is in the location of the entity
    # it is not an altLabel of the entity.
    if alt_label and " at " not in label:
        return label, clean_string(alt_label)
    #if label[-1] == ')':
    #    return label.strip(), aka# last is a )
    return label, ""


def get_size(label: str) -> str:
    """
    Get the size of the facility from the label (in AAS & SPASE).
    Return the size and the label without the size.

    Troubleshooting:
        MM (or mm) is not for millimeter (see SPASE)

    Keyword arguments:
    label -- the label to extract the size from.
    """
    size = ""
    sizes = re.findall(r"(\d+)([\.\,]\d+)? ?(cm|mm|m|km|MM|CM|M|KM)", label)
    if sizes:
        for s in sizes:
            size += ''.join(s)
    label_without_size = re.sub(size, "", label).strip()
    return clean_string(label_without_size), size


def cut_part_of(label: str):
    """
    In AAS, some entities have "part of the" in their label.
    We want to cut them out and create a relation isPartOf.
    Sometimes, the "part of the" keyword is after the location,
    sometimes it is before, so we must be careful about which
    one is part of something.

    Returns:
        the label without the part of & the part of.

    Keyword arguments:
    label -- the label to cut.
    """
    part_of_keyword = "part of the"
    part_of_begin = label.lower().find(part_of_keyword)
    if part_of_begin == -1:
        part_of_keyword = "part of" # there are no cases like this in AAS
        part_of_begin = label.lower().find(part_of_keyword)
        if part_of_begin == -1:
            return label, ""
    before_part_of = label[:part_of_begin].strip()
    parenthesis_opened = False
    # The parenthesis opened before the "part of" keyword
    if before_part_of and before_part_of[-1] == '(':
        parenthesis_opened = True
        before_part_of = before_part_of[:-1].strip()
    after_part_of = label[part_of_begin + len(part_of_keyword):].strip()
    if parenthesis_opened:
        part_of_end = after_part_of.find(')')
        part_of = after_part_of[:part_of_end].strip()
        after_part_of = after_part_of[part_of_end+1:].strip()
    else:
        part_of = after_part_of
        after_part_of = ""
    label_without_part_of = before_part_of + ' ' + after_part_of
    return clean_string(label_without_part_of), clean_string(part_of)


def cut_location(label: str,
                 delimiter: str,
                 second_delimiter: str = ';') -> Tuple[str]:
    # TODO remove second_delimiter
    """
    Get the location of an entity by splitting it on a
    certain delimiter and add new alternate labels.
    Add alternate labels in alt_labels for:
    - the entity without the location,
    - the entity without the location and acronyms,
    - the entity's acronym without the location.
    Then, call clean_string to remove the first "the " in the location.

    Keyword arguments:
    label -- the label of an entity
    delimiter -- the delimiter (" at ", ","...)
    second_delimiter -- if there are more than one locations
    alt_labels -- the set of alternate labels to add to
    """
    location = ""
    label_without_location = label
    if label.count(delimiter) == 1:
        label_without_location, location = [a.strip() for a in label.split(delimiter, maxsplit = 1)]
        """
        label_without_acronyms, label_acronym = cut_acronyms(label)
        alt_labels.add(label)
        alt_labels.add(label_without_acronyms)
        alt_labels.add(label_acronym)
        if "" in alt_labels:
            alt_labels.remove("")
        """

    # More than one location (example AAS: 'Yunnan Astronomical Observatory (YAO); Lijiang Observatory')
    # locations = [l.strip() for l in location.split(second_delimiter)]
    return label_without_location, location


def clean_string(text: str) -> str:
    """
    Removes all \n, \t and double spaces from a string.
    Remove final '.'

    Keyword arguments:
    string -- the string to clean
    """
    # text = text.replace("\n", " ")
    # add a closing parentheses if never closed
    #if (text[::-1].find('(') < text[::-1].find(')') or
    #    '(' in text and ')' not in text):
    #    text += ')'
    # remove final parentheses if never opened
    #if text.count(')') > text.count('('):
    #    text = text[::-1].replace(')', ' ', 1)[::-1]
    text = text.replace("\r", " ")
    text = re.sub(r"\t", " ", text)
    text = re.sub(r"\n+", "\n", text)
    text = re.sub(r"\r", " ", text)
    text = re.sub(r" +", " ", text).strip()
    if text and text[-1] == '.':
        text = text[:-1]
    return text.strip()


def remove_punct(text: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9 ]+", ' ', text)
    return text


def extract_items(d: dict) -> List[Tuple]:
    """
    Flatten a recursive dictionary to a list of (key, value).
    This is necessary to create triplets from for json format.

    Keyword arguments:
    d -- a recursive dictionary.
    """
    result = []
    for key, value in d.items():
        if isinstance(value, dict):
            result.extend(extract_items(value))
        else:
            result.append((key, value))
    return result


def cut_language_from_string(text: str) -> Tuple[str, str]:
    """
    Cut the language tag on '@' if there is a language tag.
    Returns the text without language tag and the language tag.
    Language tag example: @en
    The language tag should be at the end of the string.

    Keyword arguments:
    text -- will be split into a text and its language.
    """
    lang = re.findall(r"@[a-zA-Z]{2,3}$", text)
    if lang:
        lang = lang[0][1:] # remove @
        text = text[:-len(lang) - 1]
    else:
        lang = ""
    return text, lang


def get_datetime_from_iso(datetime_str: str):
    """
    Fix datetime string :
        month 00 day 00 -> 1st of January
        & remove '+' sign
    Also complete the incomplete iso dates.

    Keyword arguments:
    datetime_str -- the ISO datetime string
    """
    if datetime_str.startswith('+'): # datetime module
        datetime_str = datetime_str[1:]
    datetime_str.replace("-00T", "-01T").replace("-00-", "-01-")
    if re.match(r"^\d\d\d\d$", datetime_str):
        datetime_str += "-01-01T00:00:00"
    elif re.match(r"^\d\d\d\d-\d\d$", datetime_str):
        datetime_str += "-01T00:00:00"
    elif re.match(r"^\d\d\d\d-\d\d-\d\d$", datetime_str):
        datetime_str += "T00:00:00"

    return datetime_str


def merge_into(newer_entity_dict: dict,
               prior_entity_dict: dict):
    """
    Merge data from the prior dict into the newer dict.

    Keyword arguments:
    newer_entity_dict -- the entity dict to save data in
    prior_entity_dict -- the prior entity dict to merge with the newer
    """
    for key, values in prior_entity_dict.copy().items():
        if key == "prior_id":
            continue
        if key == "label":
            if not isinstance(values, set):
                values = {values}
            if "alt_label" in newer_entity_dict:
                 # Keep the old label as an alternate label of the new entity
                newer_entity_dict["alt_label"].update(values)
            else:
                newer_entity_dict["alt_label"] = values
        elif key in newer_entity_dict:
            merge_into = newer_entity_dict[key]
            if not isinstance(values, list) and not isinstance(values, set):
                values = [values]
            for value in values:
                if isinstance(merge_into, set):
                    merge_into.add(value)
                    continue
                elif not isinstance(merge_into, list):
                    merge_into = [merge_into]
                if value not in merge_into:
                    if key in ["latitude", "longitude"]:
                        # Keep the most precise value
                        old_value = newer_entity_dict[key]
                        if isinstance(old_value, list):
                            old_value = old_value[0]
                        if len(str(value)) > len(str(old_value)):
                            merge_into = [value]
                        elif len(str(value)) == len(str(old_value)):
                            if value != 0.0:
                                merge_into = [value]
                            else:
                                merge_into = [old_value]
                    else:
                        merge_into.append(value)
            newer_entity_dict[key] = merge_into
        else:
            newer_entity_dict[key] = values
        if "alt_label" in newer_entity_dict and "label" in newer_entity_dict:
            # Prevent label to be in alt_label.
            newer_entity_dict["alt_label"] -= {newer_entity_dict["label"]}


# Prevent computing location info multiple times
# as it requires to request a server.
location_infos = None # uninitialized

def _save_location_infos_in_cache():
    global location_infos
    if location_infos:
        path = CACHE_DIR
        if not path.exists():
            path.mkdir(parents = True, exist_ok = True)
        path = str(path / "location_infos.json")
        print(f"dumping {len(location_infos)} elements in {path}.")
        with open(path, "w", encoding = "utf-8") as f:
            json.dump(location_infos, f, indent=" ")

def load_location_infos_from_cache():
    atexit.register(_save_location_infos_in_cache)
    global location_infos
    location_infos = {}
    path = CACHE_DIR / "location_infos.json"
    if not path.exists():
        return
    path = str(path)
    with open(path, "r", encoding = "utf-8") as f:
        location_infos = json.load(f)

@timeall
def get_location_info(label: Optional[str] = None,
                      location: Optional[str] = None,
                      address: Optional[str] = None,
                      latitude: Optional[float]  = None,
                      longitude: Optional[float]  = None,
                      part_of: Optional[str] = None,
                      language: str = "en",
                      retries: int = 4,
                      from_cache = True) -> dict:
    """
    From an entity's location information such as its latitude,
    longitude, location (as in the source), address...,
    return the complete information dict if it is a ground address (on earth).
    This function uses geopy and saves information in a cache.

    FIXME: Once we have the superclass of the entities,
    if it is not a Ground entity, it has no location. Then, we do not
    need to use isupper(), islower() and hasdigit to filter location.
    Example: "Cassini" is a place according to Geopy.

    Keyword arguments:
    label -- the label of the entity
    location -- the location string as in the source
    address -- the address as in the source
    latitude -- a float of the latitude of the entity if on earth
    longitude -- a float of the longitude of the entity if on earth
    part_of -- entity name for which this entity is a subpart
    language -- language in which to retrieve addresses
    retries -- how many retries left if the first geopy request failed
    from_cache -- do not request geopy again. Set to False for debug only
    """
    global location_infos
    if location_infos is None:
        load_location_infos_from_cache()
    result = None
    saved_in = ""
    if not isinstance(location, list):
        location = [location]
    if not isinstance(part_of, list):
        part_of = [part_of]
    if isinstance(latitude, list):
        if len(latitude) == 0:
            latitude = None
        else:
            latitude = latitude[0]
    if isinstance(longitude, list):
        if len(longitude) == 0:
            longitude = None
        else:
            longitude = longitude[0]

    # Remove labels that cannot be used for location
    """
    if label:
        label, _ = get_size(label) # Remove the size of the facility
        if (re.match(r".*\d.*", label) or
            # Labels that contain any number are not place names.
            label.isupper() or label.islower() or
            # A location label can't be only made of lower or uppercases.
            not ' ' in label.strip()
            # A label that refers to a place is almost never a single
            # word (city/country). It is usually called "...observatory"
            # or "...station".
        ):
            label = None
    """

    # Remove initial "the" from the label as it performs very bad with geopy
    if label and label.lower().startswith("the "):
        label = label[4:].strip()

    # Return information if already in the cache
    if from_cache:

        latlong_empty = False
        address_empty = False
        location_empty = False
        part_of_empty = False
        label_empty = False

        if (latitude is not None and longitude is not None and
             (latitude != 0 or longitude != 0)):
            saved_in = "latlong/" + str(latitude) + '/' + str(longitude)
            data = location_infos.get(saved_in, None)
            data["location_confidence"] = 1
            if data is not None:
                return data
            elif data == {}:
                latlong_empty = True
        else:
            latlong_empty = True

        if address:
            saved_in = "geocode/" + str(address)
            data = location_infos.get(saved_in, None)
            if data is not None and data:
                data["location_confidence"] = 0.75
                return data
            elif data == {}:
                address_empty = True
            # else: not in cache yet.
        else:
            address_empty = True

        # TODO: find part_of in the result dict and call get_location_info again.
        """
        if part_of:
            only_none = True
            for part in part_of:
                if part is None:
                    continue
                only_none = False
                saved_in = "geocode/" + str(part)
                data = location_infos.get(saved_in, None)
                if data is not None and data:
                    data["location_confidence"] = 0.25
                    return data
                elif data == {}:
                    part_of_empty = True
        else:
            part_of_empty = True
        part_of_empty = part_of_empty or only_none
        """

        if location:
            only_none = True
            for loc in location: # Can have more than one location
                if loc is None:
                    continue
                only_none = False
                saved_in = "geocode/" + str(loc)
                data = location_infos.get(saved_in, None)
                if data is not None and data:
                    data["location_confidence"] = 0.5
                    return data
                elif data == {}:
                    location_empty = True
                    continue
                if ("earth." in loc.lower()):
                    location_infos[saved_in] = {}
                    return {}
                elif "space" == loc.lower():
                    location_infos[saved_in] = {}
                    return {}
        else:
            location_empty = True
        location_empty = location_empty or only_none

        if label:
            saved_in = "geocode/" + str(label)
            data = location_infos.get(saved_in, None)
            if data is not None and data:
                data["location_confidence"] = 0.25
                return data
            elif data == {}:
                label_empty = True
        else:
            label_empty = True

        # If the cache's data was empty for any of the provided information
        if latlong_empty and part_of_empty and location_empty and address_empty and label_empty:
            return {}

    # Get information with geolocator
    result_dict = {"location": "Earth"}
    try:
        if latitude is not None and longitude is not None:
            if latitude != 0 or longitude != 0:
                saved_in = "latlong/" + str(latitude) + '/' + str(longitude)
                result = geolocator.reverse((latitude, longitude),
                                             exactly_one=True,
                                             language=language)
                if result is None:
                    # No address, in the sea
                    if retries == 0:
                        data = {"location": "Ocean",
                                "location_confidence": 0.9}
                        location_infos[saved_in] = data
                        return data
                    else:
                        print("Retrying for", saved_in, ".\nretries:", retries)
                        return get_location_info(latitude=latitude,
                                                 longitude=longitude,
                                                 retries = retries - 1)
                else:
                    result_dict["location_confidence"] = 1.0

        if result is None and address:
            saved_in = "geocode/" + str(address)
            result = geolocator.geocode(address,
                                        exactly_one=True,
                                        language=language)
            if result is not None:
                result_dict["location_confidence"] = 0.75

        # TODO call get_location_info with part_of
        """
        if result is None and part_of:
            for part in part_of:
                if part is None:
                    continue
                if result is not None:
                    break
                saved_in = "geocode/" + str(part)
                result = geolocator.geocode(part,
                                            exactly_one=True,
                                            language=language)
        """

        if result is None and location:
            for loc in location: # Can have more than one location
                if loc is None:
                    continue
                if result is not None:
                    break
                saved_in = "geocode/" + str(loc)
                result = geolocator.geocode(loc,
                                            exactly_one=True,
                                            language=language)
                if result is None:
                    # This place does not exist
                    location_infos[saved_in] = {}
                else:
                    result_dict["location_confidence"] = 0.5


        if result is None and label:
            saved_in = "geocode/" + str(label)
            result = geolocator.geocode(label,
                                        exactly_one=True,
                                        language=language)
            if result and label not in str(result).split(',')[0] or not result:
                # the label is not identical to the first part of the address
                retrieved = False
                for keyword in ["antenna", "observatory", "telescope"]:
                    result = geolocator.geocode(label + " " + keyword,
                                                exactly_one=True,
                                                language=language)
                    if result and label.lower() in str(result).lower():
                        retrieved = True
                if not retrieved:
                    location_infos[saved_in] = {}
                    return {}
            if result:
                result_dict["location_confidence"] = 0.25

        if not result:
            # Did not find the location for the provided data.
            location_infos[saved_in] = {}
            return {}

    except KeyboardInterrupt:
            print("Shutdown requested...exiting")
            exit()
    except geopy.exc.GeocoderUnavailable as e:
        # Retry after 0.5s
        retries -= 1
        if retries < 0:
            location_infos[saved_in] = {}
            return {}
        print(f"Warning: {e}.\n{retries} retries left for {label}. Retrying...")
        return get_location_info(label=label,
                                 location=location,
                                 address=address,
                                 latitude=latitude,
                                 longitude=longitude,
                                 retries=retries)

    # Transform the result into a compatible data dict
    raw = result.raw

    # Add the location type
    location_type = raw.get("addresstype")
    name = raw.get("name")

    # Resolve for continent names
    # (for Africa, the geolocator returns a place in Chad)
    for continent_code, continent_name in continent_dict.items():
        if name.lower() == continent_name.lower():
            result_dict["continent"] = continent_name
            result_dict["continent_code"] = continent_code
            location_infos[saved_in] = result_dict
            return result_dict

    if location_type in ["village", "town", "city", "administrative", "municipality"]:
        location_type = "city"
        result_dict[location_type] = raw.get("name") # not "display_name"
    elif location_type in ["country", "continent", "address"]:
        result_dict[location_type] = raw.get("name") # not "display_name"

    # Add a city for non-city locations
    if location_type != "city":
        city = raw.get("city") or raw.get("town") or raw.get("village") or raw.get("administrative") or None
        if city is not None:
            result_dict["city"] = city

    # Add latitude & longitude
    if location_type != "continent" and location_type != "country":
        if latitude is None and "lat" in raw:
            latitude = float(result.raw.get("lat"))
            result_dict["latitude"] = latitude
        if longitude is None and "lon" in raw:
            longitude = float(result.raw.get("lon"))
            result_dict["longitude"] = longitude

    # Get address from latitude & longitude
    address = None
    address_str = ""
    country = None
    # Continents and countries do not have latitude & longitude.
    if location_type not in ["continent", "country"] and not "address" in raw:
        # get an address for lat & long
        address_result = _get_address(latitude, longitude)
        if address_result is None:
            pass
            #address = None
            #address_str = None
        else:
            address = address_result.raw.get("address")
            address_str = address_result.address
    elif location_type not in ["continent", "country"] and "address" in raw:
        address = raw.get("address")# .address
        address_str = raw.get("display_name")
    elif "display_name" in raw:
        address_str = str(raw.get("display_name"))

    # Add the address
    if address_str and location_type not in ["continent", "country", "city"]:
        result_dict["address"] = address_str

    # Get the country's name from the address
    if location_type != "continent":
        if address is not None:
            if "country" in address:
                country = address["country"]
        if not country:
            country = address_str.split(',')[-1].strip()

    # Get the continent from the country
    country_code = None
    if location_type != "continent" and country:
        if address and "country_code" in address:
            country_code = address["country_code"].upper()
        else:
            try:
                country_code = pycountry_convert.country_name_to_country_alpha2(country)
            except KeyError:
                # Country name does not exist.
                country = None
                country_code = None
                result_dict["continent"] = "Antarctica"
        if country:
            result_dict["country"] = country

        if country_code:
            # result_dict["country_code"] = country_code
            if country_code == "RU": # Russia
                continent = "Europe" if longitude < 59 else "Asia"
            else:
                continent_code = pycountry_convert.country_alpha2_to_continent_code(country_code)
                continent = continent_dict[continent_code]
            # result_dict["continent_code"] = continent_code
            result_dict["continent"] = continent

    # Save the computed result for future calls
    location_infos[saved_in] = result_dict
    return result_dict


def _get_address(latitude: float,
                 longitude: float,
                 retries: int = 3) -> dict[str]:
    """
    Get a dictionary of the location for a latitude and a longitude.
    Use it to get an address (with country name).
    """
    try:
        result = geolocator.reverse((latitude, longitude),
                                    exactly_one=True,
                                    language="en")
    except Exception as e:
        # Retry after 0.5s
        retries -= 1
        if retries == 0:
            return None
        time.sleep(0.5)
        print(f"Warning: {e}.\nRetrying...")
        return _get_address(latitude,
                            longitude,
                            retries)
    return result


continent_dict = {
    "NA": "North America",
    "SA": "South America",
    "AS": "Asia",
    "AF": "Africa",
    "OC": "Oceania",
    "EU": "Europe",
    "AQ" : "Antarctica"
}


def distance(latlong1: Tuple[float],
             latlong2: Tuple[float]) -> float:
    """
    Get the distance between two points on earth (in km).

    Keyword arguments:
    latlong1 -- tuple (latitude, longitude) of the first point.
    latlong1 -- tuple (latitude, longitude) of the second point.
    """
    return geodesic(latlong1, latlong2)


if __name__ == "__main__":
    pass
