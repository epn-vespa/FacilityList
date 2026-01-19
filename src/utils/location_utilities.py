"""
Utilities to get location information from various input data.
Uses geopy and a local cache to avoid multiple requests for the same location.
Saves the cache at exit.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import json
import atexit
import time
import pycountry_convert
import re

from typing import Optional, Tuple
from utils.performances import timeall
from utils.string_utilities import remove_parenthesis
from config import CACHE_DIR

import geopy
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
geolocator = Nominatim(user_agent="obspm.fr")



# Prevent computing location info multiple times
# as it requires to request a server.
location_infos = None # uninitialized

def _save_location_infos_in_cache():
    global location_infos
    if location_infos:
        path = CACHE_DIR
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


STATES = {'AL' : 'Alabama',
          'AK' : 'Alaska',
          'AZ' : 'Arizona',
          'AR' : 'Arkansas',
          'CA' : 'California',
          'CO' : 'Colorado',
          'CT' : 'Connecticut',
          'DE' : 'Delaware',
          'FL' : 'Florida',
          'GA' : 'Georgia',
          'HI' : 'Hawaii',
          'ID' : 'Idaho',
          'IL' : 'Illinois',
          'IN' : 'Indiana',
          'IA' : 'Iowa',
          'KS' : 'Kansas',
          'KY' : 'Kentucky',
          'LA' : 'Louisiana',
          'ME' : 'Maine',
          'MD' : 'Maryland',
          'MA' : 'Massachusetts',
          'MI' : 'Michigan',
          'MN' : 'Minnesota',
          'MS' : 'Mississippi',
          'MO' : 'Missouri',
          'MT' : 'Montana',
          'NE' : 'Nebraska',
          'NV' : 'Nevada',
          'NH' : 'New Hampshire',
          'NJ' : 'New Jersey',
          'NM' : 'New Mexico',
          'NY' : 'New York',
          'NC' : 'North Carolina',
          'ND' : 'North Dakota',
          'OH' : 'Ohio',
          'OK' : 'Oklahoma',
          'OR' : 'Oregon',
          'PA' : 'Pennsylvania',
          'RI' : 'Rhode Island',
          'SC' : 'South Carolina',
          'SD' : 'South Dakota',
          'TN' : 'Tennessee',
          'TX' : 'Texas',
          'UT' : 'Utah',
          'VT' : 'Vermont',
          'VA' : 'Virginia',
          'WA' : 'Washington',
          'WV' : 'West Virginia',
          'WI' : 'Wisconsin',
          'WY' : 'Wyoming',
          'DC' : 'District of Columbia',
          'AS' : 'American Samoa',
          'GU' : 'Guam',
          'MP' : 'Northern Mariana Islands',
          'PR' : 'Puerto Rico',
          'UM' : 'United States Minor Outlying Islands',
          'VI' : 'Virgin Islands', # , U.S.
}

def get_state(label: str = "",
              location: str = ""):
    """
    Extract the state name from the address or label.
    Use for entities located in the United States.
    """
    regex_state = f"\b({'|'.join(STATES.values())})\b"
    regex_state = re.compile(regex_state, flags = re.DOTALL)
    state1 = re.findall(regex_state, str(label))
    state2 = re.findall(regex_state, str(location))
    if state1 and state2:
        if state1 != state2:
            return None
    elif state1:
        return state1
    elif state2:
        return state2
    return None


# AAS (Space, Airborne), SPASE (Earth, Heliosphere)
# TODO FIXME already done in SPASE. Should already work without SPACE_LOCATION check, checked in update.py.
# TODO: check in this function instead of update.py (require type argument to the function to be added?)
SPACE_LOCATION = ["Earth.Magnetosheath",
                  "Earth.Magnetosphere",
                  "Earth.Magnetosphere.Magnetotail",
                  "Earth.Magnetosphere.Main",
                  "Earth.Magnetosphere.Polar",
                  "Earth.Magnetosphere.RadiationBelt",
                  "Earth.Moon",
                  "Earth.NearSurface",
                  "Earth.NearSurface.Atmosphere",
                  "Earth.NearSurface.AuroralRegion",
                  "Earth.NearSurface.EquatorialRegion",
                  "Earth.NearSurface.Ionosphere",
                  "Earth.NearSurface.Ionosphere.DRegion",
                  "Earth.NearSurface.Ionosphere.ERegion",
                  "Earth.NearSurface.Ionosphere.FRegion",
                  "Earth.NearSurface.Ionosphere.Topside",
                  "Earth.NearSurface.Mesosphere",
                  "Earth.NearSurface.Plasmasphere",
                  "Earth.NearSurface.PolarCap",
                  "Earth.NearSurface.Stratosphere",
                  "Earth.NearSurface.Thermosphere",
                  # "Earth.Surface"
                  "Space",
                  "Airborne"]


@timeall
def get_location_info(label: Optional[str] = None,
                      location: Optional[str] = None,
                      address: Optional[str] = None,
                      country: Optional[str] = None,
                      latitude: Optional[float]  = None,
                      longitude: Optional[float]  = None,
                      #part_of: Optional[dict] = None,
                      language: str = "en",
                      retries: int = 2,
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

    Args:
        label: the label of the entity
        location: the location string as in the source
        address: the address as in the source
        latitude: a float of the latitude of the entity if on earth
        longitude: a float of the longitude of the entity if on earth
        part_of: data dictionary of the entity which the entity is a part of
        language: language in which to retrieve addresses
        retries: how many retries left if the first geopy request failed
        from_cache: do not request geopy again. Set to False for debug only
    """
    ### Step 1: loading, preliminary data casting
    global location_infos
    if location_infos is None:
        load_location_infos_from_cache()
    result = None
    saved_in = ""
    result_dict = {"location": "Earth"}
    if isinstance(location, str):
        location = [location]
    if location:
        for l in location:
            l = l.strip()
            if l in SPACE_LOCATION:
                return dict()
    #if not isinstance(part_of, list):
    #    part_of = [part_of]
    if isinstance(country, set):
        country = list(country)[0]
    if isinstance(address, set):
        address = list(address)[0]
    if isinstance(latitude, list):
        if len(latitude) == 0:
            latitude = None
        else:
            latitude = latitude[0]
    elif isinstance(latitude, set):
        if len(latitude) == 0:
            latitude = None
        else:
            latitude = list(latitude)[0]
    if isinstance(longitude, list):
        if len(longitude) == 0:
            longitude = None
        else:
            longitude = longitude[0]
    elif isinstance(longitude, set):
        if len(longitude) == 0:
            longitude = None
        else:
            longitude = list(longitude)[0]
    if longitude:
        # Always between -180 and 180
        longitude = float(longitude)
        if longitude > 180 or longitude < -180:
            longitude = (float(longitude) % 360 + 540) % 360 - 180

    if not country:
        if address:
            for add in address:
                country = add.split(',')[-1].strip()
                _get_continent_from_country(country,
                                            "address",
                                            add,
                                            result_dict,
                                            longitude)
                _get_state_from_address(result_dict,
                                        address,
                                        label,
                                        location)
                if "country" in result_dict:
                    country = result_dict["country"]
                    break

    # Remove initial "the" from the label as it performs very bad with geopy
    if label and label.lower().startswith("the "):
        label = label[4:].strip()
    # Remove parenthesis and inside as well
    label = remove_parenthesis(label)

    ### Step 2: lookup in the cache
    # Return information if already in the cache
    if from_cache:
        result = _get_location_from_cache(label = label,
                                          location = location,
                                          address = address,
                                          country = country,
                                          latitude = latitude,
                                          longitude = longitude)
                                          #part_of = part_of)
        if result:
            return result

    ### Step 3: go through every parameter and send requests.
    # Get information with geolocator
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
        # Call get_location_info with part_of
        """
        if result is None and part_of:
            for part in part_of:
                if part is None:
                    continue
                if type(part) == str:
                    result = get_location_info(label = part)
                else:
                    result = get_location_info(label = part.get("label", None),
                                               location = part.get("location", None),
                                               latitude = part.get("latitude", None),
                                               longitude = part.get("longitude", None),
                                               address = part.get("address", None))
                if result:
                    return result
                else:
                    result = None
        """
        if result is None and location:
            for loc in location: # Can have more than one location
                if loc is None or loc.lower().startswith("earth"):
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
                # retrieved = False
                for keyword in ["antenna", "observatory", "telescope"]:
                    # Some facilities's locations are known by adding
                    # a keyword to the label. It occurs when the label
                    # is shortened (e.g. "La Silla" for "La Silla Observatory")
                    #saved_in2 = f"geocode/{label} {keyword}"
                    #result = location_infos.get(saved_in2, None)
                    if result is None:
                        result = geolocator.geocode(label + " " + keyword,
                                                    exactly_one=True,
                                                    language=language)
                    #location_infos[saved_in2] = location_infos
                    #if result and label.lower() in str(result).lower():
                    #    retrieved = True
                    if result:
                        break
                    else:
                        location_infos[saved_in] = {}
                #if not retrieved:
                #    location_infos[saved_in] = {}
                #    return {}
            if result:
                result_dict["location_confidence"] = 0.25

        if result is None and country:
            saved_in = "country/" + str(country)
            if "continent" not in result_dict:
                # Already called this in the attributes preprocessing step
                _get_continent_from_country(country,
                                            location_type = "country",
                                            address = address,
                                            result_dict = result_dict,
                                            longitude = longitude)
                _get_state_from_address(result_dict,
                                        address,
                                        label,
                                        location)
            if result_dict.get("continent", None):
                result_dict["location_confidence"] = 1.0
                location_infos[saved_in] = result_dict
                return result_dict

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
        if retries < 0: # If there is an address, we will try to extract a county and continent from it.
            if not address:
                location_infos[saved_in] = {}
                return {}
        else:
            print(f"Warning: {e}.\n{retries} retries left for {label}. Retrying...")
            return get_location_info(label=label,
                                     location=location,
                                     address=address,
                                     latitude=latitude,
                                     longitude=longitude,
                                     retries=retries)

    if not result and (address or country):
        # Try to get country and continent & add them in the result dict
        for add in address:
            country = add.split(',')[-1].strip()
            if not country:
                continue
            if "continent" not in result_dict:
                _get_continent_from_country(country,
                                            "address",
                                            address,
                                            result_dict,
                                            longitude)
                _get_state_from_address(result_dict,
                                        address,
                                        label,
                                        location)
            if "continent" in result_dict:
                location_infos[saved_in] = result_dict
                return result_dict
        location_infos[saved_in] = result_dict
        return result_dict

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
    # country = None
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

    # Get the continent from the country & standardize country name
    if "continent" not in result_dict:
        _get_continent_from_country(country,
                                    location_type,
                                    address,
                                    result_dict,
                                    longitude)
        _get_state_from_address(result_dict,
                                address,
                                label,
                                location)

    # Save the result for future calls
    location_infos[saved_in] = result_dict
    return result_dict


def _get_continent_from_country(country: str,
                                location_type,
                                address,
                                result_dict,
                                longitude):
    """
    Add the continent to the result dict.
    Standardize the country name (USA -> United States)
    Also get the state if in the USA.
    """
    country_code = None
    if location_type != "continent" and country:
        if address and "country_code" in address:
            country_code = address["country_code"].upper()
        else:
            try:
                if type(country) == set:
                    country = list(country)[0]
                elif type(country) == list:
                    country = country[0]
                country_code = pycountry_convert.country_name_to_country_alpha2(country.title())
            except KeyError:
                # Country name does not exist.
                country = None
                country_code = None
                if not address:
                    result_dict["continent"] = "Antarctica"
        if country:
            result_dict["country"] = country

        if country_code:
            # result_dict["country_code"] = country_code
            if country_code == "RU": # Russia
                continent = "Europe" if longitude < 59 else "Asia"
            else:
                if country_code == "AQ":
                    continent = "Antarctica"
                else:
                    continent_code = pycountry_convert.country_alpha2_to_continent_code(country_code)
                    continent = continent_dict[continent_code]
            # result_dict["continent_code"] = continent_code
            result_dict["continent"] = continent

            # Get country name from country code (standardize country name)
            try:
                country_std = pycountry_convert.country_alpha2_to_country_name(country_code)
                result_dict["country"] = country_std
            except:
                print(f"Warning: pycontry_convert.country_alpha2_to_country_name does not have {country_code}. ")


def _get_state_from_address(result_dict,
                            address,
                            label,
                            location):
        # Get the State
        country = result_dict.get("country", None)
        if country == "United States":
            if address and "state" in address:
                result_dict["state"] = address["state"]
            else:
                state = get_state(label = label, location = location)
                if state:
                    result_dict["state"] = state


@timeall
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

    Args:
        latlong1: tuple (latitude, longitude) of the first point.
        latlong2: tuple (latitude, longitude) of the second point.
    """
    return geodesic(latlong1, latlong2).km
    #return math.sqrt((latlong1[0] - latlong2[0])**2 + (latlong1[1] - latlong2[1])**2)


def _get_location_from_cache(label: Optional[str] = None,
                             location: Optional[list[str]] = None,
                             address: Optional[str] = None,
                             country: Optional[str] = None,
                             latitude: Optional[float]  = None,
                             longitude: Optional[float]  = None):
                             #part_of: Optional[dict] = None):
    """
    If any information exists in the cache for any of the non-empty
    parameter, returns the location information dict.

    Args:
        label: the label of the entity
        location: the location string as in the source
        address: the address as in the source
        latitude: a float of the latitude of the entity if on earth
        longitude: a float of the longitude of the entity if on earth
        part_of: data dictionary of the entity which the entity is a part of
    """
    latlong_empty = False
    address_empty = False
    country_empty = False
    location_empty = False
    #part_of_empty = False
    label_empty = False

    if (latitude is not None and longitude is not None and
            (latitude != 0 or longitude != 0)):
        saved_in = "latlong/" + str(latitude) + '/' + str(longitude)
        data = location_infos.get(saved_in, None)
        if data is not None:
            data["location_confidence"] = 1
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

    only_none = True
    """
    if part_of:
        if type(part_of) == dict:
            part_of = [part_of]
        for part in part_of:
            if part is None:
                continue
            only_none = False
            if type(part) == str:
                data = _get_location_from_cache(label = part)
            else:
                data = _get_location_from_cache(label = part.get("label", None),
                                                location = part.get("location", None),
                                                address = part.get("address", None),
                                                latitude = part.get("latitude", None),
                                                longitude = part.get("longitude", None)
                                                )
            if data:
                return data
    else:
        part_of_empty = True
    part_of_empty = part_of_empty or only_none
    """

    only_none = True
    if location:
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

    if country:
        saved_in = "country/" + str(country)
        data = location_infos.get(saved_in, None)
        if data is not None and data:
            data["location_confidence"] = 1.0
            return data
        elif data == {}:
            country_empty = True
    else:
        country_empty = True

    # If the cache's data was empty for any of the provided information
    if latlong_empty and location_empty and address_empty and label_empty and country_empty: # and part_of_empty:
        return {}
    return data
