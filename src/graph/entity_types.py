"""
Types (superclasses) names in the OBS namespace.
Those types are mostly used for categorisation purposes and to
manage lists and entities' compatibility during the merging step.

TODO: attach telescopes & classes to https://www.w3.org/TR/vocab-ssn/

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

# Pretty bad classifier:
#from transformers import pipeline
# xnli_classifier = pipeline("zero-shot-classification", model="MoritzLaurer/mDeBERTa-v3-base-mnli-xnli")

# Using LLM with Ollama: test gemma3:1b
# llm_classifier = None

from rdflib import URIRef
from utils.string_utilities import uri_to_str

class AutoStringMeta(type):
    def __repr__(cls):
        return cls._label
    def __str__(cls):
        return cls._label
    def __call__(cls):
        return cls._label

class Ufo(metaclass=AutoStringMeta):
    """
    Any kind of entity. Use this class for unclassified entities.
    """
    _label = "unknown"
    def __str__(self):
        return "unknown"
UFO = Ufo()

class Platform(Ufo):
    """
    Platforms can host telescopes and instruments.
    """
    _label = "platform"
PLATFORM = Platform()

class Instrument(Ufo):
    """
    An instrument is hosted by an observatory or a spacecraft (platform).
    But it can also be the component of a telescope.
    An instrument is not a platform.
    """
    _label = "instrument"
INSTRUMENT = Instrument()

class ObservationFacility(Ufo):
    """
    Any kind of observation facility.
    """
    _label = "observation facility"
OBSERVATION_FACILITY = ObservationFacility()

class GroundFacility(ObservationFacility):
    """
    Facilities that are on the surface on the earth and have an address,
    unless they are located in Antarctica or on the ocean.
    """
    _label = "ground facility"
GROUND_FACILITY = GroundFacility()

class SpaceFacility(ObservationFacility):
    _label = "space facility"
SPACE_FACILITY = SpaceFacility()

class GroundObservatory(GroundFacility, Platform):
    """
    Can be a station, a ground observatory network (composed of plural ground facilities).
    It usually has an address and can host telescopes.
    """
    _label = "ground observatory"
GROUND_OBSERVATORY = GroundObservatory()

class Telescope(GroundFacility, SpaceFacility, Instrument):
    """
    Solar observatories are telescopes.
    A telescope usually have an aperture.
    A telescope can be part of an observatory or spacecraft (platform).
    Some telescopes are confused with instruments, therefore they are subclass of each other.
    """
    _label = "telescope"
TELESCOPE = Telescope()

class Spacecraft(SpaceFacility, Platform):
    """
    Landers and Rovers are considered the same as spacecraft
    """
    _label = "spacecraft"
SPACECRAFT = Spacecraft()

class Airborne(SpaceFacility, Platform):
    """
    Platform operating on the atmosphere
    """
    _label = "airborne"
AIRBORNE = Airborne()

class Investigation(Spacecraft):
    """
    Sometimes there is a confusion between space missions and spacecraft.
    Therefore, to allow mapping spacecraft with space missions, they are subclass of each other.
    """
    _label = "space mission"
INVESTIGATION = Investigation()

class Survey(ObservationFacility):
    """
    Data produced by a mission. A mission can produce more than one survey. Surveys are linked to databases.
    """
    _label = "survey"
SURVEY = Survey()

class Error(Ufo):
    """
    LLM error in return format
    """
    _label = "error"
ERROR = Error()


# Types that are used in lists and therefore that can be used in mappings
ALL_TYPES = {
            SPACE_FACILITY: SpaceFacility,
            GROUND_FACILITY: GroundFacility,
            OBSERVATION_FACILITY: ObservationFacility,
            GROUND_OBSERVATORY: GroundObservatory,
            TELESCOPE: Telescope,
            INSTRUMENT: Instrument,
            SPACECRAFT: Spacecraft,
            AIRBORNE: Airborne,
            SURVEY: Survey,
            INVESTIGATION: Investigation,
            UFO: Ufo
            }


# A telescope may have an address if it is located in an observatory.
# An observatory network may be a telescope array with only one location.
MAY_HAVE_ADDR = {
    GROUND_FACILITY,
    GROUND_OBSERVATORY,
    TELESCOPE
    }

NO_ADDR = ALL_TYPES.keys() - MAY_HAVE_ADDR

# Types that can not co-exist with GROUND_OBSERVATORY
SPACE_TYPES = {
    SPACECRAFT,
    AIRBORNE,
    SPACE_FACILITY
}

def get_types_intersections(types1: set[str] | set[URIRef],
                            types2: set[str] | set[URIRef]) -> set[str]:
    """
    Types are compatible with each other even if they are not the same.

    This method was made to replace types1.intersection(types2) by
    a more robust method that would consider mapping a Ground Observatory
    with a Ground Facility as possible.

    Args:
        types1: possible types for the first list
        types2: possible types for the second list
    """
    intersection = set()
    for type1 in types1:
        for type2 in types2:
            if type1 == type2:
                intersection.add(type1)
                continue
            if type(type1) != str:
                type1 = uri_to_str(type1)
            if type(type2) != str:
                type2 = uri_to_str(type2)
            t1_ = ALL_TYPES[type1]
            t2_ = ALL_TYPES[type2]
            if issubclass(t1_, t2_) or issubclass(t2_, t1_):
                intersection.add(type1)
                intersection.add(type2)
    return intersection