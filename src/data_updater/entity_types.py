"""
Types (superclasses) names in the OBS namespace.
Those types are mostly used for categorisation purposes and to
manage lists and entities' compatibility during the merging step.
"""

# Pretty bad classifier:
#from transformers import pipeline
# xnli_classifier = pipeline("zero-shot-classification", model="MoritzLaurer/mDeBERTa-v3-base-mnli-xnli")

# Using LLM with Ollama: test gemma3:1b
# llm_classifier = None

OBSERVATION_FACILITY = "observation facility" # unused for classification
GROUND_OBSERVATORY = "ground observatory" # contain ground observatory network
SPACECRAFT = "spacecraft" # = space observatory
TELESCOPE = "telescope" # Space or Ground. = observation instruments: inside a telescope.
AIRBORNE = "airborne" # in the atmosphere
MISSION = "space mission" # More than one spacecraft in 1 mission: ~= observatory network ?
SURVEY = "survey" # data produced by a mission. A mission can produce more than one survey. Surveys are linked to databases.
# mission contains an instrument host (spacecrafts, ...)
# Other category: Lander / Rover: on Mars, on a comet, asteroid... (considered same as spacecraft, everything that is not on ground)
UFO = "unknown" # Unknown type (unrelated to our observation facilities)
INSTRUMENT = "instrument" # Not used in ALL_TYPES, only for PDS that makes links to instruments
# MISC = "miscellaneous"
ERROR = "error" # LLM error in return format

# Types except ERROR
ALL_TYPES = {
            OBSERVATION_FACILITY,
            GROUND_OBSERVATORY,
            TELESCOPE,
            SPACECRAFT,
            AIRBORNE,
            MISSION,
            UFO
            }


# A telescope may have an address if it is located in an observatory.
# An observatory network may be a telescope array with only one location.
MAY_HAVE_ADDR = {
                OBSERVATION_FACILITY,
                GROUND_OBSERVATORY,
                TELESCOPE
                }

NO_ADDR = {
    MISSION,
    SPACECRAFT,
    AIRBORNE
}

# Types that can not co-exist with GROUND_OBSERVATORY
SPACE_TYPES = {
    SPACECRAFT,
    AIRBORNE
}

# Labels used to classify entities with the model to project's labels
categories_by_descriptions = {"ground observatory": GROUND_OBSERVATORY,
                            "research institute": GROUND_OBSERVATORY,
                            "university": GROUND_OBSERVATORY,
                            "ground station": GROUND_OBSERVATORY,
                            "spacecraft/space probe": SPACECRAFT,
                            #"space probe": SPACECRAFT,
                            "airborne": AIRBORNE,
                            "cosmos observation instrument": TELESCOPE,
                            "telescope": TELESCOPE,
                            "space telescope": TELESCOPE,
                            "space mission": MISSION,
                            "space investigation": MISSION,
                            "unknown": UFO,
                            "miscellaneous": UFO,
                            # "military facility": UFO
                            }


def get_reversed(self,
                    category: str) -> str:
    """
    Return the keys that correspond to the category
    between " or ". Used in the prompt.
    """
    res = []
    for description, cat in self.categories_by_descriptions.items():
        if cat == category:
            res.append(description)
    return " or ".join(res)


def to_string(data: dict,
              exclude: list[str] = ["code",
                                    "url",]) -> str:
    """
    Utility function.

    Convert an entity's data dict into its string representation.
    Keys are sorted so that the generated string is always the same.

    Exclude entries from the data to ignore values that will not help LLM,
    such as any URL/URI, codes, etc.

    Keyword arguments:
    data -- the entity data dict
    exclude -- dict entries to exclude
    """
    res = data["label"] + '. '
    for key, value in sorted(data.items()):
        if key in exclude:
            continue
        if key == "label":
            continue
        if type(value) not in [list, set, tuple]:
            value = [value]
        if key == "alt_label":
            key = "Also known as"
        else:
            key = key.replace('_', ' ').capitalize()
        res += f"{key}: {', '.join([str(v)[:512] for v in value])}. "
    return res

"""
def _classify_xnli(text):
    output = xnli_classifier(text, list(categories.keys()), multi_label=False)
    type = categories[output['labels'][0]]
    return type
"""