"""
Types (superclasses) names in the OBS namespace.
Those types are mostly used for categorisation purposes and to
manage lists and entities' compatibility during the merging step.
"""
import atexit
import json
import requests
from config import OLLAMA_HOST, OLLAMA_MODEL, OLLAMA_TEMPERATURE, CACHE_DIR # type: ignore

# Pretty bad classifier:
#from transformers import pipeline
# xnli_classifier = pipeline("zero-shot-classification", model="MoritzLaurer/mDeBERTa-v3-base-mnli-xnli")

# Using LLM with Ollama: test gemma3:1b
llm_classifier = None

OBSERVATION_FACILITY = "observation facility" # network
GROUND_OBSERVATORY = "ground observatory"
OBSERVATORY_NETWORK = "observatory network" # ground or space
SPACECRAFT = "spacecraft" # = space observatory
TELESCOPE = "telescope" # Space or Ground. = observation instruments: inside a telescope.
AIRBORNE = "airborne" # in the atmosphere
MISSION = "space mission" # More than one spacecraft in 1 mission: ~= observatory network ?
# mission contains an instrument host (spacecrafts, ...)
# Other category: Lander / Rover: on Mars, on a comet, asteroid... (maybe same as spacecraft, everything that is not on ground)
UFO = "unknown" # Unknown type (unrelated to our observation facilities)
# MISC = "miscellaneous"
ERROR = "error" # LLM error in return format

# Types except ERROR
ALL_TYPES = {
             OBSERVATION_FACILITY,
             GROUND_OBSERVATORY,
             OBSERVATORY_NETWORK,
             TELESCOPE,
             SPACECRAFT,
             AIRBORNE,
             MISSION,
             UFO
            }

GROUND_TYPES = {
                GROUND_OBSERVATORY,
               }

# A telescope may have an address if it is located in an observatory.
# An observatory network may be a telescope array with only one location.
MAY_HAVE_ADDR = {
                 OBSERVATION_FACILITY,
                 OBSERVATORY_NETWORK,
                 TELESCOPE
                }

# Labels used to classify entities with the model to project's labels
categories_by_descriptions = {"ground observatory": GROUND_OBSERVATORY,
                              "research institute": GROUND_OBSERVATORY,
                              "university": GROUND_OBSERVATORY,
                              "ground station": GROUND_OBSERVATORY,
                              "observatory network": OBSERVATORY_NETWORK,
                              "telescope array": OBSERVATORY_NETWORK,
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

def get_reversed(category: str) -> str:
    """
    Return the keys that correspond to the category
    between " or ". Used in the prompt.
    """
    res = []
    for description, cat in categories_by_descriptions.items():
        if cat == category:
            res.append(description)
    return " or ".join(res)

# reversed_categories = {x: y for y, x in categories.items()}

# OBSERVATION_FACILITY
#   MISSION
#   GROUND_OBSERVATORY
#       TELESCOPE
#   SPACECRAFT
# UFO
def to_string(data: dict,
              exclude: list[str] = ["code",
                                    "url",]) -> str:
    """
    Convert an entity's data into its string representation.
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
        res += f"{key}: {', '.join([str(v) for v in value])}. "
    return res

"""
def _classify_xnli(text):
    output = xnli_classifier(text, list(categories.keys()), multi_label=False)
    type = categories[output['labels'][0]]
    return type
"""

# File to save the LLM's results in
path = CACHE_DIR / "llm_categories.json"
llm_categories = dict()


def _save_llm_results_in_cache():
    global llm_categories
    if llm_categories:
        if not path.parent.exists():
            path.parent.mkdir(parents = True, exist_ok = True)
        # path = str(path / "llm_categories.json")
        print(f"dumping {len(llm_categories)} LLM results in {path}.")
        with open(path, "w", encoding = "utf-8") as f:
            json.dump(llm_categories, f, indent=" ")


def _load_llm_results_from_cache():
    atexit.register(_save_llm_results_in_cache)
    global llm_categories
    path = CACHE_DIR / "llm_categories.json"
    if not path.exists():
        return
    path = str(path)
    with open(path, "r", encoding = "utf-8") as f:
        llm_categories = json.load(f)


def classify(text: str,
             choices: list[str] = None,
             from_cache: bool = True,
             cache_key: str = None):
    """
    Use a LLM to classify a text to one of the categories.
    Return the category's string that can be used as a superclass of the
    entity. If there was an error, will it return the UFO category.

    Classify an entity's textual representation. If no choices
    are provided, it will use :
        OBSERVATION_FACILITY (default)
        GROUND_OBSERVATORY
        OBSERVATORY_NETWORK
        SPACECRAFT
        TELESCOPE
        MISSION
        UFO (Unknown/miscellaneous)
        ERROR (LLM error)

    Keyword arguments:
    description -- the string representation of an entity.
    choices -- the list of categories to classify from.
               Set carefully accordingly to the data in a list.
    from_cache -- whether to retrieve the category from a previous LLM run.
    cache_key -- key of the entity in the cache dict. List name + uri.
    """
    global llm_categories
    if from_cache and not cache_key:
        raise ValueError("Provided from_cache but not cache_key.")
    if from_cache and not llm_categories:
        _load_llm_results_from_cache()

    if (from_cache and llm_categories and
        cache_key and cache_key in llm_categories):
        category = llm_categories[cache_key]
        if category != ERROR:
            return category # if error, re-compute.

    if not choices:
        choices = categories_by_descriptions.values()

    possible_categories = set()
    llm_choices = set()
    for key, value in categories_by_descriptions.items():
        for choice in choices:
            if choice == value:
                possible_categories.add(value)
                llm_choices.add(key)

    # Preambule
    prompt = "Only return the category, do not explain. "
    prompt += "Do not create a category from the text or your prior knowledge. "

    # Explainations
    if GROUND_OBSERVATORY in possible_categories:
        prompt += "University, station and observatory (obs) are ground observation facilities (on earth). "
    if OBSERVATORY_NETWORK in possible_categories:
        prompt += "More than one observatory is an observatory network. "
        prompt += "More than one telescope is a telescope array. "
    if TELESCOPE in possible_categories:
        prompt += "Cosmos observation instruments and telescopes can have a wavelength, a size, a host observatory etc. "
        # prompt += "Cosmos observation instruments and telescopes are used to observe cosmos bodies, radiations and particles."
        #prompt += "A cosmos observation instrument or telescope is used to observe cosmos bodies, radiations and particles. "
        #prompt += "Cosmos observation instruments and telescopes may belong to an observatory. "
        #prompt += "Cosmos observation instruments and telescopes may also be part of a space mission. "
    if MISSION in possible_categories:
        prompt += f"Space missions are the observation of a cosmos body or event. "
    if AIRBORNE in possible_categories:
        prompt += "Airbornes are planes sent into the atmosphere to make cosmos observations. "
    if SPACECRAFT in possible_categories:
        prompt += "A spacecraft or space probe can also be a satellite that observe cosmos events. "
    if UFO in possible_categories:
        prompt += "Everything that is not space observation, such as weather or geographic probes (or satellites), space debris, telecommunication satellites, military facilities or other objects are miscellaneous. "
        prompt += "If you are unsure, always return unknown. "
        prompt += "If you lack information in the text to classify, always return unknown. "
    # prompt += f"Return a label from the list : [{','.join(categories_by_descriptions.keys())}].\n\n"
    # prompt += f"Return a category from the list : \n-{'\n-'.join(llm_choices)}\n\n"

    # Categories
    prompt += f"Categories : \n-{'\n-'.join(llm_choices)}\n\n"

    # Entity representation
    prompt += f"Text to classify: {text}"
    print(prompt)


    response = requests.post(
        f'{OLLAMA_HOST}/api/generate',
        json={
            'model': OLLAMA_MODEL,
            'prompt': prompt,
            'stream': False,
            'temperature': OLLAMA_TEMPERATURE, # low temperature = more determinist. Default = 0.8
        }
    )
    if response.ok:
        cat = response.json()['response'].strip().lower()
        cat = cat.lstrip('-').lstrip()
        print("cat=", cat)# categories_by_descriptions[cat])
        if cat in categories_by_descriptions:
            cat = categories_by_descriptions[cat]
        else:
            print(f"Error: the Ollama model did not return an category from :\n" +
                  f"{','.join(categories_by_descriptions.keys())}.\n" +
                  f"It returned {cat} instead.\n " +
                  f"Return {UFO} for prompt \"{prompt}\"")
            llm_categories[cache_key] = ERROR
            return UFO
        llm_categories[cache_key] = cat
        return cat
    else:
        llm_categories[cache_key] = ERROR
        print(f"Ollama error: {response.text}.\nReturn {UFO} for prompt \"{prompt}\"")
        return None
