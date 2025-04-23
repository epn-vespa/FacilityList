"""
Types (superclasses) names in OBS.
"""
import atexit
import json
import requests

from utils import config # for ollama

# Ollama Configuration
OLLAMA_HOST = 'http://localhost:11434'
# MODEL = 'gemma3:1b'
# MODEL = 'gemma3:4b'
MODEL = 'gemma3:12b' # 8.1GB, 12 Billion parameters. Very slow. Only use on powerful servers.
# MODEL = 'llama3.1'

# Pretty bad classifier:
#from transformers import pipeline
# xnli_classifier = pipeline("zero-shot-classification", model="MoritzLaurer/mDeBERTa-v3-base-mnli-xnli")

# Using LLM with Ollama: test gemma3:1b
llm_classifier = None

OBSERVATION_FACILITY = "observation facility" # network
GROUND_OBSERVATORY = "ground observatory"
OBSERVATORY_NETWORK = "observatory network" # ground or space
SPACECRAFT = "spacecraft" # = space observatory
AIRBORNE = "airborne" # in the atmosphere
TELESCOPE = "telescope" # Space or Ground. = observation instruments: inside a telescope.
MISSION = "space mission" # More than one spacecraft in 1 mission: ~= observatory network ?
# mission contains an instrument host (spacecrafts, ...)
# Other category: Lander / Rover: on Mars, on a comet, asteroid... (maybe same as spacecraft, everything that is not on ground)
UFO = "unknown" # Unknown type (unrelated to our observation facilities)
# MISC = "miscellaneous"
ERROR = "error" # LLM error in return format


# Category labels used to classify entities with the XNLI model
categories_by_descriptions = {"ground observatory": GROUND_OBSERVATORY,
                              "research institute": GROUND_OBSERVATORY,
                              "university": GROUND_OBSERVATORY,
                              "ground station": GROUND_OBSERVATORY,
                              "observatory network": OBSERVATORY_NETWORK,
                              "telescopes array": OBSERVATORY_NETWORK,
                              "spacecraft/space probe": SPACECRAFT,
                              #"space probe": SPACECRAFT,
                              "airborne": AIRBORNE,
                              "cosmos observation instrument": TELESCOPE,
                              "telescope": TELESCOPE,
                              "space mission": MISSION,
                              "space investigation": MISSION,
                              "earth observation probe": UFO,
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
    Convert an entity's data into a dictionary.

    Keyword arguments:
    data -- the entity data dict
    exclude -- columns to exclude
    """
    res = data["label"] + '. '
    for key, value in data.items():
        if key in exclude:
            continue
        if key == "label":
            continue
        if type(value) not in [list, set, tuple]:
            value = [value]
        if key == "alt_label":
            key = "Also known as"
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
path = config.cache_dir / "llm_categories.json"
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

def load_llm_results_from_cache():
    atexit.register(_save_llm_results_in_cache)
    global llm_categories
    path = config.cache_dir / "llm_categories.json"
    if not path.exists():
        return
    path = str(path)
    with open(path, "r", encoding = "utf-8") as f:
        llm_categories = json.load(f)


def classify(text: str,
             choices: list[str] = None,
             from_cache: bool = True):
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
    """
    global llm_categories
    if from_cache and not llm_categories:
        load_llm_results_from_cache()

    if from_cache and llm_categories and text in llm_categories:
        category = llm_categories[text]
        if category != ERROR:
            return llm_categories[text]

    if not choices:
        choices = categories_by_descriptions.keys()

    possible_categories = set()
    llm_choices = set()
    for key, value in categories_by_descriptions.items():
        for choice in choices:
            if choice == key:
                possible_categories.add(value)
                llm_choices.add(key)
    """
    prompt = "Only return the category, do not explain. "
    prompt += "Do not create a category from the text or your prior knowledge. "
    if GROUND_OBSERVATORY in possible_categories:
        prompt += f"Universities, bases, laboratories, stations and observatories (or Obs.) are usually {get_reversed(GROUND_OBSERVATORY)}. "
    if TELESCOPE in possible_categories:
        prompt += f"Cosmos observation instruments and {get_reversed(TELESCOPE)} can have a wavelength and a size. "
    if MISSION in possible_categories:
    #    prompt += "Ground observation facilities, instruments can be part of a space mission. "

        prompt += f"{get_reversed(MISSION)} aim at observing celest objects and might have observation wavelength. "
    if SPACECRAFT in possible_categories:
        prompt += f"{get_reversed(SPACECRAFT)} are in orbitation or in the space. "
    if UFO in possible_categories:
        prompt += f"Everything that is not for cosmos observation, such as weather or geographic facilities, space debris or other objects are {get_reversed(UFO)}."
        prompt += f"If you are unsure, always return {UFO}. "
    prompt += f"Return a label from the list : [{','.join(choices)}].\n\n"
    prompt += f"Text to classify: {text}"

    # Old prompt V2
    prompt = "Only return the category, do not explain. "
    prompt += "Do not create a category."# or your prior knowledge. "
    # prompt += "To help yourself decide, first identify the described facility's noun using POS. "
    prompt += "University, base, laboratory, station, observatory (Obs.) are ground observatories. "#observation facilities. "
    prompt += "Cosmos observation instruments such as telescopes can have a wavelength and a size and may be located somewhere. " # in an observatory
    # prompt += "Space mission are observation mission that may require the cooperation of observatories, telescopes etc."
    # prompt += f"Space missions are a collaboration between researchers to achieve the exploration of a cosmos object or event. "
    prompt += f"Space missions are planned to achieve the exploration of a cosmos object or event. "
    prompt += "Spacecrafts or space probes are orbiting earth or other cosmos object. "
    prompt += "Objects that are not cosmos observation facilities, such as weather or geographic facilities, debris etc are unknown. "
    prompt += "If you are unsure, return unknown. "
    # prompt += "If you do not have enough information (text too short and no evident keyword), always return unkown. "
    #prompt += f"Return a label from the list : [{','.join(categories_by_descriptions.keys())}].\n\n"
    prompt += f"Return a label from the list :\n-{'\n-'.join(set(categories_by_descriptions.keys()))}\n\n"
    prompt += f"Text to classify: {text}"
    """

    prompt = "Only return the category, do not explain. "
    prompt += "Do not create a category from the text or your prior knowledge. "
    if GROUND_OBSERVATORY in possible_categories:
        prompt += "University, station and observatory (obs) are ground observation facilities (on earth). "
    if OBSERVATORY_NETWORK in possible_categories:
        prompt += "A group of more than one observatory is an observatory network. "
        prompt += "A group of more than one telescope is a telescope array. "
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
    if UFO in possible_categories:
        prompt += "Everything that is not space observation, such as weather or geographic facilities, space debris or other objects are miscellaneous. "
        prompt += "If you are unsure, always return unknown. "
        prompt += "If you lack information in the text to classify, always return unknown. "
    # prompt += f"Return a label from the list : [{','.join(categories_by_descriptions.keys())}].\n\n"
    # prompt += f"Return a category from the list : \n-{'\n-'.join(llm_choices)}\n\n"

    prompt += f"Categories : \n-{'\n-'.join(llm_choices)}\n\n"


    prompt += f"Text to classify: {text}"
    print(prompt)


    response = requests.post(
        f'{OLLAMA_HOST}/api/generate',
        json={
            'model': MODEL,
            'prompt': prompt,
            'stream': False
        }
    )
    if response.ok:
        cat = response.json()['response'].strip().lower()
        print("cat=", cat)# categories_by_descriptions[cat])
        if cat not in categories_by_descriptions:
            print(f"Error: the Ollama model did not return an category from :\n" +
                  f"{','.join(categories_by_descriptions.keys())}.\n" +
                  f"It returned {cat} instead.\n " +
                  f"Return {UFO} for prompt \"{prompt}\"")
            return UFO
        if cat in categories_by_descriptions:
            cat = categories_by_descriptions[cat]
        llm_categories[text] = cat
        return cat
    else:
        llm_categories[text] = ERROR
        print(f"Ollama error: {response.text}.\nReturn {UFO} for prompt \"{prompt}\"")
        return None
