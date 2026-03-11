"""
LLM connection utility functions

"""
import atexit
import json
import pickle
import re
import os
import requests
import config
from config import OLLAMA_TEMPERATURE, LLM_CATEGORIES_FILE, LLM_EMBEDDINGS_FILE, PROMPT_SAME_DISTINCT, CACHE_DIR
from collections import defaultdict
from graph.entity_types import *


class LLMConnection():
    """
    Singleton class to manage the connection to the LLM.
    It enables sending generate, classify and embed requests to the LLM.
    """

    #_instance = None
    _initialized = False

    # Singleton
    def __new__(cls, *args, **kwds):
        #if not cls._instance and not cls._initialized:
        cls._context_length = dict()
        cls._llm_categories = dict()
        cls._llm_embeddings = dict()
        cls._initialized = True

        return cls # ._instance


    @classmethod
    def _get_llm_context_length(cls,
                                ollama_model: str) -> int:
        """
        Get the context length of a model. A context length is the prompt's
        maximal length.

        Args:
            ollama_model: the model name.
        """

        context_length = cls._context_length.get(ollama_model, 0)
        if context_length:
            return context_length

        response = requests.post(
                f'{config.OLLAMA_HOST}/api/show',
                json = {
                    'model': ollama_model,
                }
        )
        try:
            infos = response.json()['model_info']
            architecture = infos['general.architecture']
            context_length = infos[architecture + '.context_length']
            cls._context_length[ollama_model] = context_length
        except KeyError:
            print(response.json()['error'])
            print("To download an Ollama model, use 'ollama pull [model_name]'.")
            exit(code=1)
        return context_length


    @classmethod
    def _save_llm_categories_in_cache(cls):
        """
        Save the LLM categories in a json file in the cache folder.
        """
        if cls._llm_categories:
            if not LLM_CATEGORIES_FILE.parent.exists():
                LLM_CATEGORIES_FILE.parent.mkdir(parents = True, exist_ok = True)
            print(f"dumping {len(cls._llm_categories)} LLM categories in {str(LLM_CATEGORIES_FILE)}.")
            with open(LLM_CATEGORIES_FILE, "w", encoding = "utf-8") as f:
                json.dump(cls._llm_categories, f, indent = 2)


    @classmethod
    def _load_llm_categories_from_cache(cls):
        """
        Load the LLM categories from a json file in the cache folder.
        """
        atexit.register(cls._save_llm_categories_in_cache)
        if not LLM_CATEGORIES_FILE.exists():
            return
        with open(LLM_CATEGORIES_FILE, "r", encoding = "utf-8") as f:
            try:
                cls._llm_categories = json.load(f)
            except:
                cls._llm_categories = dict()


    @classmethod
    def _save_llm_embeddings_in_cache(cls):
        """
        Dump the LLM embeddings in a pickle file in the cache folder.
        """
        if cls._llm_embeddings:
            LLM_EMBEDDINGS_FILE.parent.mkdir(parents = True, exist_ok = True)
            print(f"dumping {len(cls._llm_embeddings)} LLM embeddings in {str(LLM_EMBEDDINGS_FILE)}.")
            with open(LLM_EMBEDDINGS_FILE, "w") as f:
                pickle.dump(cls._llm_embeddings, f)


    @classmethod
    def _load_llm_embeddings_from_cache(cls):
        """
        Load the LLM embeddings from a pickle file in the cache folder.
        """
        atexit.register(cls._save_llm_embeddings_in_cache)
        if not LLM_EMBEDDINGS_FILE.exists():
            return
        with open(LLM_EMBEDDINGS_FILE, "r") as f:
            try:
                cls._llm_embeddings = pickle.load(f)
            except:
                cls._llm_embeddings = dict()


    @classmethod
    def embed(cls,
              text: str,
              from_cache: bool = False,
              cache_key: str = ""):
        """
        Get the embeddings of the provided text.

        Args:
            text: the textual represnetation of an entity to embed.
            from_cache: whether to retrieve embeddings from a previous LLM run.
            cache_key: key of the entity in the cache dict. List name + uri.
        """
        if from_cache and not cache_key:
            raise ValueError("Provided from_cache but not cache_key.")
        if (from_cache and not cls._llm_embeddings):
            cls._load_llm_embeddings_from_cache()
        if (from_cache and cls._llm_embeddings):
            embeddings = cls._llm_embeddings.get(cache_key, None)
            if embeddings:
                return embeddings # if error, re-compute.
        prompt = "Represent this entity for search: " + text
        response = requests.post(
            f"{config.OLLAMA_HOST}/api/embeddings",
            json={
                "model": config.OLLAMA_MODEL,
                "prompt": prompt
                }
        )
        if response.ok:
            embeddings = response.json()["embedding"]
            cls._llm_embeddings[cache_key] = embeddings
            return embeddings
        else:
            cls._llm_embeddings[cache_key] = None
            print(f"Ollama error: {response.text}.\nReturn None for prompt \"{prompt}\"")
            return None

 
    # Labels used to classify entities with the model to project's labels
    _categories_by_descriptions = {"ground observatory": GROUND_OBSERVATORY,
                                   "research institute": GROUND_OBSERVATORY,
                                   "university": GROUND_OBSERVATORY,
                                   "ground station": GROUND_OBSERVATORY,
                                   "spacecraft": SPACECRAFT,
                                   "space probe": SPACECRAFT,
                                   "airborne": AIRBORNE,
                                   "space plane": AIRBORNE,
                                   "balloon": AIRBORNE,
                                   "cosmos observation instrument": TELESCOPE,
                                   "telescope": TELESCOPE,
                                   "space telescope": TELESCOPE,
                                   "space mission": INVESTIGATION,
                                   "space investigation": INVESTIGATION,
                                   "unknown": UFO,
                                   "miscellaneous": UFO,
                                  }

    @classmethod
    def classify(cls,
                 text: str,
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
            SPACECRAFT
            TELESCOPE
            MISSION
            UFO (Unknown/miscellaneous)
            ERROR (LLM error)

        Args:
            description: the string representation of an entity.
            choices: the list of possible categories for the entity.
            from_cache: whether to retrieve the category from a previous LLM run.
            cache_key: key of the entity in the cache dict. List name + uri.
        """
        if from_cache and not cache_key:
            raise ValueError(f"Provided from_cache but not cache_key to function {cls.classify.__name__}.")

        if from_cache and not cls._llm_categories:
            cls._load_llm_categories_from_cache()

        if (from_cache and cls._llm_categories and
            cache_key and cache_key in cls._llm_categories):
            category = cls._llm_categories[cache_key]
            if category != ERROR:
                return category # if error, re-compute.

        if not choices:
            choices = cls._categories_by_descriptions.values()

        possible_categories = set()
        llm_choices = set()
        for key, value in cls._categories_by_descriptions.items():
            if value in choices:
                possible_categories.add(value)
                llm_choices.add(key)

        # Preamble
        prompt = "Only return the category. Do not write anything else. Do not return more than one category."
        prompt += "Do not create a category from the text or your prior knowledge. "

        # Explainations
        if GROUND_OBSERVATORY in possible_categories:
            prompt += "University, station and observatory (obs) are ground observation facilities (on earth). "
        if TELESCOPE in possible_categories:
            prompt += "Cosmos observation instruments and telescopes can have a wavelength, a size, a host observatory etc. "
        if INVESTIGATION in possible_categories:
            prompt += f"Space missions are the observation of a cosmos body or event, often made by a spacecraft, telescope or observatory. "
        if AIRBORNE in possible_categories:
            prompt += "An airborne is a plane or balloon sent into the atmosphere to make cosmos observations. "
        if SPACECRAFT in possible_categories:
            prompt += "A spacecraft or space probe can also be a satellite that observe cosmos events. "
        if UFO in possible_categories:
            prompt += "Unknown is for everything that is not for space observation, such as weather or geographic probes (or satellites), space debris, telecommunication satellites, military facilities, online databases are miscellaneous. "
            prompt += "If you are unsure, always return unknown. "
            prompt += "If you lack information in the text to classify, always return unknown. "

        # Categories
        prompt += f"Categories : \n-{'\n-'.join(llm_choices)}\n\n"

        # Entity representation
        prompt += f"Text to classify: {text}"

        context_length = cls._get_llm_context_length(config.OLLAMA_MODEL)
        if len(prompt) > context_length:
            prompt = prompt[:context_length]

        # Get the category from the LLM
        cat = cls.generate(prompt, config.OLLAMA_MODEL, from_cache = from_cache, cache_key = cache_key)
        cat = cat.lstrip('-').lstrip()
        cat = cat.split("\n")[0] # Some models (such as Gemma) return more than one category
        if cat in cls._categories_by_descriptions:
            cat = cls._categories_by_descriptions[cat]
        else:
            print(f"Error: the Ollama model did not return an category from :\n" +
                f"{','.join(cls._categories_by_descriptions.keys())}.\n" +
                f"It returned {cat} instead.\n " +
                f"Return {UFO} for prompt \"{prompt}\"")
            cls._llm_categories[cache_key] = ERROR
            return UFO
        cls._llm_categories[cache_key] = cat
        return cat



    _generation_cache_loaded = False
    _generation_cache = dict()
    _BACKUP_EVERY = 10
    _backup_countdown = _BACKUP_EVERY
    @classmethod
    def _load_generation_cache(cls,
                              model: str):
        # FIXME if we change model, it will stay loaded in the same cache file
        if cls._generation_cache_loaded:
            return
        cls._generation_cache_filename = CACHE_DIR / f"{model}-generate.json"
        if cls._generation_cache_filename.exists():
            with open(cls._generation_cache_filename, "r") as file:
                cls._generation_cache = json.load(file)
        else:
            cls._generation_cache = dict()
        cls._generation_cache_loaded = True
        atexit.register(cls._save_generation_cache)


    @classmethod
    def _save_generation_cache(cls):
        with open(cls._generation_cache_filename, "w") as file:
            json.dump(cls._generation_cache, file, indent = 2)


    @classmethod
    def generate(cls,
                 prompt: str,
                 model: str,
                 num_predict: int = 256,
                 from_cache: bool = False,
                 cache_key: str = None) -> str:

        """
        Send a simple generate query to the Ollama API.

        Args:
            prompt: the prompt to send to the LLM.
            model: the model to use.
            num_predict: maximum length of the predicted message.
            from_cache: if True, also use a cache_key.
            cache_key: identifier to use to retrieve the response in later runs.
        """
        if from_cache:
            if not cache_key:
                raise ValueError("from_cache provided but no cache_key.")
            cls._load_generation_cache(model)
            response = cls._generation_cache.get(cache_key, None)
            if response:
                return response

        response = requests.post(
            f'{config.OLLAMA_HOST}/api/generate',
            json={
                'model': model,
                'prompt': prompt,
                'stream': False,
                'temperature': OLLAMA_TEMPERATURE, # low temperature = more determinist. Default = 0.8
                'num_predict': num_predict,
            }
        )
        if response.ok:
            response = response.json()['response'].strip()
            response = cls.remove_tags(response)
            if from_cache:
                cls._generation_cache[cache_key] = response
                cls._backup_countdown -= 1
                if cls._backup_countdown == 0:
                    cls._save_generation_cache()
                    cls._backup_countdown = cls._BACKUP_EVERY
            return response
        else:
            raise requests.ConnectionError(response.json()["error"])


    @classmethod
    def remove_tags(cls,
                    response: str):
        """
        Tags are markers of the model's thought process and should not
        appear in the final answer.

        Args:
            response: string generated by an LLM
        """
        response = re.sub(r'<[^>]+>.*?</[^>]+>', '', response, flags=re.DOTALL)
        return response.strip()


    @classmethod
    def choose_best_candidate_and_justify(cls,
                                          entity,
                                          candidates: list) -> tuple[int, str]:
        """
        Given a a list of candidates, choose the best candidate
        and justify the choice.

        Args:
            entity: the entity that we found candidates for
            candidates: the list of possible candidates (Entity)
        """
        to_exclude = ["code", "url", "uri", "type_confidence", "location_confidence", "modified", "deprecated", "source", "exact_match", "type", "latitude", "longitude", "has_part", "is_part_of"]
        prompt0 = f"Choose the best candidate for the target entity.\n" + \
                  f"-1: No entity matches." + \
                  f"{', '.join(['\n' + str(i) + ': ' + c.to_string(exclude = to_exclude) for i, c in enumerate(candidates)])}.\n" + \
                  f"\n\nTarget entity: {entity.to_string(exclude = to_exclude, )}" + \
                  f"Justify your choice.\n\n" + \
                  f"Return format:" + \
                  f"match: \n" + \
                  f"justification: \n\n" + \
                  f"Return example 1: \n" + \
                  f"match: -1\n" + \
                  f"justification: No match were identified. The two observatories are located on different mountains.\n" + \
                  f"Return example 2: \n" + \
                  f"match: 0\n" + \
                  f"justification: Both entities share the same label. They do not have any incompatible feature. Their aperture and funding agency match."


        regex = r"match:\s*(.*)\s*justification:\s*(.*)"
        res, justification = None, None
        response = ""
        retries = 3
        total_retries = 0
        prompt = prompt0
        while not res and not justification:
            try:
                response = cls.generate(prompt,
                                        model = config.OLLAMA_MODEL)
                best, justification = re.findall(regex, response, re.DOTALL | re.IGNORECASE)[0]
                best = int(best)
                print(best, justification)
                return best, justification
            except:
                if retries == 0:
                    raise ValueError(f"The provided answer by {config.OLLAMA_MODEL} was not correct after {total_retries} retries.")
                prompt = prompt0 + "This is the answer you provided:\n" + response + "\n" + \
                "Reformulate it to fit this format:\n" + "\n" + \
                "match: ...\njustification: ..."
                ""
                retries -= 1
                total_retries += 1
        """"
        response = requests.post(
            f'{OLLAMA_HOST}/api/generate',
            json={
                'model': config.OLLAMA_MODEL,
                'prompt': prompt,
                'stream': False,
                'temperature': OLLAMA_TEMPERATURE, # low temperature = more determinist. Default = 0.8
            }
        )
        if response.ok:
            response = response.json()['response'].strip()
            response = cls.remove_tags(response)
            return response
        else:
            return None
        """
        return response

    _cache_same_distinct_loaded = False
    _cache_same_distinct = defaultdict(lambda: defaultdict(tuple[bool, str]))
    @classmethod
    def _load_cache_same_distinct(cls):
        if cls._cache_same_distinct_loaded:
            return
        path = CACHE_DIR / f"same_distinct{config.OLLAMA_MODEL_NAME}.json"
        if os.path.exists(path):
            with open(path, "r") as file:
                cls._cache_same_distinct = json.load(file)
        atexit.register(cls._save_cache_same_distinct)
        cls._loaded = True

    @classmethod
    def _save_cache_same_distinct(cls):
        CACHE_DIR.mkdir(parents = True, exist_ok = True)
        path = CACHE_DIR / f"same_distinct{config.OLLAMA_MODEL_NAME}.json"
        with open(path, "w") as file:
            json.dump(cls._cache_same_distinct, file, indent = 2)


    @classmethod
    def validate_same_distinct(cls,
                               entity1,
                               entity2,
                               from_cache: bool = True) -> tuple[bool, str]:
        """
        Let the LLM validate or invalidate a candidate pair.

        Return:
            a boolean (True for same, False for distinct)
            a justification string generated by the LLM

        Args:
            entity1: first entity
            entity2: compared entity
            from_cache: save LLMs responses into a cache.
                        Use responses from previous calls.
        """
        """
        if from_cache:
            if not cls._cache_same_distinct_loaded:
                cls._load_cache_same_distinct()
            if entity1.uri in cls._cache_same_distinct:
                if entity2.uri in cls._cache_same_distinct[entity1.uri]:
                    return cls._cache_same_distinct[entity1.uri][entity2.uri]
            elif entity2.uri in cls._cache_same_distinct:
                if entity1.uri in cls._cache_same_distinct[entity2.uri]:
                    return cls._cache_same_distinct[entity2.uri][entity1.uri]
        """
        to_exclude = ["code", "url", "ext_ref", "uri", "type", "type_confidence", "location_confidence", "modified", "deprecated", "source", "exact_match", "latitude", "longitude", "has_part", "is_part_of", "prior_id"]
        prompt = PROMPT_SAME_DISTINCT
        prompt += "\nEntity 1: " + entity1.to_string(exclude = to_exclude, limit = 200)
        prompt += "\nEntity 2: " + entity2.to_string(exclude = to_exclude, limit = 200)
        prompt1 = prompt
        retries = 3
        if from_cache:
            cache_key = ' '.join(sorted([entity1.uri, entity2.uri]))
            if cache_key in cls._cache_same_distinct:
                return cls._cache_same_distinct[cache_key]
        total_retries = 0
        regex = r"(response:)?[\s\n]*(.*)[\s\n]*justification:[\s\n]*(.*)"
        regex = r".*[\s\n]*(same|distinct).*?justification[\s\n\*:]*(.*)"
        while retries > 0:
            # try:
            response = cls.generate(prompt1,
                                    model = config.OLLAMA_MODEL)
            print("response:")
            print("-----")
            print(response)
            print("-----")
            res_parsed = re.findall(regex, response, re.DOTALL | re.IGNORECASE)
            print(entity1.label)
            print(entity2.label)
            print("-----res parsed---")
            print(res_parsed)
            print("---------")
            is_same, justification = res_parsed[0] #.findall(regex, response, re.DOTALL | re.IGNORECASE)[0]
            if "same" in is_same.lower() and not "distinct" in is_same.lower():
                is_same = True
            elif "distinct" in is_same.lower() and not "same" in is_same.lower():
                is_same = False
            else:
                raise ValueError(f"The LLM's response was neither same nor distinct.")
            if from_cache:
                cls._cache_same_distinct[cache_key] = [is_same, justification]
                cls._backup_countdown -= 1
                if cls._backup_countdown == 0:
                    print("BACKUP NOW")
                    cls._save_cache_same_distinct()
                    cls._backup_countdown = cls._BACKUP_EVERY
                    print("BACKUP DONE")
            return is_same, justification
            """
            except:
                if retries == 0:
                    raise ValueError(f"The provided answer by {config.OLLAMA_MODEL} was not correct after {total_retries} retries.")
                print("\tresponse was malformated:", response, "retrying.")
                print("______________")
                prompt1 = prompt + "This is the answer you provided:\n" + response + "\n" + \
                "Reformulate it to fit this format:\n" + "\n" + \
                "response: same|distinct\njustification: ..."
                ""
                print(response)
                print(f"Malformated LLM response. Retry {retries}.")
                retries -= 1
                total_retries += 1
            """


    @classmethod
    def validate_same_distinct_narrow_broad(cls,
                                            entity1,
                                            entity2) -> tuple[int, str]:
        """
        Returns:
            A tuple with an int value corresponding to
            a relation, and a justification string.
            Values are:
                0 for distinct,
                1 for same,
                2 for narrow,
                3 for broad

        Args:
            entity1: first entity
            entity2: compared entity
        """
        to_exclude = ["code", "url", "uri", "ext_ref", "type_confidence", "location_confidence", "modified", "deprecated", "source", "exact_match", "latitude", "longitude", "has_part", "is_part_of", "prior_id"]
        languages = ["en", "fr", "ca", "es", "de"]
        prompt = "Say weither those two entities are the same, distinct, broad, narrow.\n" \
        "Examples:\n" \
        "response: narrow. justification: voyager I (entity2) is the first of the two voyager mission (entity1). entity1 refers to both missions, while entity2 to one of them, so entity2 is a narrow entity of entity1.\n" + \
        "response: same. justification: both entities refer to the same infrastructure that were built on year 2013. They do not have any conflictual feature, so they are the same entity.\n" + \
        "resonse: distinct. justification: both entities are located on different continent. Moreover, one of them seems to be part of a NASA program while the other is from a JAXA program.\n" + \
        "resonse: distinct. justification: the first entity is part of a NASA program while the other is from a JAXA program.\n" + \
        "response: distinct. justification: Mauna Kea and Mauna Loa observatories are distinct observatories. The second entity is an infrastructure that is part of the Mauna Loa observatory. Therefore, it is not related to the Mauna Kea observatory.\n" \
        "response: broad. justification:the first entity (APOLLO 1) seems to be part of the second entity (APOLLO program) as APOLLO 1 is described to be the first of three APOLLO missions. therefore, entity2 is the broader entity of entity1.\n" + \
        "response: distinct. justification: entity1 is a telescope that is located at the observatory described in entity2. therefore, they are related but distinct entities.\n" + \
        "response: same. justification: DEEP SPACE 1, VIKING 2 ORBITER (labels of entity1), are two different names for ds1 (entity2).\n" + \
        "\nEntity 1: " + entity1.to_string(exclude = to_exclude, languages = languages) + \
        "\nEntity 2: " + entity2.to_string(exclude = to_exclude, languages = languages)

        prompt1 = prompt
        regex = r"response:\s*(.*)\s*justification:\s*(.*)"
        retries = 3
        total_retries = 0
        cache_key = '|'.join(sorted([entity1.uri, entity2.uri]))
        while retries > 0:
            try:
                response = cls.generate(prompt1,
                                        model = config.OLLAMA_MODEL,
                                        from_cache = True,
                                        cache_key = cache_key)
                relation, justification = re.findall(regex, response, re.DOTALL | re.IGNORECASE)[0]
                relation = relation.lower()
                if "same" in relation:
                    relation = 1
                elif "distinct" in relation:
                    relation = 0
                elif "narrow" in relation:
                    relation = 2
                elif "broad" in relation:
                    relation = 3
                else:
                    raise ValueError(f"The LLM's response was not in same, distinct, narrow or broad.")
                return relation, justification
            except:
                if retries == 0:
                    raise ValueError(f"The provided answer by {config.OLLAMA_MODEL} was not correct after {total_retries} retries.")
                prompt1 = prompt + "This is the answer you provided:\n" + response + "\n" + \
                "Reformulate it to fit this format:\n" + "\n" + \
                "response: same|distinct\njustification: ..."
                ""
                print(response)
                print(f"Malformated LLM response. Retry {retries}.")
                retries -= 1
                total_retries += 1


    # TODO remove this unused func
    def clean_response(response: str):
        """
        LLMs add '*' making the response impossible to parse
        """
        return response.replace("*", "").replace("\n", "")
