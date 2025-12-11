"""
LLM connection utility functions

"""
import atexit
import json
import pickle
import re
import requests
from config import OLLAMA_TEMPERATURE, LLM_CATEGORIES_FILE, LLM_EMBEDDINGS_FILE
import config
from utils.performances import timeall, timeit
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
        if not hasattr(cls, '_instance'):
            cls._instance = super(LLMConnection, cls).__new__(cls)
        return cls._instance


    def __init__(self):
        if not self._initialized:
            self._context_length = dict()
            self._llm_categories = dict()
            self._llm_embeddings = dict()
            self._initialized = True


    @property
    def llm_categories(self) -> dict:
        return self._llm_categories


    @llm_categories.setter
    def llm_categories(self,
                       categories: dict):
        self._llm_categories = categories


    @property
    def llm_embeddings(self) -> dict:
        return self._llm_embeddings


    @llm_embeddings.setter
    def llm_embeddings(self,
                       embeddings: dict):
        self._llm_embeddings = embeddings



    def get_llm_context_length(self,
                               ollama_model: str) -> int:
        """
        Get the context length of a model. A context length is the prompt's
        maximal length.

        Args:
            ollama_model: the model name.
        """

        context_length = self._context_length.get(ollama_model, 0)
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
            self._context_length[ollama_model] = context_length
        except KeyError:
            print(response.json()['error'])
            print("To download an Ollama model, use 'ollama pull [model_name]'.")
            exit(code=1)
        return context_length


    def _save_llm_categories_in_cache(self):
        """
        Save the LLM categories in a json file in the cache folder.
        """
        if self.llm_categories:
            if not LLM_CATEGORIES_FILE.parent.exists():
                LLM_CATEGORIES_FILE.parent.mkdir(parents = True, exist_ok = True)
            # path = str(path / "llm_categories.json")
            print(f"dumping {len(self.llm_categories)} LLM categories in {str(LLM_CATEGORIES_FILE)}.")
            with open(LLM_CATEGORIES_FILE, "w", encoding = "utf-8") as f:
                json.dump(self.llm_categories, f, indent=" ")


    def _load_llm_categories_from_cache(self):
        """
        Load the LLM categories from a json file in the cache folder.
        """
        atexit.register(self._save_llm_categories_in_cache)
        if not LLM_CATEGORIES_FILE.exists():
            return
        with open(LLM_CATEGORIES_FILE, "r", encoding = "utf-8") as f:
            try:
                self.llm_categories = json.load(f)
            except:
                self.llm_categories = dict()


    def _save_llm_embeddings_in_cache(self):
        """
        Dump the LLM embeddings in a pickle file in the cache folder.
        """
        if self.llm_embeddings:
            LLM_EMBEDDINGS_FILE.parent.mkdir(parents = True, exist_ok = True)
            print(f"dumping {len(self.llm_embeddings)} LLM embeddings in {str(LLM_EMBEDDINGS_FILE)}.")
            with open(LLM_EMBEDDINGS_FILE, "w") as f:
                pickle.dump(self.llm_embeddings, f)


    @timeit
    def _load_llm_embeddings_from_cache(self):
        """
        Load the LLM embeddings from a pickle file in the cache folder.
        """
        atexit.register(self._save_llm_embeddings_in_cache)
        if not LLM_EMBEDDINGS_FILE.exists():
            return
        with open(LLM_EMBEDDINGS_FILE, "r") as f:
            try:
                self.llm_embeddings = pickle.load(f)
            except:
                self.llm_embeddings = dict()


    @timeall
    def embed(self,
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
        if (from_cache and not self.llm_embeddings):
            self._load_llm_embeddings_from_cache()
        if (from_cache and self.llm_embeddings):
            embeddings = self.llm_embeddings.get(cache_key, None)
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
            self.llm_embeddings[cache_key] = embeddings
            return embeddings
        else:
            self.llm_embeddings[cache_key] = None
            print(f"Ollama error: {response.text}.\nReturn None for prompt \"{prompt}\"")
            return None

 
    # Labels used to classify entities with the model to project's labels
    categories_by_descriptions = {"ground observatory": GROUND_OBSERVATORY,
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

    @timeall
    def classify(self,
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
            raise ValueError(f"Provided from_cache but not cache_key to function {self.classify.__name__}.")

        if from_cache and not self.llm_categories:
            self._load_llm_categories_from_cache()

        if (from_cache and self.llm_categories and
            cache_key and cache_key in self.llm_categories):
            category = self.llm_categories[cache_key]
            if category != ERROR:
                return category # if error, re-compute.

        if not choices:
            choices = self.categories_by_descriptions.values()

        possible_categories = set()
        llm_choices = set()
        for key, value in self.categories_by_descriptions.items():
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

        context_length = self.get_llm_context_length(config.OLLAMA_MODEL)
        if len(prompt) > context_length:
            prompt = prompt[:context_length]

        # Get the category from the LLM
        cat = self.generate(prompt, config.OLLAMA_MODEL)
        cat = cat.lstrip('-').lstrip()
        cat = cat.split("\n")[0] # Some models (such as Gemma) return more than one category
        if cat in self.categories_by_descriptions:
            cat = self.categories_by_descriptions[cat]
        else:
            print(f"Error: the Ollama model did not return an category from :\n" +
                f"{','.join(self.categories_by_descriptions.keys())}.\n" +
                f"It returned {cat} instead.\n " +
                f"Return {UFO} for prompt \"{prompt}\"")
            self.llm_categories[cache_key] = ERROR
            return UFO
        self.llm_categories[cache_key] = cat
        return cat


    @classmethod
    def generate(self,
                 prompt: str,
                 model: str) -> str:

        """
        Send a simple generate query to the Ollama API.

        Args:
            prompt: the prompt to send to the LLM.
            model: the model to use.
        """
        response = requests.post(
            f'{config.OLLAMA_HOST}/api/generate',
            json={
                'model': model,
                'prompt': prompt,
                'stream': False,
                'temperature': OLLAMA_TEMPERATURE, # low temperature = more determinist. Default = 0.8
            }
        )
        if response.ok:
            response = response.json()['response'].strip().lower()
            response = self.remove_tags(response)
            return response
        else:
            raise requests.ConnectionError(response.json()["error"])


    @classmethod
    def remove_tags(self,
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
    def choose_best_candidate_and_justify(self,
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
                response = self.generate(prompt,
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
            response = self.remove_tags(response)
            return response
        else:
            return None
        """
        return response


    @timeall
    def validate_same_distinct(self,
                               entity1,
                               entity2) -> tuple[bool, str]:
        """
        Let the LLM validate or invalidate a candidate pair.

        Return:
            a boolean (True for same, False for distinct)
            a justification string generated by the LLM

        Args:
            entity1: first entity
            entity2: compared entity
        """
        to_exclude = ["code", "url", "ext_ref", "uri", "type_confidence", "location_confidence", "modified", "deprecated", "source", "exact_match", "latitude", "longitude", "has_part", "is_part_of", "prior_id"]
        prompt = "Say weither those two entities are the same, distinct.\n" \
        "Templates:\n" \
        "response: distinct. justification: voyager I is the first of the two voyager mission. one refer to the first mission, the other to both missions, so they are distinct entities.\n" + \
        "response: same. justification: both entities refer to the same infrastructure that were built on year 2013. They do not have any conflictual feature, so they are the same entity.\n" + \
        "resonse: distinct. justification: both entities are located on different continent. Moreover, one of them seems to be part of a NASA program while the other is from a JAXA program.\n" + \
        "response: distinct. justification: even though the first entity (APOLLO 1) seems to be part of the second entity (APOLLO program), they are distinct entities as APOLLO 1 is described to be the first of three APOLLO missions. therefore, they are related but distinct entities.\n" + \
        "response: distinct. justification: entity1 is a telescope that is located at the observatory described in entity2. therefore, they are related but distinct entities.\n" + \
        "response: same. justification: DEEP SPACE 1, VIKING 2 ORBITER (labels of entity1), are two different names for ds1 (entity2).\n" + \
        "\nEntity 1: " + entity1.to_string(exclude = to_exclude) + \
        "\nEntity 2: " + entity2.to_string(exclude = to_exclude)
        #, is part of or has part and justify.\n" \
        #"response: is part of. justification: it is stated that the first entity is part of the ESA Southern observatory, while the second entity seems to refer to the ESA Southern observatory. Both entities' location match.\n" + \
        #"response: has part. justification: the first entity is a broader entity of the second entity. It is stated that the UR spacecraft (2nd entity) is a part of the UR investigation (1st entity).\n" + \
        prompt1 = prompt
        regex = r"response:\s*(.*)\s*justification:\s*(.*)"
        retries = 3
        total_retries = 0
        while retries > 0:
            try:
                print("PROMPT", prompt1)
                response = self.generate(prompt1,
                                         model = config.OLLAMA_MODEL)
                is_same, justification = re.findall(regex, response, re.DOTALL | re.IGNORECASE)[0]
                if "same" in is_same.lower() and not "distinct" in is_same.lower():
                    is_same = True
                elif "distinct" in is_same.lower() and not "same" in is_same.lower():
                    is_same = False
                else:
                    raise ValueError(f"The LLM's response was neither same nor distinct.")
                print(is_same, justification)
                return is_same, justification
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


    @timeall
    def validate_same_distinct_narrow_broad(self,
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
        to_exclude = ["code", "url", "uri", "type_confidence", "location_confidence", "modified", "deprecated", "source", "exact_match", "latitude", "longitude", "has_part", "is_part_of", "prior_id"]
        prompt = "Say weither those two entities are the same, distinct, broad, narrow.\n" \
        "Templates:\n" \
        "response: narrow. justification: voyager I (entity2) is the first of the two voyager mission (entity1). entity1 refers to both missions, while entity2 to one of them, so entity2 is a narrow entity of entity1.\n" + \
        "response: same. justification: both entities refer to the same infrastructure that were built on year 2013. They do not have any conflictual feature, so they are the same entity.\n" + \
        "resonse: distinct. justification: both entities are located on different continent. Moreover, one of them seems to be part of a NASA program while the other is from a JAXA program.\n" + \
        "response: broad. justification:the first entity (APOLLO 1) seems to be part of the second entity (APOLLO program) as APOLLO 1 is described to be the first of three APOLLO missions. therefore, entity2 is the broader entity of entity1.\n" + \
        "response: distinct. justification: entity1 is a telescope that is located at the observatory described in entity2. therefore, they are related but distinct entities.\n" + \
        "response: same. justification: DEEP SPACE 1, VIKING 2 ORBITER (labels of entity1), are two different names for ds1 (entity2).\n" + \
        "\nEntity 1: " + entity1.to_string(exclude = to_exclude) + \
        "\nEntity 2: " + entity2.to_string(exclude = to_exclude)

        prompt1 = prompt
        regex = r"response:\s*(.*)\s*justification:\s*(.*)"
        retries = 3
        total_retries = 0
        while retries > 0:
            try:
                print("PROMPT", prompt1)
                response = self.generate(prompt1,
                                         model = config.OLLAMA_MODEL)
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