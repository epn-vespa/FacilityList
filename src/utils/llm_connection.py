"""
LLM connection utility functions

"""
import atexit
import json
import requests
from config import OLLAMA_HOST, OLLAMA_MODEL, OLLAMA_TEMPERATURE, LLM_CATEGORIES_FILE, LLM_EMBEDDINGS_FILE  # type: ignore
from utils.performances import timeall
from data_updater.entity_types import *

class LLM():


    #_instance = None
    _initialized = False

    # Singleton
    def __new__(cls, *args, **kwds):
        #if not cls._instance and not cls._initialized:
        if not hasattr(cls, '_instance'):
            cls._instance = super(LLM, cls).__new__(cls)
        return cls._instance


    def __init__(self):
        if not self._initialized:
            self._context_length = dict()
            self._llm_categories = dict()
            self._llm_embeddings = dict()
            self._load_llm_categories_from_cache()
            self._load_llm_embeddings_from_cache()
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

        Keyword arguments:
        ollama_model -- the model name
        """

        context_length = self._context_length.get(ollama_model, 0)
        if context_length:
            return context_length

        response = requests.post(
                f'{OLLAMA_HOST}/api/show',
                json = {
                    'model': ollama_model,
                }
        )
        infos = response.json()['model_info']
        architecture = infos['general.architecture']
        context_length = infos[architecture + '.context_length']
        self._context_length[ollama_model] = context_length
        return context_length


    def _save_llm_categories_in_cache(self):
        if self.llm_categories:
            if not LLM_CATEGORIES_FILE.parent.exists():
                LLM_CATEGORIES_FILE.parent.mkdir(parents = True, exist_ok = True)
            # path = str(path / "llm_categories.json")
            print(f"dumping {len(self.llm_categories)} LLM categories in {str(LLM_CATEGORIES_FILE)}.")
            with open(LLM_CATEGORIES_FILE, "w", encoding = "utf-8") as f:
                json.dump(self.llm_categories, f, indent=" ")


    def _load_llm_categories_from_cache(self):
        atexit.register(self._save_llm_categories_in_cache)
        if not LLM_CATEGORIES_FILE.exists():
            return
        with open(LLM_CATEGORIES_FILE, "r", encoding = "utf-8") as f:
            try:
                self.llm_categories = json.load(f)
            except:
                self.llm_categories = dict()


    def _save_llm_embeddings_in_cache(self):
        if self.llm_embeddings:
            if not LLM_EMBEDDINGS_FILE.parent.exists():
                LLM_EMBEDDINGS_FILE.parent.mkdir(parents = True, exist_ok = True)
            print(f"dumping {len(self.llm_embeddings)} LLM embeddings in {str(LLM_EMBEDDINGS_FILE)}.")
            with open(LLM_EMBEDDINGS_FILE, "w") as f:
                json.dump(self.llm_embeddings, f)


    def _load_llm_embeddings_from_cache(self):
        atexit.register(self._save_llm_embeddings_in_cache)
        if not LLM_EMBEDDINGS_FILE.exists():
            return
        with open(LLM_EMBEDDINGS_FILE, "r") as f:
            try:
                self.llm_embeddings = json.load(f)
            except:
                self.llm_embeddings = dict()


    @timeall
    def embed(self,
              text: str,
              from_cache: bool = False,
              cache_key: str = ""):
        """
        Get the embeddings of the provided text.
        Troubleshooting:
            /embeddings is deprecated. Should use /embed ?
            https://github.com/ollama/ollama/blob/main/docs/openai.md#v1embeddings
            /v1/embeddings => compatible with OpenAI.

        Keyword arguments:
        text -- the textual represnetation of an entity to embed.
        from_cache -- whether to retrieve embeddings from a previous LLM run.
        cache_key -- key of the entity in the cache dict. List name + uri.
        """
        if from_cache and not cache_key:
            raise ValueError("Provided from_cache but not cache_key.")
        if (from_cache and self.llm_embeddings and
            cache_key and cache_key in self.llm_embeddings):
            embeddings = self.llm_embeddings[cache_key]
            if embeddings:
                return embeddings # if error, re-compute.
        prompt = "Represent this entity for search: " + text
        response = requests.post(
            f"{OLLAMA_HOST}/api/embeddings",
            json={
                "model": OLLAMA_MODEL,
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


    @timeall
    def classify(self,
                 text: str,
                 choices: list[str] = None,
                 from_cache: bool = True,
                 cache_key: str = None,
                 model: str = OLLAMA_MODEL):
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
        if from_cache and not cache_key:
            raise ValueError("Provided from_cache but not cache_key.")

        if (from_cache and self.llm_categories and
            cache_key and cache_key in self.llm_categories):
            category = self.llm_categories[cache_key]
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

        # Preamble
        prompt = "Only return the category. Do not write anything else. Do not return more than one category."
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
            prompt += "Everything that is not space observation, such as weather or geographic probes (or satellites), space debris, telecommunication satellites, military facilities, online databases are miscellaneous. "
            prompt += "If you are unsure, always return unknown. "
            prompt += "If you lack information in the text to classify, always return unknown. "
        # prompt += f"Return a label from the list : [{','.join(categories_by_descriptions.keys())}].\n\n"
        # prompt += f"Return a category from the list : \n-{'\n-'.join(llm_choices)}\n\n"

        # Categories
        prompt += f"Categories : \n-{'\n-'.join(llm_choices)}\n\n"

        # Entity representation
        prompt += f"Text to classify: {text}"

        context_length = self.get_llm_context_length(model)
        if len(prompt) > context_length:
            prompt = prompt[:context_length]

        response = requests.post(
            f'{OLLAMA_HOST}/api/generate',
            json={
                'model': model,
                'prompt': prompt,
                'stream': False,
                'temperature': OLLAMA_TEMPERATURE, # low temperature = more determinist. Default = 0.8
            }
        )
        if response.ok:
            cat = response.json()['response'].strip().lower()
            cat = cat.lstrip('-').lstrip()
            if cat in categories_by_descriptions:
                cat = categories_by_descriptions[cat]
            else:
                print(f"Error: the Ollama model did not return an category from :\n" +
                    f"{','.join(categories_by_descriptions.keys())}.\n" +
                    f"It returned {cat} instead.\n " +
                    f"Return {UFO} for prompt \"{prompt}\"")
                self.llm_categories[cache_key] = ERROR
                return UFO
            self.llm_categories[cache_key] = cat
            return cat
        else:
            self.llm_categories[cache_key] = ERROR
            print(f"Ollama error: {response.text}.\nReturn {UFO} for prompt \"{prompt}\"")
            return None


    def generate(self,
                 prompt: str,
                 model: str = OLLAMA_MODEL):

        """
        Send a simple generate query to the Ollama API.

        """
        response = requests.post(
            f'{OLLAMA_HOST}/api/generate',
            json={
                'model': model,
                'prompt': prompt,
                'stream': False,
                'temperature': OLLAMA_TEMPERATURE, # low temperature = more determinist. Default = 0.8
            }
        )
        if response.ok:
            response = response.json()['response'].strip().lower()
            return response
        else:
            return None