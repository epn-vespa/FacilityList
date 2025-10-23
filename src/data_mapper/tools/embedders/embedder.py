"""
Define the superclass Embedder.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import abc
import numpy as np
import hashlib
import os
import pickle

from typing import List
from graph.entity import Entity
from data_mapper.tools.tool import Tool
from config import CACHE_DIR

class Embedder(Tool):
    """
    An embedder returns embeddings for a single list of entities.
    It will be used to index the list's entities.
    """
    WEIGHT = 1.0

    NAME = "Generic Embedder (superclass)"


    @abc.abstractmethod
    def compute(self,
                entities: List[Entity]) -> np.ndarray:
        """
        Compute the embedding of for a list of entities.
        """
        raise NotImplementedError("This method should be overridden by subclasses.")


    def save_to_cache(self,
                      string: str,
                      embeddings: np.ndarray) -> None:
        """
        Save embeddings to cache.
        """
        string_code = hashlib.sha256(string.encode('utf-8')).hexdigest()
        cache_folder = CACHE_DIR / "embeddings" / self.NAME
        os.makedirs(cache_folder, exist_ok = True)
        filename = cache_folder / string_code + '.pkl'
        with open(filename, 'wb') as file:
            pickle.dump(embeddings, file)


    def load_from_cache(self,
                        string: str) -> np.ndarray | None:
        string_code = hashlib.sha256(string.encode('utf-8')).hexdigest()
        cache_folder = CACHE_DIR / "embeddings" / self.NAME
        os.makedirs(cache_folder, exist_ok = True)
        filename = cache_folder / string_code + '.pkl'
        if os.path.exists(filename):
            with open(filename, 'rb') as file:
                return pickle.load(file)


    def __str__(self):
        return self.NAME