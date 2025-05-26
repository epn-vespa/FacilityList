"""
Compute the Cosine Similarity between two entities basing on their
textual data: labels, alt labels, descriptions, location, superclass.
Use a transformer encoder to compute the entities' embeddings.
To test:
    Is saving the embeddings in the cache quicker than computing them
    every time ?

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from typing import Generator, List, Union

from sentence_transformers import SentenceTransformer, util
import torch
from tqdm import tqdm
from data_updater.extractor.extractor import Extractor
from graph import Graph
from data_merger.entity import Entity
from data_merger.scorer.score import Score
from data_merger.synonym_set import SynonymSet
from utils.performances import timeall, timeit
import multiprocessing
import numpy as np
from config import CACHE_DIR

import os
os.environ["TOKENIZERS_PARALLELISM"] = "false"
os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"
BATCH_SIZE = 32
N_THREADS = 4

# MODEL = "all-MiniLM-L6-v2"
MODEL = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"


class CosineSimilarityScorer(Score):

    # Name of the score computed by this class (as in score.py)
    NAME = "sentence_cosine_similarity"


    model = None


    @timeall
    def compute(graph: Graph,
                entities1: List[Union[Entity, SynonymSet]],
                entities2: List[Union[Entity, SynonymSet]],
                list1: Extractor,
                list2: Extractor) -> Generator[float, None, None]:
        """
        Compute a cosine similarity score between the two entities' textual
        informations (label, description, definition). Use lists and batches
        for better performances. Save embeddings in numpy files (cache).

        Keyword arguments:
        graph -- the graph
        entities1 -- reference entities
        entities2 -- compared entities
        """
        if CosineSimilarityScorer.model is None:
            device = "cuda" if torch.cuda.is_available() else "cpu"
            if device == "cpu":
                N_THREADS = multiprocessing.cpu_count()
                torch.set_num_threads(N_THREADS)
            CosineSimilarityScorer.model = SentenceTransformer(MODEL, device = device)

        EMBEDDINGS_FILE_1 = CACHE_DIR / f"embeddings{len(entities1)}_{list1.NAMESPACE}.npy"
        if os.path.exists(EMBEDDINGS_FILE_1):
            encoded_entities1 = np.load(EMBEDDINGS_FILE_1)
        else:
            encoded_entities1 = CosineSimilarityScorer.encode_batch(entities1)
            np.save(EMBEDDINGS_FILE_1, encoded_entities1)

        EMBEDDINGS_FILE_2 = CACHE_DIR / f"embeddings{len(entities2)}_{list2.NAMESPACE}.npy"
        if os.path.exists(EMBEDDINGS_FILE_2):
            encoded_entities2 = np.load(EMBEDDINGS_FILE_2)
        else:
            encoded_entities2 = CosineSimilarityScorer.encode_batch(entities2)
            np.save(EMBEDDINGS_FILE_2, encoded_entities2)

        for entity1 in tqdm(encoded_entities1, desc = "Computing cosine similarity for encoded entities"):
            for entity2 in encoded_entities2:
                yield util.pytorch_cos_sim(entity1, entity2)[0][0].item()


    @timeit
    def encode_batch(entities: List[Entity]):
        """
        Get the encoded tensors of the entities' textual informations.
        Those informations include the label, alternate labels, definition,
        location...
        Return a list of tensors for each entity.

        Keyword arguments:
        entities -- the list of entities to encode
        """
        print(f"Encoding the entities with {CosineSimilarityScorer.model}")
        texts = []
        for entity in entities:
            text = ""
            text += " ".join(entity.get_values_for("label"))  + ', '
            text += " ".join(entity.get_values_for("alt_label"))  + ', '
            text += " Location: ".join(entity.get_values_for("location")) + ', '
            text += " ".join(entity.get_values_for("definition")) + ', '
            texts.append(text)
        # no need to normalize embeddings as we compute a cosine similarity.
        return CosineSimilarityScorer.model.encode(texts,
                                                   batch_size = BATCH_SIZE,
                                                   show_progress_bar = True,
                                                   convert_to_tensor = True,
                                                   normalize_embeddings = False)