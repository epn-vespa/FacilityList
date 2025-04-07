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

from typing import List, Union

from sentence_transformers import SentenceTransformer, util
from data_merger.entity import Entity
from data_merger.graph import Graph
from data_merger.scorer.score import Score
from data_merger.synonym_set import SynonymSet
from utils.performances import timeall, timeit

import os
os.environ["TOKENIZERS_PARALLELISM"] = "false"

class CosineSimilarityScorer(Score):

    # Name of the score computed by this class (as in score.py)
    NAME = "sentence_cosine_similarity"


    model = SentenceTransformer("all-MiniLM-L6-v2")


    @timeall
    def compute(graph: Graph,
                entities1: List[Union[Entity, SynonymSet]],
                entities2: List[Union[Entity, SynonymSet]]) -> float:
        """
        Compute a cosine similarity score between the two entities' textual
        informations (). We use lists and batches.

        Troubleshooting:
            Cuda cannot be used in a multithread with fork().
            https://pytorch.org/docs/main/notes/multiprocessing.html

        Keyword arguments:
        graph -- the graph
        entities1 -- reference entities
        entities2 -- compared entities
        """
        encoded_entities12 = CosineSimilarityScorer.encode_batch(entities1 + entities2)
        encoded_entities1 = encoded_entities12[:len(entities1)]
        encoded_entities2 = encoded_entities12[len(entities1):]
        # encoded_entities2 = CosineSimilarityScorer.encode_batch(entities2)

        cosine_similarities = [util.pytorch_cos_sim(e1, e2)[0][0].item()
                               for e1, e2 in zip(encoded_entities1,
                                                 encoded_entities2)]
        return cosine_similarities


    @timeit
    def encode_batch(entities: List[Entity],
                     batch_size: int = 128):
        """
        Get the encoded tensors of the entities' textual informations.
        Those informations include the label, alternate labels, description,
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
            text += " ".join(entity.get_values_for("description")) + ', '
            texts.append(text)
         # no need to normalize the tensors as we compute a cosine similarity.
        return CosineSimilarityScorer.model.encode(texts,
                                                   batch_size = batch_size,
                                                   show_progress_bar = True,
                                                   convert_to_tensor = True,
                                                   normalize_embeddings = False)