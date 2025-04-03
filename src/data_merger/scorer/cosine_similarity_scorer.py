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

from typing import Union

from sentence_transformers import SentenceTransformer, util
from data_merger.entity import Entity
from data_merger.graph import Graph
from data_merger.scorer.score import Score
from data_merger.synonym_set import SynonymSet

model = SentenceTransformer("all-MiniLM-L6-v2")

class CosineSimilarityScorer(Score):

    # Name of the score computed by this class (as in score.py)
    NAME = "cosine_similarity"


    def compute(graph: Graph,
                entity1: Union[Entity, SynonymSet],
                entity2: Union[Entity, SynonymSet]) -> float:
        """
        Compute a cosine similarity score between the two entities' textual
        informations ()

        Keyword arguments:
        graph -- the graph
        entity1 -- reference entity
        entity2 -- compared entity
        """
        encoded_entity1 = CosineSimilarityScorer.encode(entity1)
        encoded_entity2 = CosineSimilarityScorer.encode(entity2)
        cosine_similarity = util.pytorch_cos_sim(encoded_entity1, encoded_entity2)
        return cosine_similarity


    def encode(entity: Entity):
        """
        Get the encoded tensors of the entity's textual informations.
        Those informations include the label, alternate labels, description,
        location...

        Keyword arguments:
        entity -- the entity to encode
        """
        text = ""
        text += " " + entity.get_values_for("label")
        text += " ".join(entity.get_values_for("alt_label"))
        text += " ".join(entity.get_values_for("description"))
        text += " Location: ".join(entity.get_values_for("location"))
        return model.encode(text, convert_to_tensor = True)