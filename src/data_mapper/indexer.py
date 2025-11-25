import numpy as np
from collections import defaultdict
from graph.extractor.extractor import Extractor
from graph.entity import Entity
from graph.entity_types import *
from data_mapper.tools.embedders.embedder import Embedder
from sklearn.metrics.pairwise import cosine_similarity

class Indexer:
    pass

class Indexer():
    """
    Class that saves all embeddings of each entity from each list.
    Embeddings can be merged between two entities if they are validated synonyms.
    """

    # All indexes by extractor and entity types
    _registry = defaultdict(lambda: defaultdict(lambda: defaultdict(frozenset)))


    def __new__(cls,
                extractor: Extractor,
                embedders: list[Embedder],
                entity_types: list[str],
                entities: list[Entity] = None,
                embeddings: np.ndarray = None):
        ent_types = frozenset(entity_types)
        if ent_types in cls._registry:
            if extractor in cls._registry[ent_types]:
                frozen = frozenset(embedders)
                if frozen in cls._registry[ent_types][extractor]:
                    return cls._registry[ent_types][extractor][frozen]

        # Sinon, on crée une nouvelle instance
        instance = super().__new__(cls)
        return instance


    def __init__(self,
                 extractor: Extractor,
                 embedders: list[Embedder],
                 entity_types: list[str],
                 entities: list[Entity] = [],
                 embeddings: np.ndarray = np.ndarray(0)):
        self.embedders = frozenset(embedders)
        self.entity_types = frozenset(entity_types)
        if extractor in self._registry:
            if self.embedders in Indexer._registry[extractor]:
                return

        self.indexes = {entity: embedding for entity, embedding in zip(entities, embeddings)}
        self.extractor = extractor
        if extractor:
            Indexer._registry[self.entity_types][extractor][self.embedders] = self


    def search_nearest(self,
                       embeddings: np.array,
                       top_k: int,
                       blacklisted_entities: list[Entity] = [],
                       whitelisted_entities: list[Entity] = []) -> list[tuple[Entity, float]]:
        """
        Returns: a list of tuples (Entity, float) where the float represents the
            similarity score between the candidate entity and the provided embeddings

        Args:
            embeddings: the embeddings of the entity to search nearest neighbors for
            top_k: how many entities should be returned
            blacklisted_entities: entities that are incompatible with entity1
            whitelisted_entities: entities that can be used
        """
        # 1. Compute distance
        entities_by_similarity = []
        if whitelisted_entities:
            for entity2 in whitelisted_entities:
                if entity2 in blacklisted_entities:
                    continue
                embeddings2 = self.indexes[entity2]
                similarity = cosine_similarity([embeddings], [embeddings2])
                entities_by_similarity.append((entity2, similarity.item()))
        else:
            for entity2 in self.indexes:
                if entity2 in blacklisted_entities:
                    continue
                embeddings2 = self.indexes[entity2]
                similarity = cosine_similarity([embeddings], [embeddings2])
                entities_by_similarity.append((entity2, similarity.item()))

        return sorted(entities_by_similarity, key = lambda x: x[1], reverse = True)[0:top_k]


    def merge_embeddings(self,
                         entity1: Entity,
                         entity2: Entity,
                         indexer2: Indexer = None,
                         extractor2: Extractor = None) -> None:
        """
        /!\\ they should be merged before adding the synonym relation,
        otherwise the weights of each embedding will not be correct!

        Merge embeddings of entity1 and entity2 if they are compatible
        (= they are built using the same embedder).

        Args:
            entity1: merged entity from this indexer
            entity2: merged entity from the extractor2's indexer
            indexer2: if known, pass the entity2's indexer
            entity_types: used to retrieve the indexer2
            extractor2: the extractor of the second entity
        """
        if not indexer2:
            indexer2 = Indexer._registry.get(self.entity_types, None)
            if not indexer2:
                print("Could not find indexer2 in the registry.")
                return
            indexer2 = indexer2.get(extractor2, None)
            if not indexer2:
                print("Could not find indexer2 in the registry.")
                return
            indexer2 = indexer2.get(self.embedders, None)
        if not indexer2:
            print("Could not find indexer2 in the registry.")
            return
        embeddings1 = self.indexes.get(entity1, None)
        embeddings2 = indexer2.indexes.get(entity2, None)
        if embeddings1 is None or embeddings2 is None:
            print("Returning...")
            return

        # Weight the embeddings so that each sysnonym has the same importance in the merged embeddings
        syn_ent1 = len(entity1.get_synonyms()) + 1
        syn_ent2 = len(entity2.get_synonyms()) + 1
        merged_embeddings = (embeddings1 * syn_ent1 + embeddings2 * syn_ent2) / (syn_ent1 + syn_ent2)
        indexer2.indexes[entity2] = merged_embeddings
        self.indexes[entity1] = merged_embeddings


    def get_embeddings(self,
                       entity: Entity) -> np.array:
        """
        If this entity is indexed in this indexer, return
        its embeddings.
        """
        if entity in self.indexes:
            return self.indexes[entity]
        else:
            return None