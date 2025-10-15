"""
Class that generates hybrid embeddings (concatenated) and
refine the retrieval results with other scores (weighted).

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import numpy as np
from sklearn.decomposition import PCA
from typing import List, Iterator, Type, Set

from data_mapper.tools.embedders.embedder import Embedder
from data_mapper.tools.filters.filter import Filter
from data_mapper.tools.scores.score import Score
from data_mapper.tools.tool import Tool
from data_mapper.tools.mapping_tools_list import MappingToolsList
from graph.entity import Entity
from graph.graph import Graph
from graph.extractor.extractor import Extractor
import faiss # pip3 install faiss-cpu (use faiss-gpu for GPU support)

class HybridRetriever():


    def __init__(self):
        self.embedders = []
        self.filters = []
        self.scores = []
        self.pca = None
        self.index = None
        self.indexed_entities = []


    def add_embedder(self,
                     embedder: Type[Embedder]):
        """
        Add an embedder. Every embedder has to produce embeddings
        that will be concatenated to form an hybrid embedding. Then, a PCA will
        be applied on the resulting embeddings to reduce their dimension.
        """
        self.embedders.append(embedder())


    def add_filter(self,
                   filter: Filter):
        """
        Add a filter to the pipeline.
        """
        self.filters.append(filter)


    def add_score(self,
                  score: Score):
        """
        Add a score to the pipeline.
        """
        self.scores.append(score)


    def _normalize(self,
                   embeddings: np.ndarray) -> np.ndarray:
        """
        Normalize the embeddings in place.
        """
        norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
        embeddings /= norms + 1e-10


    def fit(self,
            entities: List[Entity]) -> None:
        """
        Fit pipeline on a list of entities and create a FAISS index.
        """
        # Embeddings concatenation
        all_embeddings_entity = []
        for embedder in self.embedders:
            # Create an Embedder instance
            embeddings = embedder.compute(entities)
            all_embeddings_entity.append(embeddings)
        hybrid_embeddings = np.concatenate(all_embeddings_entity, axis=1)
        self._normalize(hybrid_embeddings)

        # PCA reduction
        # self.pca = PCA(n_components = 256)
        # reduced_embeddings = self.pca.fit_transform(hybrid_embeddings)
        # self._normalize(reduced_embeddings)

        # FAISS indexation
        print("shape of hybrid embeddings:", hybrid_embeddings.shape)
        #self.index = faiss.IndexFlatIP(hybrid_embeddings.shape[1])
        #self.index.add(hybrid_embeddings)
        dim = hybrid_embeddings.shape[1]
        #quantizer = faiss.IndexFlatIP(dim) # https://github.com/facebookresearch/faiss/wiki/Faiss-building-blocks:-clustering,-PCA,-quantization
        # nlist creates n clusters
        #self.index = faiss.IndexIVFFlat(quantizer, dim, 100, faiss.METRIC_INNER_PRODUCT)
        #self.index.train(hybrid_embeddings)
        self.index = faiss.IndexFlatIP(dim)
        self.index.add(hybrid_embeddings)

    def transform(self,
                  entities: List[Entity]) -> np.ndarray:
        """
        Transform a list of entities into hybrid embeddings.
        """
        all_embeddings = []
        for embedder in self.embedders:
            embeddings = embedder.compute(entities)
            all_embeddings.append(embeddings)
        hybrid_embeddings = np.concatenate(all_embeddings, axis=1)
        self._normalize(hybrid_embeddings)

        # PCA reduction
        #self.pca = PCA(n_components = 256)
        #reduced_embeddings = self.pca.transform(hybrid_embeddings)
        #self._normalize(reduced_embeddings)
        #return reduced_embeddings
        print(hybrid_embeddings.shape)
        print(all_embeddings[0].shape)

        return hybrid_embeddings


    def filter(self,
               entity1: Entity,
               entity2: Entity) -> bool:
        """
        Apply all filters to two entities. If one filter returns False,
        the entities are considered incompatible.

        Args:
            entity1: reference entity
            entity2: compared entity
        """
        for filter in self.filters:
            if not filter.are_compatible(entity1, entity2):
                return False
        return True


    def _compute_global_score(self,
                              entity1: Entity,
                              entity2: Entity) -> float:
        """
        Compute a global score between two entities by combining
        all the scores. The scores are weighted by their
        weight attribute.
        
        Args:
            entity1: reference entity
            entity2: compared entity
        """
        total_weight = 0
        weighted_score = 0
        for score in self.scores:
            s = score.compute(entity1, entity2)
            if s >= 0: # If the score could be computed
                weighted_score += s * score.WEIGHT
                total_weight += score.WEIGHT
        if total_weight == 0:
            return -1 # No score could be computed
        return weighted_score / total_weight        


    def search(self,
               entities: List[Entity],
               top_k: int = 10) -> Iterator[List[tuple[int, float]]]:
        """
        Search the top_k nearest neighbors for each entity in entities.

        Args:
            entities: list of entities to match
            top_k: number of nearest neighbors to return
        """
        for entity in entities:

            # Eliminate incompatible entities
            allowed_ids = []
            for i, indexed_entity in enumerate(self.indexed_entities):
                if self.filter(entity, indexed_entity):
                    allowed_ids.append(i)
                else:
                    print("\t\tIncompatible:", indexed_entity, "\n\t\t\t", entity)
            if not allowed_ids:
                yield []
                continue

            all_embeddings = []
            for embedder in self.embedders:
                embeddings = embedder.compute(entity)
                all_embeddings.append(embeddings)
            hybrid_embeddings = np.concatenate(all_embeddings, axis=1)
            self._normalize(hybrid_embeddings)

            #reduced_embeddings = self.pca.transform(hybrid_embeddings)
            #self._normalize(reduced_embeddings)

            selector = faiss.IDSelectorBatch(np.array(allowed_ids, dtype='int64'))
            params = faiss.IVFSearchParameters()
            params.sel = selector

            # FAISS search
            # hybrid_embeddings = hybrid_embeddings.reshape(1, -1)
            distances, indices = self.index.search(hybrid_embeddings, top_k)#, params = params)
            results = []
            entity_results = []
            for j in range(top_k):
                entity_results.append((indices[0][j], distances[0][j]))
            results.append(entity_results)

            # Re-rank using other scores
            print("entity results before:", entity_results)
            for i, (idx, dist) in enumerate(entity_results):
                indexed_entity = self.indexed_entities[idx]
                global_score = self._compute_global_score(entity, indexed_entity)
                results[0][i] = (idx, global_score)
            results[0].sort(key = lambda x: x[1], reverse = True)
            yield results[0]


    def disambiguate(self,
                     extractor1: Extractor,
                     extractor2: Extractor,
                     on_types: Set[str] | str = "all",
                     with_tools: List[Type[Tool]] = MappingToolsList.ALL_TOOLS,
                     limit: int = -1,
                     ignore_deprecated = True) -> None:
        """
        Map entities from extractor1 to entities from extractor2 using the
        hybrid retriever. extractor1 and extractor2 might be inversed depending
        on the size of the lists, in order to index the largest list.

        Args:
            extractor1: first extractor (entities to map)
            extractor2: second extractor (entities to map to that will be indexed)
            on_types: only map entities of these types
            with_tools: list of tools (embedders, filters, scores) to use
            limit: limit the number of entities to map from list2
        """
        for tool in with_tools:
            if issubclass(tool, Embedder):
                self.add_embedder(tool)
            elif issubclass(tool, Filter):
                self.add_filter(tool) # TODO do not instanciate classes in filters
            elif issubclass(tool, Score):
                self.add_score(tool)  # TODO same
            else:
                raise ValueError(f"Tool {tool} is not an Embedder, Filter or Score.")
        if not self.embedders:
            raise ValueError("At least one embedder is required.")
        g = Graph()
        uri_entities1 = g.get_entities_from_list(source = extractor1,
                                             ent_type = on_types,
                                             no_equivalent_in = extractor2,
                                             limit = limit,
                                             ignore_deprecated = ignore_deprecated)
        uri_entities2 = g.get_entities_from_list(source = extractor2,
                                             ent_type = on_types,
                                             no_equivalent_in = extractor1,
                                             limit = limit,
                                             ignore_deprecated = ignore_deprecated)
        entities1, entities2 = [], []
        for uri, in uri_entities1:
            entities1.append(Entity(uri))
        for uri, in uri_entities2:
            entities2.append(Entity(uri))
        
        if not entities1:
            print(f"Warning: no entities found for {extractor1.NAMESPACE} with types {', '.join(on_types)}. Ignoring.")
            return
        if not entities2:
            print(f"Warning: no entities found for {extractor2.NAMESPACE} with types {', '.join(on_types)}. Ignoring.")
            return
        if len(entities1) > len(entities2):
            # Map entities1 into entities2. We want entities2 to be larger.
            entities1, entities2 = entities2, entities1
            extractor1, extractor2 = extractor2, extractor1

        if type(on_types) == str:
            on_types = [on_types]
        print(f"Mapping {len(entities1)} entities from {extractor1.NAMESPACE} " +
              f"with {len(entities2)} entities from {extractor2.NAMESPACE} " +
              f"on types: {', '.join(on_types)} " +
              f"with tools: {', '.join([t.NAME for t in with_tools])}...")
        
        print(f"Indexing {len(entities2)} entities from {extractor2.NAMESPACE}...")
        self.indexed_entities = entities2
        self.fit(entities2)
        print("Indexing done.")

        print(f"Searching for matches for {len(entities1)} entities from {extractor1.NAMESPACE}...")
        for i, results in enumerate(self.search(entities1, top_k = 10)):
            entity1 = entities1[i]
            print(f"Entity {i+1}/{len(entities1)}: {entity1.label} ({entity1.uri})")

            if not results:
                print("No compatible entities found.")
                continue
            for idx, score in results:
                entity2 = self.indexed_entities[idx]
                print(f"  Match: {entity2.label} ({entity2.uri}) with score {score:.4f}")
            c = input("Continue")
            if c == "exit":
                exit()


        print("Search done.")