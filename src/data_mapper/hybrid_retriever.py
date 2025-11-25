"""
Class that generates hybrid embeddings (concatenated) and
refine the retrieval results with other scores (weighted).

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import numpy as np
import psutil # battery checks
from sklearn.decomposition import PCA
from typing import List, Iterator, Type, Set, Tuple, Any
from tqdm import tqdm

from data_mapper.tools.embedders.embedder import Embedder
from data_mapper.tools.filters.filter import Filter
from data_mapper.tools.scorers.scorer import Scorer
from data_mapper.tools.matchers.matcher import Matcher
from data_mapper.tools.tool import Tool
from data_mapper.tools.mapping_tools_list import MappingToolsList
from data_mapper.indexer import Indexer
from data_mapper.gui import server
from graph.entity import Entity
from graph.extractor.extractor import Extractor
import faiss # pip3 install faiss-cpu (use faiss-gpu for GPU support)
from llm.llm_connection import LLMConnection
from data_mapper.validator import strat1

from config import USERNAME
import config


class HybridRetriever():


    # For an embedder with WEIGHT = 1, reduce its dimension to this with PCA
    BASE_N_COMPONENTS = 512


    def __init__(self):
        self.embedders = []
        self.filters = []
        self.matchers = []
        self.scorers = []
        self.pca = [] # one PCA per embedder
        self.index = None
        self.indexed_entities = []
        self.embedders_weight = [] # weight of the embedders in the final score


    def add_embedder(self,
                     embedder: Type[Embedder]):
        """
        Add an embedder. Every embedder has to produce embeddings
        that will be concatenated to form an hybrid embedding. Then, a PCA will
        be applied on the resulting embeddings to reduce their dimension.
        Sort the embedders by name to make them compatible between indexers.
        """
        e = embedder()
        added = False
        for i, emb in enumerate(self.embedders):
            if emb.NAME > e.NAME:
                self.embedders.insert(i, e)
                self.embedders_weight.insert(i, e.WEIGHT)
                added = True
        if not added:
            self.embedders.append(e)
            self.embedders_weight.append(e.WEIGHT)



    def add_filter(self,
                   filter: Type[Filter]):
        """
        Add a filter to the pipeline.
        """
        self.filters.append(filter)


    def add_scorer(self,
                   scorer: Type[Scorer]):
        """
        Add a scorer to the pipeline.
        """
        self.scorers.append(scorer)


    def add_matcher(self,
                    matcher: Type[Matcher]):
        """
        Add a matcher to the pipeline.
        """
        self.matchers.append(matcher())


    def _normalize(self,
                   embeddings: np.ndarray) -> None:
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


    def fit(self,
            entities: List[Entity]) -> None:
        """
        Index entities in the FAISS indexer. Starts by embedding them using
        the embedders of this class and reducing them using a PCA to give
        every embedding the same importance (relatively to the Embedders'
        weights).
        The created index is a Flat index if there are less than 1000 entities,
        but an IVF index if there are more than 1000 entities (based on cluster
        search) to fasten the distance search.

        Args:
            entities: the entities to index. They must come from one list.
        """

        all_embeddings_entity = []
        for i, embedder in enumerate(self.embedders):
            # Create an Embedder instance
            embeddings = embedder.compute(entities)
            # Weighted PCA reduction
            # Reduce with PCA taking the embedder's WEIGHT into account
            if i == len(self.pca):
                n_components = int(self.BASE_N_COMPONENTS * embedder.WEIGHT)
                if n_components > min(embeddings.shape):
                    n_components = min(embeddings.shape)
                self.pca.append(PCA(n_components = n_components))
                embeddings = self.pca[i].fit_transform(embeddings)
            elif i < len(self.pca):
                embeddings = self.pca[i].transform(embeddings)
            all_embeddings_entity.append(embeddings)
        hybrid_embeddings = np.concatenate(all_embeddings_entity, axis=1)
        self._normalize(hybrid_embeddings)


        # FAISS index
        dim = hybrid_embeddings.shape[1]
        # If more than 1000 entities, it will be faster with centroid search
        if len(hybrid_embeddings) > 1000:
            # Maximum of centroids for the dataset size
            nlist = len(hybrid_embeddings) // 40
            quantizer = faiss.IndexFlatIP(dim)

            self.index = faiss.IndexIVFFlat(
                quantizer,         # quantizer de base
                dim,               # dimension
                nlist,             # nombre de clusters
                faiss.METRIC_INNER_PRODUCT  # type de mesure
            )
            self.index.nprobe = 10

            if not self.index.is_trained:
                self.index.train(hybrid_embeddings)
            self.index.add(hybrid_embeddings)
        else:
            self.index = faiss.IndexFlatIP(dim)
            self.index.add(hybrid_embeddings)

        print("Nb embeddings FAISS:", self.index.ntotal)
        print("Nb entités référencées:", len(self.indexed_entities))

        # PCA reduction
        # self.pca = PCA(n_components = 256)
        # reduced_embeddings = self.pca.fit_transform(hybrid_embeddings)
        # self._normalize(reduced_embeddings)



    def transform(self,
                  entities: List[Entity]) -> np.ndarray:
        """
        Transform a list of entities into hybrid embeddings.
        """
        all_embeddings = []
        for i, embedder in enumerate(self.embedders):
            embeddings = embedder.compute(entities)
            if len(self.pca) > i:
                embeddigs = self.pca[i].transform(embeddings)
            all_embeddings.append(embeddings)
        hybrid_embeddings = np.concatenate(all_embeddings, axis=1)
        self._normalize(hybrid_embeddings)

        # PCA reduction
        #self.pca = PCA(n_components = 256)
        #reduced_embeddings = self.pca.transform(hybrid_embeddings)
        #self._normalize(reduced_embeddings)
        #return reduced_embeddings

        return hybrid_embeddings


    def apply_matchers(self,
                       entity1: Entity,
                       entity2: Entity) -> Tuple[Matcher, float, float, Any]:
        """
        Apply all matchers to two entities. If one matcher returns True,
        the entities are considered the same, unless one filter eliminates
        the possibility.

        Returns:
             None if no match
             The matcher that matched if entities match

        Args:
            entity1: reference entity
            entity2: compared entity
        """
        for matcher in self.matchers:
            field1, field2, value = matcher.compute(entity1, entity2)
            if value:
                return matcher, field1, field2, value
        return None, None, None, None


    def apply_filters(self,
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


    def _apply_scorers(self,
                       entity1: Entity,
                       entity2: Entity,
                       prev_score: float = 0,
                       prev_weight: float = 0) -> tuple[float, dict[float]]:
        """
        Compute a global score between two entities by combining
        all the scores. The scores are weighted by their
        weight attribute.

        Args:
            entity1: reference entity
            entity2: compared entity
            prev_score: the score obtained with embeddings cosine similarity
            prev_weight: the weight of all embeddings
        """
        total_weight = prev_weight
        weighted_score = prev_score
        scores_dict = {"cosine_similarity": prev_score}
        for scorer in self.scorers:
            s = scorer.compute(entity1, entity2)
            scores_dict[scorer.NAME] = s
            if s >= 0: # If the score could be computed
                weighted_score += s * scorer.WEIGHT
                total_weight += scorer.WEIGHT
        if total_weight == 0:
            return 0, scores_dict # No score could be computed
        scores_dict["hybrid"] = weighted_score
        return weighted_score / total_weight, scores_dict


    def search(self,
               entities: List[Entity],
               search_k: int = 15,
               top_k: int = 10) -> Iterator[List[tuple[int, float]]]:
        """
        Search the top_k nearest neighbors for each entity in entities.

        Args:
            entities: list of entities to match
            search_k: number of nearest neighbors to search for
            top_k: number of candidate pairs to return after re-ranking with scores
        """
        for entity in entities[2:]:

            # Eliminate incompatible entities
            allowed_ids = []
            for i, indexed_entity in enumerate(self.indexed_entities):
                if self.filter(entity, indexed_entity):
                    allowed_ids.append(i)
            if not allowed_ids:
                print("no entity could be extracted for:", entity)
                yield []
                continue

            all_embeddings = []
            for i, embedder in enumerate(self.embedders):
                embeddings = embedder.compute(entity)
                if i < len(self.pca):
                    embeddings = self.pca[i].transform(embeddings)
                all_embeddings.append(embeddings)
            hybrid_embeddings = np.concatenate(all_embeddings, axis=1)
            self._normalize(hybrid_embeddings)

            #reduced_embeddings = self.pca.transform(hybrid_embeddings)
            #self._normalize(reduced_embeddings)

            if type(self.index) == faiss.IndexIVFFlat:
                selector = faiss.IDSelectorBatch(np.array(allowed_ids, dtype='int64'))
                params = faiss.IVFSearchParameters()
                params.sel = selector

                # FAISS search
                # hybrid_embeddings = hybrid_embeddings.reshape(1, -1)
                distances, indices = self.index.search(hybrid_embeddings, search_k, params = params)
            else:
                distances, indices = [[]], [[]]
                search_kk = search_k # Variable to search for more entities
                # if all were incompatible.
                while len(distances[0]) < top_k and search_kk < self.index.ntotal * 2:
                    distances, indices = self.index.search(hybrid_embeddings, search_k)
                    for d, i in zip(distances[0].copy(), indices[0].copy()):
                        if i == -1:
                            break
                        # Filter manually on allowed ids
                        if i not in allowed_ids:
                            ii = np.where(indices[0] == i)[0]
                            indices = np.delete(indices, ii, axis = 1)
                            distances = np.delete(distances, ii, axis = 1)
                    search_kk *= 2

            results = []
            entity_results = []

            for j in range(top_k): # range(min(top_k, len(indices[0])))
                entity_results.append((indices[0][j], distances[0][j]))
            results.append(entity_results)

            # Re-rank using other scores
            for i, (idx, dist) in enumerate(entity_results):
                indexed_entity = self.indexed_entities[idx]
                global_score, score_dict = self._apply_scorers(entity,
                                                               indexed_entity,
                                                               prev_score = dist,
                                                               prev_weight = sum(self.embedders_weight))
                results[0][i] = (idx, global_score, score_dict)
            results[0].sort(key = lambda x: x[1], reverse = True)
            yield entity, results[0][:top_k]


    def validate(self,
                 entity1: Entity,
                 entity2: Entity,
                 score_value: float,
                 score_name: str,
                 justification_string: str = None,
                 is_human_validation: bool = False) -> None:

        idx = self.indexed_entities.index(entity2)
        self.index.remove_ids(np.array([idx]))
        self.indexed_entities.pop(idx)
        entity1.add_synonym(entity2,
                            score_value=score_value,
                            score_name=score_name,
                            justification_string=justification_string,
                            is_human_validation=is_human_validation,
                            no_validation=False,
                            validator_name=USERNAME if is_human_validation else config.OLLAMA_MODEL_NAME
                           )


    def disambiguate(self,
                     extractor1: Extractor,
                     extractor2: Extractor,
                     on_types: Set[str] | str = "all",
                     with_tools: List[Type[Tool]] = MappingToolsList.ALL_TOOLS,
                     limit: int = -1,
                     ignore_deprecated = True,
                     human_validation: bool = False) -> None:
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
            ignore_deprecated: do not map entities that are deprecated
            human_validation: de-activate LLM validation and let the user validate each mapping
        """
        self.filters = []
        self.matchers = []
        self.embedders = []
        self.scorers = []
        for tool in with_tools:
            if issubclass(tool, Embedder):
                self.add_embedder(tool)
            elif issubclass(tool, Filter):
                self.add_filter(tool) # TODO do not instanciate classes in filters
            elif issubclass(tool, Scorer):
                self.add_score(tool)  # TODO same
            elif issubclass(tool, Matcher):
                self.add_matcher(tool)
            else:
                raise ValueError(f"Tool {tool} is not an Embedder, Filter or Scorer.")
        if not self.embedders:
            raise ValueError("At least one embedder is required.")
        entities1 = Entity.get_entities_from_list(extractors = extractor1,
                                                  ent_type = on_types,
                                                  no_equivalent_in = extractor2,
                                                  limit = limit,
                                                  ignore_deprecated = ignore_deprecated)
        entities2 = Entity.get_entities_from_list(extractors = extractor2,
                                                  ent_type = on_types,
                                                  no_equivalent_in = extractor1,
                                                  limit = limit,
                                                  ignore_deprecated = ignore_deprecated)

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

        self.indexed_entities = entities2
        self.fit(entities2)
        print("Indexing done.")


        # Generate a results table to disambiguate
        all_results = []
        first_choices = dict() # If an entity from entities2 is the first choice more than once,
        # we can formulate the LLM query differently (taking the POV of entity2 comparing
        # between two or more entities from entities1)
        if True:#human_validation:
            print(f"Searching for matches for {len(entities1)} entities from {extractor1.NAMESPACE}...")
            for i, (entity1, results) in enumerate(self.search(entities1, search_k = 15, top_k = 10)):
                # entity1 = entities1[i]

                # Firstly, create a list with all entities and their candidates.
                # Then, sort the lists by best scores
                print(f"Entity {i+1}/{len(entities1)}: {entity1.label} ({entity1.uri})")

                if not results:
                    print("No compatible entities found.")
                    continue
                for idx, score in results:
                    entity2 = self.indexed_entities[idx]
                    print(f"  Match: {entity2.label} ({entity2.uri}) with score {score:.4f}")
                c = input("Continue: ")
                if c == "exit":
                    exit()
                elif c.isdigit():
                    c = int(c)
                    if c >= 0 and c < len(results):
                        entity2_idx, score = results[c]
                        self.validate(entity1,
                                      self.indexed_entities[entity2_idx],
                                      score_value = score,
                                      score_name = "hybrid",
                                      is_human_validation = human_validation)

            print("Search done.")
        else:
            for i, (entity1, results) in enumerate(self.search(entities1, search_k = 100, top_k = 15)):
                if not results:
                    continue # No compatible entities found.
                # entity1 = entities1[i]
                matches = []
                for idx, score in results:
                    entity2 = self.indexed_entities[idx]
                    # matches.append((entity2, score)) # global score
                    matches.append(entity2)
                # Diambiguate
                llm_response = LLMConnection.choose_best_candidate_and_justify(entity1, matches)
                print("\n")
                print(llm_response)


    def fit2(self,
             entities: list[Entity]) -> np.ndarray:
        """
        Reduce dimensions with PCA and the embedders' weight factor.
        Concatenate embeddings in order to create hybrid embeddings.
        Normalize embeddings and return.
        """
        all_embeddings_entity = []
        for i, embedder in enumerate(self.embedders):
            # Create an Embedder instance
            embeddings = embedder.compute(entities)
            # Weighted PCA reduction
            # Reduce with PCA taking the embedder's WEIGHT into account
            if i == len(self.pca):
                n_components = int(self.BASE_N_COMPONENTS * embedder.WEIGHT)
                if n_components > min(embeddings.shape):
                    n_components = min(embeddings.shape)
                self.pca.append(PCA(n_components = n_components))
                embeddings = self.pca[i].fit_transform(embeddings)
            elif i < len(self.pca):
                embeddings = self.pca[i].transform(embeddings)
            all_embeddings_entity.append(embeddings)
        hybrid_embeddings = np.concatenate(all_embeddings_entity, axis=1)
        self._normalize(hybrid_embeddings)
        return hybrid_embeddings


    def process_lists(self,
                      extractor1: Extractor,
                      extractor2: Extractor,
                      on_types: Set[str] | str = None,
                      with_tools: List[Type[Tool]] = MappingToolsList.ALL_TOOLS,
                      limit: int = -1,
                      ignore_deprecated = True,
                      top_k: int = 10,
                      human_validation: bool = True) -> None:
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
            ignore_deprecated: do not map entities that are deprecated
            human_validation: de-activate LLM validation and let the user validate each mapping
        """
        self.filters = []
        self.matchers = []
        self.embedders = []
        self.scorers = []
        for tool in with_tools:
            if issubclass(tool, Embedder):
                self.add_embedder(tool)
            elif issubclass(tool, Filter):
                self.add_filter(tool)
            elif issubclass(tool, Scorer):
                self.add_scorer(tool)
            elif issubclass(tool, Matcher):
                self.add_matcher(tool)
            else:
                raise ValueError(f"Tool {tool} is not an Embedder, Filter, Matcher or Scorer.")
        #if not self.embedders:
        #    raise ValueError("At least one embedder is required.")

        if type(on_types) == str:
            if on_types == "all":
                on_types = None
            else:
                on_types = [on_types]

        # First we find all entities to index them
        all_entities1 = Entity.get_entities_from_list(extractors = extractor1,
                                                      ent_type = on_types,
                                                      limit = limit,
                                                      ignore_deprecated = ignore_deprecated)
        all_entities2 = Entity.get_entities_from_list(extractors = extractor2,
                                                      ent_type = on_types,
                                                      limit = limit,
                                                      ignore_deprecated = ignore_deprecated)

        if not all_entities1:
            print(f"Warning: no entities found for {extractor1.NAMESPACE} with types {', '.join(on_types)}. Ignoring.")
            return
        if not all_entities2:
            print(f"Warning: no entities found for {extractor2.NAMESPACE} with types {', '.join(on_types)}. Ignoring.")
            return

        entities1 = Entity.get_entities_from_list(extractors = extractor1,
                                                  ent_type = on_types,
                                                  no_equivalent_in = extractor2,
                                                  limit = limit,
                                                  ignore_deprecated = ignore_deprecated)
        entities2 = Entity.get_entities_from_list(extractors = extractor2,
                                                  ent_type = on_types,
                                                  no_equivalent_in = extractor1,
                                                  limit = limit,
                                                  ignore_deprecated = ignore_deprecated)

        if not entities1:
            print(f"Warning: no entities found for {extractor1.NAMESPACE} with types {', '.join(on_types)}. Ignoring.")
            return
        if not entities2:
            print(f"Warning: no entities found for {extractor2.NAMESPACE} with types {', '.join(on_types)}. Ignoring.")
            return


        """
        if len(entities1) > len(entities2):
            # Map entities1 into entities2. We want entities2 to be larger.
            entities1, entities2 = entities2, entities1
            extractor1, extractor2 = extractor2, extractor1
            all_entities1, all_entities2 = all_entities2, all_entities1
        """
        if self.embedders:
            embeddings1 = self.fit2(all_entities1)
            indexer1 = Indexer(extractor = extractor1,
                               embedders = self.embedders,
                               entity_types = on_types,
                               entities = all_entities1,
                               embeddings = embeddings1)
            embeddings2 = self.fit2(all_entities2)
            indexer2 = Indexer(extractor = extractor2,
                               embedders = self.embedders,
                               entity_types = on_types,
                               entities = all_entities2,
                               embeddings = embeddings2)

        for n, entity1 in tqdm(enumerate(entities1),
                               total=len(entities1),
                               desc=extractor1.NAMESPACE + " " + extractor2.NAMESPACE):
            matched = False
            if self.embedders:
                embeddings = indexer1.get_embeddings(entity1)
            blacklisted_entities = []
            for entity2 in entities2:
                if not self.apply_filters(entity1, entity2):
                    blacklisted_entities.append(entity2)
                    continue
                matcher, field1, field2, value = self.apply_matchers(entity1, entity2)
                if matcher is not None:
                    # Add synonyms
                    if self.embedders:
                        indexer1.merge_embeddings(entity1, entity2, indexer2)
                    entities2.remove(entity2)
                    entity1.add_synonym(entity2,
                                        score_name = matcher.NAME,
                                        subject_match_field = field1,
                                        object_match_field = field2,
                                        match_string = value)
                    matched = True
                    continue
            if matched:
                # Do not repeat for entity1
                continue

            if not self.embedders and not self.scorers:
                # Only matchers
                continue

            if self.embedders:
                ranked = indexer2.search_nearest(embeddings,
                                                 top_k = top_k,
                                                 whitelisted_entities = entities2,
                                                 blacklisted_entities = blacklisted_entities)
                print("after search_nearest=", len(ranked))
            else:
                ranked = [(e, 0) for e in entities2 if e not in blacklisted_entities]
            print("len(ranked)=", len(ranked), "len(entities2)=", len(entities2))
            #print("Candidates for", entity1)
            #for entity2, similarity in nearest:
            #    print(entity2, similarity)
            nearest = []
            for entity2, prev_score in ranked:
                new_score, score_dict = self._apply_scorers(entity1,
                                                            entity2,
                                                            prev_score,
                                                            prev_weight = sum(self.embedders_weight))
                nearest.append((entity2, new_score, score_dict))

            nearest = sorted(nearest, key = lambda x: x[1], reverse = True)[0:top_k]

            if human_validation:
                candidates = dict()
                for candidate, score, score_dict in nearest:
                    candidate_dict = candidate.__dict__()
                    candidate_dict_values = list(candidate_dict.values())[0]
                    candidate_dict_values["score"] = score
                    candidates[list(candidate_dict.keys())[0]] = candidate_dict_values
                server.update_state(entity = entity1.__dict__(),
                                    candidates = candidates,
                                    total = len(entities1),
                                    current = n,
                                    list1 = extractor1.NAMESPACE,
                                    list2 = extractor2.NAMESPACE)
                userchoice, justification = server.wait_for_user_choice()
                print("user choice:", userchoice)
                print("justification string:", justification)
                if userchoice != "none":
                    entity2, score, scores_dict = [(e, s, d) for e, s, d in nearest if e.label == userchoice][0]
                    print(entity2, score)
                    indexer1.merge_embeddings(entity1, entity2, indexer2)
                    entities2.remove(entity2)
                    entity1.add_synonym(entity2,
                                        score_value = score,
                                        score_name = "hybrid",
                                        scores = scores_dict,
                                        justification_string = justification,
                                        is_human_validation = human_validation,
                                        validator_name = USERNAME
                                        )
                    continue
                    # Add entity
                #if not app.running:
                #    app.run(debug = True, use_reloader = False)
                #    app.running = True
            else:
                # Apply strategy to select significantly near entities
                indexes = strat1([n[1] for n in nearest])
                print(indexes)
                print(type(indexes))
                if not indexes:
                    # Too small probability
                    continue
                nearest_filtered = [nearest[int(i)] for i in indexes]
                print("Remaining candidates after strategy application:", len(nearest_filtered))
                best, justification = LLMConnection.choose_best_candidate_and_justify(entity1, [n[0] for n in nearest_filtered])
                if best < 0:
                    continue
                else:
                    entity2, score, scores_dict = nearest[best]
                    print(entity2, score)
                    indexer1.merge_embeddings(entity1, entity2, indexer2)
                    entities2.remove(entity2)
                    entity1.add_synonym(entity2,
                                        score_value = score,
                                        score_name = "hybrid",
                                        scores = scores_dict,
                                        justification_string = justification,
                                        is_human_validation = human_validation,
                                        validator_name = config.OLLAMA_MODEL_NAME
                                        )
                    continue


            # Stop on low battery to save the generated mappings
            battery = psutil.sensors_battery()
            if battery.percent < 5:
                print("\033Low battery. Interrupting and saving mappings and progress.\n" +
                      "Re-load checkpoint using the output folder as an input.\033[0m")
                exit()