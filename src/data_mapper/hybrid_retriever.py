"""
Class that generates hybrid embeddings (concatenated) and
refine the retrieval results with other scores (weighted).

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import numpy as np
import psutil # battery checks
from sklearn.decomposition import PCA
from typing import List, Type, Set, Tuple, Any
from tqdm import tqdm

from data_mapper.tools.embedders.embedder import Embedder
from data_mapper.tools.filters.filter import Filter
from data_mapper.tools.scorers.scorer import Scorer
from data_mapper.tools.matchers.matcher import Matcher
from data_mapper.tools.tool import Tool
from data_mapper.tools.mapping_tools_list import MappingToolsList
from data_mapper.indexer import Indexer
from data_mapper.selector import Selector
from data_mapper.gui import server
from graph.entity import Entity
from graph.extractor.extractor import Extractor
# import faiss # pip3 install faiss-cpu (use faiss-gpu for GPU support)
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
        self.embedders_weight = [] # weight of the embedders in the final score
        self.selector = None # score selector to disambiguate from highest to lowest scores


    def add_embedder(self,
                     embedder: Embedder):
        """
        Add an embedder. Every embedder has to produce embeddings
        that will be concatenated to form an hybrid embedding. Then, a PCA will
        be applied on the resulting embeddings to reduce their dimension.
        Sort the embedders by name to make them compatible between indexers.
        """
        added = False
        for i, emb in enumerate(self.embedders):
            if emb.NAME > embedder.NAME:
                self.embedders.insert(i, embedder)
                self.embedders_weight.insert(i, embedder.WEIGHT)
                added = True
        if not added:
            self.embedders.append(embedder)
            self.embedders_weight.append(embedder.WEIGHT)


    def add_filter(self,
                   filter: Filter):
        """
        Add a filter to the pipeline.
        """
        self.filters.append(filter)


    def add_scorer(self,
                   scorer: Scorer):
        """
        Add a scorer to the pipeline.
        """
        self.scorers.append(scorer)


    def add_matcher(self,
                    matcher: Matcher):
        """
        Add a matcher to the pipeline.
        """
        self.matchers.append(matcher)


    def _normalize(self,
                   embeddings: np.ndarray) -> None:
        """
        Normalize the embeddings in place.
        """
        norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
        embeddings /= norms + 1e-10


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


    def apply_scorers(self,
                      entity1: Entity,
                      entity2: Entity,
                      prev_score: float = 0,
                      prev_weight: float = 0) -> tuple[float, dict[float]] | Scorer:
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
        scores_dict = {"cosine_similarity": prev_score} # Cosine similarity of hybrid embeddings
        for scorer in self.scorers:
            s = scorer.compute(entity1, entity2)
            if scorer.apply_threshold(s):
                return None, None, scorer, s
            scores_dict[scorer.NAME] = s
            if s >= 0: # If the score could be computed
                weighted_score += s * scorer.WEIGHT
                total_weight += scorer.WEIGHT
        if total_weight == 0:
            return 0, scores_dict # No score could be computed
        scores_dict["hybrid"] = weighted_score / total_weight
        return weighted_score / total_weight, scores_dict, None, None


    def fit(self,
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
                      human_validation: bool = True,
                      allow_broad_narrow: bool = False) -> None:
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
        self.selector = Selector(extractor1, extractor2, on_types)
        self.selector.set_limit(top_k = 3,
                                limit_iter = 500,
                                z_score = 0.385,
                                max_distinct_streak = 10)
        for tool in with_tools:
            if isinstance(tool, Embedder):
                self.add_embedder(tool)
            elif isinstance(tool, Filter):
                self.add_filter(tool)
            elif isinstance(tool, Scorer):
                self.add_scorer(tool)
            elif isinstance(tool, Matcher):
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
            print(f"Warning: no entities to map for {extractor1.NAMESPACE} with types {', '.join(on_types)}. Ignoring.")
            return
        if not entities2:
            print(f"Warning: no entities to map for {extractor2.NAMESPACE} with types {', '.join(on_types)}. Ignoring.")
            return


        """
        if len(entities1) > len(entities2):
            # Map entities1 into entities2. We want entities2 to be larger.
            entities1, entities2 = entities2, entities1
            extractor1, extractor2 = extractor2, extractor1
            all_entities1, all_entities2 = all_entities2, all_entities1
        """
        if self.embedders:
            embeddings1 = self.fit(all_entities1)
            indexer1 = Indexer(extractor = extractor1,
                               embedders = self.embedders,
                               entity_types = on_types,
                               entities = all_entities1,
                               embeddings = embeddings1)
            embeddings2 = self.fit(all_entities2)
            indexer2 = Indexer(extractor = extractor2,
                               embedders = self.embedders,
                               entity_types = on_types,
                               entities = all_entities2,
                               embeddings = embeddings2)
        else:
            indexer1 = None
            indexer2 = None
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
                    self.validate_mapping(indexer1 = indexer1,
                                          indexer2 = indexer2,
                                          extractor1 = extractor1,
                                          extractor2 = extractor2,
                                          entity1 = entity1,
                                          entity2 = entity2,
                                          entities2 = entities2,
                                          score_name = matcher.NAME,
                                          subject_match_field = field1,
                                          object_match_field = field2,
                                          match_string = value
                                          )
                    matched = True
                    break
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
            else:
                ranked = [(e, 0) for e in entities2 if e not in blacklisted_entities]
            nearest = []
            for entity2, prev_score in ranked:
                new_score, score_dict, scorer, score_value = self.apply_scorers(entity1,
                                                                                entity2,
                                                                                prev_score,
                                                                                prev_weight = sum(self.embedders_weight))
                if scorer is not None:
                    # Validate score (threshold reached)
                    self.validate_mapping(indexer1, indexer2,
                                          extractor1, extractor2,
                                          entity1, entity2,
                                          entities2, score_value, scores_dict, score_name = scorer.NAME,
                                          justificatoin_string = scorer.threshold_str(),
                                          is_human_validation = human_validation,
                                          validator_name = "threshold",
                                          subject_match_field = None, # TODO keep track of this
                                          object_match_field = None, # TODO
                                          match_string = None # TODO
                                          )
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
                    self.validate_mapping(indexer1, indexer2,
                                          extractor1 = extractor1,
                                          extractor2 = extractor2,
                                          entity1 = entity1,
                                          entity2 = entity2,
                                          entities2 = entities2,
                                          score_value = score,
                                          scores_dict = scores_dict,
                                          score_name = "hybrid",
                                          justification_string = justification,
                                          is_human_validation = human_validation,
                                          validator_name = USERNAME)
                    continue
                    # Add entity
                #if not app.running:
                #    app.run(debug = True, use_reloader = False)
                #    app.running = True
            else:
                for entity2, score, score_dict in nearest:
                    self.selector.add_score(entity1, entity2, score, score_dict)
        #with open(extractor1.NAMESPACE + '-' + extractor2.NAMESPACE + '.csv', 'w') as f:
        #    f.write(str(self.selector))
        if not human_validation:
            # Validate from highest to lowest score
            for score, entity1, entity2, scores_dict in self.selector:
                # TODO use a dynamic allow_broad_narrow, depending on the entities' type.
                if entity2 not in entities2:
                    continue
                if allow_broad_narrow:
                    llmchoice, justification = LLMConnection().validate_same_distinct_narrow_broad(entity1, entity2)
                else:
                    llmchoice, justification = LLMConnection().validate_same_distinct(entity1, entity2)
                if llmchoice == 1: # same
                    self.validate_mapping(indexer1 = indexer1,
                                          indexer2 = indexer2,
                                          extractor1 = extractor1,
                                          extractor2 = extractor2,
                                          entity1 = entity1,
                                          entity2 = entity2,
                                          entities2 = entities2,
                                          score_value = score,
                                          scores_dict = scores_dict,
                                          score_name = "hybrid",
                                          justification_string  = justification,
                                          is_human_validation = human_validation,
                                          validator_name = config.OLLAMA_MODEL_NAME)
                    self.selector.remove_entities(entity1, entity2)
                    self.selector.cut_distinct_streak()
                elif llmchoice in [2, 3]: # narrow 2, broad 3. Add predicate arg.
                    self.validate_mapping(indexer1 = indexer1,
                                          indexer2 = indexer2,
                                          extractor1 = extractor1,
                                          extractor2 = extractor2,
                                          entity1 = entity1,
                                          entity2 = entity2,
                                          predicate = "narrow" if llmchoice == 2 else "broad",
                                          entities2 = entities2,
                                          score_value = score,
                                          scores_dict = scores_dict,
                                          score_name = "hybrid",
                                          justification_string  = justification,
                                          is_human_validation = human_validation,
                                          validator_name = config.OLLAMA_MODEL_NAME)
                    self.selector.cut_distinct_streak()
                else:
                    self.invalidate_mapping()
                    self.selector.update_distinct_streak()
                self.check_battery()


    def validate_mapping(self,
                         indexer1: Indexer,
                         indexer2: Indexer,
                         extractor1: Extractor,
                         extractor2: Extractor,
                         entity1: Entity,
                         entity2: Entity,
                         entities2: list[Entity],
                         score_value: float = None,
                         score_name: str = None,
                         scores_dict: dict[float] = None,
                         justification_string: str = None,
                         is_human_validation: bool = False,
                         validator_name: str = None,
                         subject_match_field: str = None,
                         object_match_field: str = None,
                         match_string: str = None,
                         predicate: str = None):
        """
        Args:
            predicate: "broad" | "narrow" or None if an exactMatch mapping.
        """
        if self.embedders:
            indexer1.merge_embeddings(entity1, entity2, indexer2)
        entities2.remove(entity2)
        if not predicate:
            entity1.add_synonym(entity2,
                                extractor1 = extractor1,
                                extractor2 = extractor2,
                                score_value = score_value,
                                score_name = score_name,
                                scores = scores_dict,
                                filters = self.filters,
                                justification_string = justification_string,
                                is_human_validation = is_human_validation,
                                validator_name = validator_name,
                                subject_match_field = subject_match_field,
                                object_match_field = object_match_field,
                                match_string = match_string
                                )
        else:
            entity1.add_broad_narrow_relation(entity2,
                                              extractor1 = indexer1.extractor,
                                              extractor2 = indexer2.extractor,
                                              score_value = score_value,
                                              score_name = score_name,
                                              scores = scores_dict,
                                              justification_string = justification_string,
                                              is_human_validation = is_human_validation,
                                              validator_name = validator_name,
                                              is_broad = predicate == "broad"
                                              )


    def invalidate_mapping(self,) -> None:
        # TODO
        print("Classified as distinct entities by LLM. Save somewhere ?")


    def check_battery(self):
        # Stop on low battery to save the generated mappings
        battery = psutil.sensors_battery()
        if battery.percent < 5:
            print("\033Low battery. Interrupting and saving mappings and progress.\n" +
                    "Re-load checkpoint using the output folder as an input.\033[0m")
            exit()