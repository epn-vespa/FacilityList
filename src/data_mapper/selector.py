"""
Save one mapping's candidate pairs after filtering
and save their scores. Only propose candidate pairs that
have a high score.

TODO:
    Apply strategies (defined in validator) to keep only
    certain scores that are significantly higher in top-k.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import math
from collections import defaultdict
from typing import Iterable

from graph.entity import Entity
from graph.extractor.extractor import Extractor


class Selector():
    all_selectors = dict()

    def __new__(cls,
                extractor1: Extractor,
                extractor2: Extractor,
                entity_types: list[str]):
        entity_types_str = '_'.join(sorted(entity_types))
        if extractor1.NAMESPACE > extractor2.NAMESPACE:
            extractor1, extractor2 = extractor2, extractor1
        if extractor1 in cls.all_selectors:
            if extractor2 in cls.all_selectors[extractor1]:
                if entity_types_str in cls.all_selectors[extractor1][extractor2]:
                    return cls.all_selectors[extractor1][extractor2][entity_types_str]
            else:
                cls.all_selectors[extractor1][extractor2] = {entity_types_str: None}
        else:
            cls.all_selectors[extractor1] = {extractor2: {entity_types_str: None}}
        selector = super().__new__(cls)
        cls.all_selectors[extractor1][extractor2][entity_types_str] = selector
        return selector

    def __init__(self,
                 extractor1: Extractor,
                 extractor2: Extractor,
                 entity_types: list[str]):
        # Dict of {entity1: {entity2: score}}
        self._mappings = defaultdict(list)
        self._ignore = []

    def add_score(self,
                  entity1: Entity,
                  entity2: Entity,
                  score: float,
                  score_dict: dict[float]):
        self._mappings[score].append((entity1, entity2, score_dict))


    def remove_entities(self,
                        entity1: Entity,
                        entity2: Entity):
        """
        Does not remove entities but add them in
        an ignore list. Will prevent iterating over them.
        """
        self._ignore.append(entity1)
        self._ignore.append(entity2)


    def set_limit(self,
                  top_k: int = 5,
                  limit_iter: int = -1,
                  z_score: float = -1,
                  max_distinct_in_row: int = 15):
        """
        Set stopping conditions.

        Args:
            top_k: how many times an entity from the first list can be selected.
            limit_iter: interrupt after n iterations.
            z_score: 0.385 ~= 65%, 1.96 ~= 95 % (if lists use many filters, a lower z_score is better)
            max_distinct_in_row: to make it effective, must call update_distinct() and cut_distinct().
        """
        self._top_k = top_k
        self._limit_iter = limit_iter
        self._z_score = z_score
        self._max_distinct_in_row = max_distinct_in_row
        self._distinct_in_row = 0


    def update_distinct(self):
        self._distinct_in_row += 1


    def cut_distinct(self):
        self._distinct_in_row = 0


    def __iter__(self) -> Iterable:
        """
        Sort mappings from higher to lower scores.
        """
        s = sorted(self._mappings.keys(), reverse = True)
        if s:
            mean = sum(s) / len(s)
            std = math.sqrt(sum((x - mean)**2 for x in s) / len(s))
            threshold = mean + self._z_score * std
            end = False
            iter_n = 0
            tries_count = defaultdict(int)
        for score in s:
            if score < threshold:
                print(f"Threshold reached. Current score: {score}, threshold: {threshold}")
                break
            for raw in self._mappings[score]:
                if self._distinct_in_row >= self._max_distinct_in_row:
                    print("Got distinct too many times in a row. Interrupting...")
                    end = True
                    break
                entity1, entity2, score_dict = raw
                if entity1 in self._ignore or entity2 in self._ignore:
                    # Can not modify self._mappings while iterating over it
                    # so we use _ignore to jump over mappings including
                    # entities that are already mapped
                    continue
                if tries_count[entity1] >= self._top_k:
                    # already tried for entity1 more than top_k times
                    continue
                elif tries_count[entity2] >= self._top_k:
                    # already tried for entity2 more than top_k times
                    continue
                if self._limit_iter > 0 and iter_n >= self._limit_iter:
                    end = True
                    print(f"Reached iteration limit: {iter_n}. Interrupting...")
                    break
                tries_count[entity1] += 1
                tries_count[entity2] += 1
                iter_n += 1
                yield score, entity1, entity2, score_dict
            if end:
                break


    def __str__(self) -> str:
        """
        Get a string representation of csv format for the
        most probable candidate pairs.
        """
        res = ""
        to_exclude = ["code", "url", "uri", "type_confidence", "location_confidence", "modified", "deprecated", "source", "exact_match", "type", "latitude", "longitude", "has_part", "is_part_of"]
        for score, entity1, entity2, _ in self:
            res += f" ,\"{score}\",\"" + entity1.to_string(exclude = to_exclude) + "\",\"" + entity2.to_string(exclude = to_exclude) + "\"\n"
        return res