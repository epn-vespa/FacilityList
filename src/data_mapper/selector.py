"""
Save one mapping's candidate pairs after filtering
and save their scores. Only propose candidate pairs that
have a high score.
"""
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

    def add_score(self,
                  entity1: Entity,
                  entity2: Entity,
                  score: float):
        self._mappings[score].append((entity1, entity2))


    def __iter__(self) -> Iterable:
        """
        Sort mappings from higher to lower scores.
        """
        s = sorted(self._mappings, key = lambda x: x[0])
        for score, (entity1, entity2) in s:
            yield score, entity1, entity2


    def __str__(self) -> str:
        """
        Get a string representation of csv format for the
        most probable candidate pairs.
        """
