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


    def __iter__(self) -> Iterable:
        """
        Sort mappings from higher to lower scores.
        """
        s = sorted(self._mappings.keys(), reverse = True)
        for score in s:
            for raw in self._mappings[score]:
                entity1, entity2, score_dict = raw
                if entity1 in self._ignore or entity2 in self._ignore:
                    # Can not modify self._mappings while iterating over it
                    continue
                yield score, entity1, entity2, score_dict


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