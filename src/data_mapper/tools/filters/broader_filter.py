"""
If the broader entities of the compared entities are not in a synonym set,
those entities can not be the same.

Example: if two instruments are on different spacecraft,
they are not the same instrument.

This filter should be added exclusively for special mappings (instruments)
and only be performed after mapping their platforms. Ex:
pds, spase[all, -instrument]: ...
pds, spase[instrument]: broad

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
from data_mapper.tools.filters.filter import Filter
from graph.entity import Entity


class BroaderFilter(Filter):

    NAME = "same_broader"

    def are_compatible(self,
                       entity1: Entity,
                       entity2: Entity) -> bool:
        broaders1 = entity1.get_values_for("is_part_of")
        broaders2 = entity2.get_values_for("is_part_of")
        if not broaders1 or not broaders2:
            return True
        for broader in broaders1.copy():
            broaders1.update(Entity(broader).get_synonyms())
        for broader in broaders2.copy():
            broaders2.update(Entity(broader).get_synonyms())
        if broaders1.intersection(broaders2):
            return True
        return False
