"""
Methods to merge entities on cross-list identifiers.
Entities that are merged this way do not need to be paired
for comparison. They can already be in the same synset and
they do not need to be mapped as candidates in candidate pairs.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from graph.extractor.extractor import Extractor
from graph.graph import Graph
from graph.extractor.wikidata_extractor import WikidataExtractor
from graph.extractor.naif_extractor import NaifExtractor
from graph.entity import Entity

from rdflib.namespace import SKOS
from rdflib import Literal
from tqdm import tqdm

from utils.performances import timeit


class AttributeMatcher():


    def __init__(self):
        pass


    @timeit
    def merge_wikidata_naif(self) -> None:
        """
        Merge Wikidata and NAIF entities if both namespaces
        exist using the NAIF_ID relation of Wikidata.

        Wikidata-NAIF is a resolved mapping and no NAIF entity should be unpaired
        after running this function.
        """
        graph = Graph()

        # Loop on each wikidata class
        wikidata_entities = Entity.get_entities_from_list(WikidataExtractor())
        map_remaining = False

        for wikidata_entity in wikidata_entities:
            naif_codes = wikidata_entity.get_values_for("NAIF_ID")
            for naif_code in naif_codes:
                naif_ids = []
                for naif_id, _, _ in graph.triples((None,
                                                    SKOS.notation,
                                                    Literal(naif_code))):
                    naif_ids.append(naif_id)
                if len(naif_ids) == 1:
                    # There is only one NAIF entity with this ID.
                    naif_entity = Entity(naif_ids[0])
                    wikidata_entity.add_synonym(naif_entity,
                                                extractor1 = WikidataExtractor,
                                                extractor2 = NaifExtractor,
                                                no_validation = True,
                                                subject_match_field="NAIF_ID",
                                                object_match_field="code",
                                                match_string = str(naif_code))
                elif len(naif_ids) > 1:
                    # Ambiguous NAIF identifier.
                    # need further disambiguation with CandidatePair.
                    map_remaining = True
                    for naif_id in naif_ids:
                        naif_entity = Entity(naif_id)

        if map_remaining:
            self.merge_on(extractor1 = WikidataExtractor(),
                          extractor2 = NaifExtractor(),
                          attr1 = "label",
                          attr2 = "label")

            self.merge_on(extractor1 = WikidataExtractor(),
                          extractor2 = NaifExtractor(),
                          attr1 = "alt_label",
                          attr2 = "label")

    @timeit
    def merge_on(self,
                 extractor1: Extractor,
                 extractor2: Extractor,
                 attr1: str | list[str],
                 attr2: str | list[str]):
        """
        Merge entities from two lists if their attributes (attr1 & attr2)
        are identical. Use this for external identifiers that are not
        ambiguous (NAIF is ambiguous).

        TODO:
            Instead of querying all entities from list2, get only entities that
            match from list1's attribute value.

        Args:
            extractor1: first extractor.
            extractor2: second extractor.
            attr1: attribute of the first extractor to match.
            attr2: attribute of the first extractor to match.
        """
        list1_entities = Entity.get_entities_from_list(extractor1, no_equivalent_in = extractor2)
        list2_entities = Entity.get_entities_from_list(extractor2, no_equivalent_in = extractor1)
        list2 = []

        # Pre-loop to get entity2 and its value for attr2
        for entity2 in list2_entities:
            values2 = entity2.get_values_for(attr2, unique = False)
            for value in values2:
                list2.append((entity2, value))
        if len(list2) == 0:
            # Generate mapping for remaining entities
            return

        for entity1 in tqdm(list1_entities):
            #if entity1 in already_linked:
            #    continue
            values1 = entity1.get_values_for(attr1, unique = False)
            if not values1:
                continue

            for value1 in values1:
                for entity2, value2 in list2:
                    if value1 == value2:
                        # Merge entities or synsets
                        entity1.add_synonym(entity2,
                                            extractor1 = extractor1,
                                            extractor2 = extractor2,
                                            no_validation = True,
                                            score_name = "string_match",
                                            subject_match_field = attr1,
                                            object_match_field = attr2,
                                            match_string = value1)

if __name__ == "__main__":
    pass
