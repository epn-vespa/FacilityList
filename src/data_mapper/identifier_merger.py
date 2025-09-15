"""
Methods to merge entities on cross-list identifiers.
Entities that are merged this way do not need to be paired
for comparison. They can already be in the same synset and
they do not need to be mapped as candidates in candidate pairs.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from tqdm import tqdm
from graph import Graph
from data_mapper.candidate_pair import CandidatePair, CandidatePairsManager, CandidatePairsMapping
from data_mapper.entity import Entity
from data_mapper.synonym_set import SynonymSet, SynonymSetManager
from data_updater.extractor import wikidata_extractor

from rdflib.namespace import SKOS
from rdflib import Literal

from utils.performances import timeit


class IdentifierMerger():


    def __init__(self):
        pass


    @timeit
    def merge_wikidata_naif(self,
                            CPM: CandidatePairsManager) -> bool:
        """
        Merge Wikidata and NAIF entities if both namespaces
        exist using the NAIF_ID relation of Wikidata.
        Return False if Wikidata or NAIF namespace do not exist.

        Troubleshooting:
            Some NAIF_ID are used for more than one NAIF entity.
            Maybe we should still create a candidate pair for wikidata-naif
            entities and disambiguate later (using fuzzy etc).
        errors:
            naif:lunar-flashlight / wikidata:yohkoh-gamma-and-x-ray-solar-satellite
                (NAIF ID -164)
        """
        graph = Graph()

        # Loop on each wikidata class
        wikidata_entities = graph.get_entities_from_list(CPM.list1)

        for wikidata_uri, synset in wikidata_entities:
            if synset is not None:
                wikidata_entity = SynonymSetManager._SSM.get_synset_for_entity(wikidata_uri,
                                                                               synset)
            else:
                wikidata_entity = Entity(wikidata_uri)
            naif_codes = wikidata_entity.get_values_for("NAIF_ID")
            for naif_code in naif_codes:
                naif_ids = []
                for naif_id, _, _ in graph.triples((None,
                                                    SKOS.notation,
                                                    Literal(naif_code))):#, datatype = XSD.string))):
                    naif_ids.append(naif_id)
                if len(naif_ids) == 1:
                    # There is only one NAIF entity with this ID.
                    naif_entity = Entity(naif_ids[0])
                    SynonymSetManager._SSM.add_synpair(wikidata_entity,
                                                       naif_entity)
                elif len(naif_ids) > 1:
                    # Ambiguous NAIF identifier.
                    # need further disambiguation with CandidatePair.
                    for naif_id in naif_ids:
                        naif_entity = Entity(naif_id)
                        CPM.add_candidate_pairs(CandidatePair(naif_entity,
                                                              wikidata_entity))
        return True


    @timeit
    def merge_on(self,
                 CPM: CandidatePairsMapping,
                 attr1: str,
                 attr2: str,
                 map_remaining: bool = False):
        """
        Merge entities from two lists if their attributes (attr1 & attr2)
        are identical. Use this for external identifiers that are not
        ambiguous (NAIF is ambiguous).

        Keyword arguments:
        CPM -- Candidate Pairs Mapping of the two lists
        attr1 -- attribute of list1 to compare with attr2
        attr2 -- attribute of list2 to compare with attr1
        map_remaining -- whether to generate a mapping for the entities that
                         were not merged after merging on attr1 & attr2.
        """
        graph = Graph()
        list1_entities = graph.get_entities_from_list(CPM.list1,
                                                      # no_equivalent_in=CPM.list2,
                                                      has_attr = [attr1])
        list2_entities = graph.get_entities_from_list(CPM.list2,
                                                      # no_equivalent_in=CPM.list1,
                                                      has_attr = [attr2])
        #already_linked = SynonymSetManager._SSM.get_entities_in_synset(CPM.list1,
        #                                                               CPM.list2)
        list2 = []


        # Pre-loop to get entity2 and its value for attr2
        for entity2, synset2 in list2_entities:
            #if entity2 in already_linked:
            #    continue
            if synset2 is not None:
                entity2 = SynonymSetManager._SSM.get_synset_for_entity(entity2, synset2)
            else:
                entity2 = Entity(entity2)
            value2 = entity2.get_values_for(attr2, unique = True)
            if not value2:
                continue
            list2.append((entity2, value2))
        if len(list2) == 0:
            # Generate mapping for remaining entities
            return

        merged = 0
        total_entity1 = 0

        for entity1, synset1 in list1_entities:
            #if entity1 in already_linked:
            #    continue
            total_entity1 += 1
            if synset1 is not None:
                entity1 = SynonymSetManager._SSM.get_synset_for_entity(entity1, synset1)
            else:
                entity1 = Entity(entity1)
            value1 = entity1.get_values_for(attr1, unique = True)
            if not value1:
                break

            for entity2, value2 in list2:
                if value1 == value2:
                    # Merge entities or synsets
                    SynonymSetManager._SSM.add_synpair(entity1, entity2)
                    merged += 1

        # Save the newly added synonym sets into the graph
        SynonymSetManager._SSM.save_all()

        # Generate mapping for the remaining entities
        if (map_remaining and
            merged < total_entity1 and
            merged < len(list2)):
            CPM.generate_mapping()


if __name__ == "__main__":
    pass
