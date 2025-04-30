"""
Methods to merge entities on cross-list identifiers.
Entities that are merged this way do not need to be paired
for comparison. They can already be in the same synset and
they do not need to be mapped as candidates in candidate pairs.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from graph import Graph
from data_merger.candidate_pair import CandidatePair, CandidatePairsManager
from data_merger.entity import Entity
from data_merger.synonym_set import SynonymSetManager
from data_updater.extractor import wikidata_extractor

from rdflib.namespace import SKOS
from rdflib import Literal, XSD

from utils.performances import timeit


class IdentifierMerger():


    def __init__(self,
                 graph: Graph):
        self._graph = graph


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
        graph = self._graph

        # Loop on each wikidata class
        wde = wikidata_extractor.WikidataExtractor()
        wikidata_entities = graph.get_entities_from_list(wde)

        for wikidata_uri, synset in wikidata_entities:
            if synset is not None:
                wikidata_entity = SynonymSetManager().get_synset_for_entity(wikidata_uri)
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
                    SynonymSetManager._SSM.add_synset(wikidata_entity,
                                                      naif_entity)
                elif len(naif_ids) > 1:
                    # Ambiguous NAIF identifier.
                    # need further disambiguation with CandidatePair.
                    for naif_id in naif_ids:
                        naif_entity = Entity(naif_id)
                        CPM.add_candidate_pairs(CandidatePair(naif_entity,
                                                              wikidata_entity))
        return True


if __name__ == "__main__":
    pass