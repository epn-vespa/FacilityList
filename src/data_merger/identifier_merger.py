"""
Methods to merge entities on cross-list identifiers.
Entities that are merged this way do not need to be paired
for comparison. They can already be in the same synset and
they do not need to be mapped as candidates in candidate pairs.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from typing import List
import uuid
from data_merger.candidate_pair import CandidatePair
from data_merger.graph import Graph
from data_updater.extractor import wikidata_extractor

from rdflib.namespace import OWL, SKOS
from rdflib import Literal, XSD

class IdentifierMerger():

    def __init__(self,
                 graph: Graph):
        self._graph = graph

    def merge_wikidata_naif(self) -> List[CandidatePair]:
        """
        Merge Wikidata and NAIF entities if both namespaces
        exist using the NAIF_ID relation of Wikidata.

        Troubleshooting:
            Some NAIF_ID are used for more than one NAIF entity.
            Maybe we should still create a candidate pair for wikidata-naif
            entities and disambiguate later (using fuzzy etc).
        errors:
            naif:lunar-flashlight / wikidata:yohkoh-gamma-and-x-ray-solar-satellite
                (NAIF ID -164)
        """
        graph = self._graph

        candidate_pairs = []

        if (graph.is_available("wikidata")
            and graph.is_available("naif")):
            # Loop on each wikidata class
            wde = wikidata_extractor.WikidataExtractor()
            wikidata_entities = graph.get_entities_from_list(wde)
            for wikidata_entity in wikidata_entities:
                for _, _, naif_code in graph.triples((wikidata_entity,
                                                      graph.OBS["NAIF_ID"],
                                                      None)):
                    naif_ids = []
                    for naif_id, _, _ in graph.triples((None,
                                                        SKOS.notation,
                                                        Literal(naif_code, datatype = XSD.string))):
                        naif_ids.append(naif_id)
                    if len(naif_ids) == 1:
                        # There is only one NAIF entity with this ID.
                        graph.add((wikidata_entity, OWL.equivalentClass, naif_ids[0]))
                        graph.add((naif_ids[0], OWL.equivalentClass, wikidata_entity))
                    elif len(naif_ids) > 1:
                        # need further disambiguation. Use a CandidatePair.
                        for naif_id in naif_ids:
                            candidate_pairs.append(CandidatePair(naif_id, wikidata_entity))
        return candidate_pairs

if __name__ == "__main__":
    im = IdentifierMerger()
    im.merge_wikidata_naif()