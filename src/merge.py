#!/bin/python3

"""
Perform merging between all lists.
Compute scores between entities from the lists in the input ontology
and create CandidatePair entities to save the similarity scores.
Finally, group entities in a Synonym Set if their similarity score
is high enough.

Arguments:

Author:
    Liza Fretel (liza.fretel@obsmp.fr)
"""
from argparse import ArgumentParser
from data_merger.candidate_pair import CandidatePairsManager, CandidatePairsMapping
from data_merger.identifier_merger import IdentifierMerger
from data_merger.graph import Graph
from data_merger.scorer.acronym_scorer import AcronymScorer
from data_merger.scorer.cosine_similarity_scorer import CosineSimilarityScorer
from data_merger.synonym_set import SynonymSetManager
from data_merger.scorer.fuzzy_scorer import FuzzyScorer
from data_updater.extractor.aas_extractor import AasExtractor
from data_updater.extractor.naif_extractor import NaifExtractor
from data_updater.extractor.spase_extractor import SpaseExtractor
from data_updater.extractor.wikidata_extractor import WikidataExtractor
from utils.performances import timeit

class Merger():


    def __init__(self,
                 ontology_file: str = ""):
        SynonymSetManager()
        self._graph = Graph()
        self._graph.parse(ontology_file) # = Graph(ontology_file)
        if ontology_file:
            self.init_graph() # Create basic classes


    @property
    def graph(self):
        return self._graph


    def init_graph(self):
        """
        Add CandidatePair & SynSet classes in the OBS namespace.
        """
        # TODO use update() on graph.


    def merge_identifiers(self):
        im = IdentifierMerger(self._graph)
        CPM_wiki_naif = CandidatePairsManager(WikidataExtractor.NAMESPACE,
                                              NaifExtractor.NAMESPACE)

        # merge wiki naif if the namespaces are available.
        if im.merge_wikidata_naif(CPM_wiki_naif):

            # Disambiguate cases with two candidates
            # (necessary because NAIF has duplicate identifiers
            # for different entities)
            CPM_wiki_naif.disambiguate_candidates(scores = [FuzzyScorer])
            CPM_wiki_naif.save_all() # Save remaining candidate pairs.
        del(CPM_wiki_naif)
        del(im)


    def merge_mapping(self):
        """
        Computes a mapping between classes from namespaces two by two.
        Generates candidate pairs before performing disambiguation.
        Repeat for every mapping until the mapping is done.
        Mapping will take already existing Synonym Sets into account,
        and link a term to a synonym set instead of creating a new one
        if there is a synonym set that already exists for one of the
        candidates.
        TODO FIXME if there are two candidates in a synset, then merge synsets
        """

        # Create mapping between unlinked available lists
        CPM_aas_spase = CandidatePairsMapping(AasExtractor(),
                                              SpaseExtractor())
        CPM_aas_spase.generate_mapping(self.graph)
        CPM_aas_spase.disambiguate(scores = [FuzzyScorer,
                                             CosineSimilarityScorer,
                                             AcronymScorer])

        # Deal with remaining candidate pairs (TODO)
        #self.CPM.disambiguate(self.graph,
        #                      NaifExtractor(),
        #                      WikidataExtractor())

        # CPM_aas_spase.save_all()
        CPM_aas_spase.save_to_graph()
        del(CPM_aas_spase)

        # /!\ Save the synonym sets in the graph (do not remove)
        SynonymSetManager._SSM.save_all()

    @timeit
    def write(self,
              output_ontology: str):
        print(f"Writing the result ontology into {output_ontology}...")
        with open(output_ontology, 'w') as file:
            file.write(self.graph.serialize())


def main(input_ontology: str = "",
         output_ontology: str = ""):
    merger = Merger(input_ontology)
    merger.merge_identifiers()
    merger.merge_mapping()
    merger.write(output_ontology)


if __name__ == "__main__":

    parser = ArgumentParser(
        prog = "updater.py",
        description = "Compute scores between entities from different lists " +
            "and create CandidatePair with similarity scores between entities.")

    parser.add_argument("-i",
                    "--input-ontology",
                    dest = "input_ontology",
                    default = "",
                    type = str,
                    required = True,
                    help = "Input ontology on which we will perform " +
                    "entity mapping between different sources.")

    parser.add_argument("-o",
                    "--output-ontology",
                    dest = "output_ontology",
                    default = "output_merged.ttl",
                    type = str,
                    required = False,
                    help = "Output ontology file to save the merged data.")

    args = parser.parse_args()
    main(args.input_ontology, args.output_ontology)