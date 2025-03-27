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
from data_merger.candidate_pair import CandidatePairsManager
from data_merger.identifier_merger import IdentifierMerger
from data_merger.graph import Graph
from data_merger.synset import SynonymSetManager
from data_updater.extractor.naif_extractor import NaifExtractor
from data_updater.extractor.wikidata_extractor import WikidataExtractor


class Merger():

    _CPM = CandidatePairsManager()

    _SSM = SynonymSetManager()


    def __init__(self,
                 ontology_file: str = ""):
        self._graph = Graph(ontology_file)
        if not ontology_file:
            self.init_graph() # Create basic classes


    @property
    def graph(self):
        return self._graph


    @property
    def CPM(self):
        """
        Candidate Pair Manager getter
        """
        return self._CPM


    @property
    def SSM(self):
        """
        Synonym Set Manager getter
        """
        return self._SSM


    def init_graph(self):
        """
        Add CandidatePair & SynSet classes in the OBS namespace.
        """
        # TODO use update() on graph.


    def merge_identifiers(self):
        im = IdentifierMerger(self._graph)
        candidate_pairs_wiki_naif = im.merge_wikidata_naif(self.SSM)
        self.CPM.add_candidate_pairs(candidate_pairs_wiki_naif,
                                     NaifExtractor.NAMESPACE,
                                     WikidataExtractor.NAMESPACE)
        # Disambiguate cases with two candidates
        # (necessary because NAIF has duplicate identifiers
        # for different entities)
        self.CPM.disambiguate(self.graph,
                              self.SSM,
                              NaifExtractor(),
                              WikidataExtractor())

        # /!\ Save the synonym sets in the graph (do not remove)
        self.SSM.save_all()


def main(input_ontology: str = "",
         output_ontology: str = ""):
    merger = Merger(input_ontology)
    merger.merge_identifiers()

    if output_ontology:
        # save the CandidatePairs.
        merger.CPM.save_all()
        with open(output_ontology, 'w') as file:
            file.write(merger.graph.serialize())


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
                    default = "",
                    type = str,
                    required = False,
                    help = "Output ontology file to save the merged data.")

    args = parser.parse_args()
    main(args.input_ontology, args.output_ontology)