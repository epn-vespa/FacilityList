#a!/bin/python3

"""Perform extraction for all lists. Add data to the VO ontology without
checking if it already exists from another list. This is a preliminary
step before we filter out the duplicate entities.

Arguments:
-- VO ontology file (turtle)
-- objects type (ex: Observatory if it is a list of observatories).
   The object type must be in the taxonomy of the ontology.
   Add it to the concepts' taxonomy otherwise.
"""

from rdflib.namespace import SKOS

from argparse import ArgumentParser
from rdflib import Namespace, URIRef
from graph import Graph # rdflib.Graph singleton with OBS namespace
from typing import List
from aas_extractor import AasExtractor
from utils import label_to_uri

class Merger():
    def __init__(self,
            ontology_file: str = ""):
        self._graph = Graph(ontology_file)

    @property
    def graph(self):
        return self._graph

    def merge(self,
            data: List,
            source: str = "",
            cat: str = "UFO"):
        """
        Adds the data from the dict to the Ontology.

        Keyword arguments:
        data -- a list of dictionaries like [{"uri":"a", "Label":"b"}]
        source -- the URL of the data source
        cat -- the category of the objects in the list if they are
        not already in the dictionary's features.
        """
        for identifier, features in data.items():
            # TODO Identifier is the identifier according to the source list.
            # We want to save it with the source. Maybe use reification.
            # get label
            if "label" in features:
                label = features["label"]
                subj_uri = label_to_uri(label)
            else:
                subj_uri = identifier
            for predicate, obj in features.items():
                self.graph.add((subj_uri, predicate, obj), source = source)
            if "type" not in features: 
                self.graph.add((subj_uri, "type", cat), source = source)
            # Create the OBS uri
            

def main(input_ontology: str = ""):
    aas_extractor = AasExtractor()
    data_aas = aas_extractor.extract()
    merger = Merger(input_ontology)
    merger.merge(data_aas, source = aas_extractor.get_source())
    print(merger.graph.serialize())

if __name__ == "__main__":
    parser = ArgumentParser(
        prog = "merge.py",
        description = "Merge data from different lists to ontology.")
    parser.add_argument("-i",
            "--input-ontology",
            dest = "input_ontology",
            default = "",
            type = str,
            required = False,
            help = "Input ontology that will be merged with new data.")
    args = parser.parse_args()
    main(args.input_ontology)
