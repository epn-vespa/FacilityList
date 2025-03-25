"""
Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import os
import warnings

from typing import List
from rdflib import URIRef
from data_updater.graph import Graph as G
from data_updater.extractor.extractor import Extractor


class Graph(G):

    """
    Instanciate a rdflib Graph as a singleton to prevent multiple
    instantiation. Replace rdflib.Graph and reuses rdflib.Graph's attributes.
    This overrides data_updater's Graph to add other functions.

    Usage:
        from graph import Graph
        g = Graph() # instanciate
        obs = g.OBS # get Observation Facility namespace

        # Then, use g as a rdflib graph object:
        g.add((g.OBS["subj"], URIRef("predicate"), URIRef("obj")))
    """


    def __init__(self,
                 filename: str):
        """
        Initialize the graph.

        Keyword arguments:
        filename -- input ontology file
        """
        if Graph._graph is not None:
            if not Graph._warned:
                warnings.warn("Can not create another instance of MyGraph. Ignored.")
                Graph._warned = True
            return
        Graph._graph = G()

        input_ontology_namespaces = []
        if os.path.exists(filename):
            Graph._graph.parse(filename)
            for namespace in list(Graph._graph.namespaces()):
                if namespace[0] in Extractor.AVAILABLE_NAMESPACES:
                    input_ontology_namespaces.append(namespace[0])
        # Used to control inter-list identifiers (example: NAIF-Wikidata)
        # If two lists that have cross identifiers are in the namespaces,
        # then they can be linked with OWL.equivalentClass.
        self._available_namespaces = input_ontology_namespaces


    @property
    def graph(self):
        return Graph._graph


    def __call__(self):
        return Graph._graph


    def __getattr__(self, attr):
        """
        For other attribute
        """
        return getattr(Graph._graph, attr)


    """
    @property
    def OBS(self):
        return OntologyMapping._OBS


    @property
    def GEO(self):
        return OntologyMapping._GEO


    @property
    def WB(self):
        return OntologyMapping._WB
    """


    def is_available(self,
                     namespace: str) -> bool:
        """
        Returns true if the namespace of a list is available in the graph.

        Keyword arguments:
        namespace -- the namespace to test
        """
        return namespace in self._available_namespaces


    def get_entities_from_list(self,
                               # namespace: str,
                               source: Extractor) -> List[URIRef]:
        """
        Get all the entities that come from a list.

        Keyword arguments:
        namespace -- the list's namespace
        source -- the source extractor to get entities from
        """

        query = f"""
        SELECT ?entity
        WHERE {{
            ?entity obs:source obs:{source.URI} .
        }}
        """

        results = self.graph.query(query)
        return [x[0] for x in results]