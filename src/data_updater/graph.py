"""
Instanciate a graph only once as a singleton for all other classes.
The Graph class replaces rdflib.Graph and reuses Graph's attributes.
It stores the Observation Facilities namespace in the class variable OBS.

Usage:
    from graph import Graph
    g = Graph() # instanciate
    obs = g.OBS # get Observation Facility namespace

    # Then, use g as a rdflib graph object:
    g.add((g.OBS["subj"], URIRef("predicate"), URIRef("obj")))

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
from rdflib import Graph as G, Namespace
import warnings

class Graph():
    _graph = None
    _OBS = Namespace("http://semanticweb.org/obspm/ontologies/2025/2/VO-schema/obsfacilities")
    _warned = False

    def __init__(self, filename = ""):
        if Graph._graph is not None:
            if not Graph._warned:
                warnings.warn("Can not create another instance of MyGraph. Ignored.")
                Graph._warned = True
            return
        Graph._graph = G()
        self.bind("obs", Graph._OBS)
        if filename:
            Graph._graph.parse(filename)
            # init graph

    @property
    def graph(self):
        return Graph._graph

    @property
    def OBS(self):
        return Graph._OBS

    def __call__(self):
        return Graph._graph

    def __getattr__(self, attr):
        return getattr(Graph._graph, attr)

if __name__ == "__main__":
    from rdflib import URIRef
    g = Graph()
    g.add((g.OBS["Liza"], URIRef("eats"), URIRef("pizza")))
    g2 = Graph() # should print a warning
    g2.add((g.OBS["Laura"], URIRef("drinks"), URIRef("coffee")))
    print(g.serialize()) # should print 
