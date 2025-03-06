"""
Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from typing import Type, Union, Tuple
from rdflib import Graph as G, Namespace, Literal, URIRef, Node
from rdflib.namespace import RDF, SKOS
import warnings

class OntologyMapping():
    """
    Metadata mapping between Ontology standards and the extracted dictionaries.
    Use this class for quick dictionary to RDF conversion.
    Stores the Observation Facilities namespace in the class variable OBS.
    Also stores other namespaces.
    """

    _OBS = Namespace("http://semanticweb.org/obspm/ontologies/2025/2/VO-schema/obsfacilities")
    _GEO = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")

    _MAPPING = {
        "type": RDF.type,
        "label": SKOS.prefLabel,
        "definition": SKOS.definition,
        "alt_label": SKOS.altLabel,
        "uri": URIRef,
        "wavelength": _OBS.wavelength, # AAS
        "location": _GEO.location, # AAS
    }
    #_REVERSE_MAPPING = {v: k for k, v in _MAPPING.items()}

    def __init__(self):
        pass

    @property
    def graph(self):
        return self._graph

    def convert_attr(
            self,
            attr: Union[str, Node]):
        """
        Convert an attribute accordingly to the mapping.
        For example, if attribute is "definition", returns SKOS.definition.

        Keyword arguments:
        attr -- the attribute to convert
        """
        if type(attr) == str:
            return OntologyMapping._MAPPING.get(
                    attr,
                    self.OBS[attr])
        else:
            return attr
            # return OntologyMapping._REVERSE_MAPPING.get(attr, None)

    @property
    def OBS(self):
        return OntologyMapping._OBS

    @property
    def GEO(self):
        return OntologyMapping._GEO

    def __getattr__(
            self,
            attr):
        return self.convert_attr(attr)

class Graph():
    """
    Instanciate a rdflib Graph as a singleton to prevent multiple
    instantiation. Replace rdflib.Graph and reuses rdflib.Graph's attributes.

    Usage:
        from graph import Graph
        g = Graph() # instanciate
        obs = g.OBS # get Observation Facility namespace

        # Then, use g as a rdflib graph object:
        g.add((g.OBS["subj"], URIRef("predicate"), URIRef("obj")))
    """
    _graph = None
    _OM = OntologyMapping()
    _warned = False # Warn only once for multiple instantiation.

    def __init__(self, filename = ""):
        if Graph._graph is not None:
            if not Graph._warned:
                warnings.warn("Can not create another instance of MyGraph. Ignored.")
                Graph._warned = True
            return
        Graph._graph = G()
        self.bind("obs", self.OBS)
        self.bind("geo", self.GEO)
        if filename:
            Graph._graph.parse(filename)
            # init graph

    @property
    def graph(self):
        return Graph._graph

    @property
    def OBS(self):
        return self.OM.OBS

    @property
    def GEO(self):
        return self.OM.GEO

    @property
    def OM(self):
        return Graph._OM

    def __call__(self):
        return Graph._graph

    def __getattr__(self, attr):
        return getattr(Graph._graph, attr)

    def add(
            self,
            params: Tuple[str, str, str],
            objtype: Type = Literal,
            source: Union[URIRef, str] = None):
        """
        Add a RDF triple to the graph.
        Override rdflib's Graph.add() method.
        For conversion from a dictionary, the object type has to be specified,
        as an object can be an ontological reference (URIRef) or a value.

        Keyword arguments:
        subj -- the subject of the triple.
        predicate -- the predicate of the triple.
        obj -- the object of the triple.
        objtype -- the type of the object (Literal, URIRef...)
        source -- the origin of the added triple (URL, URI...)
        """
        if len(params) != 3:
            raise ValueError("params must be a tuple of 3 elements:\
                    subj, pred, obj")
        subj = params[0]
        predicate = params[1]
        obj = params[2]

        if type(subj) == str:
            subj_uri = self.OBS[subj]  # Convert subject to URI with _OBS

        if type(predicate) == str:
            predicate = self.OM.convert_attr(predicate)

        if predicate is None:
            raise ValueError(f"Predicate can not be None.")

        obj_value = objtype(obj)

        # Add to the graph
        self._graph.add((subj_uri, predicate, obj_value))

        # TODO reification to integrate source

if __name__ == "__main__":
    g = Graph()
    g.add((g.OBS["Liza"], URIRef("eats"), URIRef("pizza")))
    g2 = Graph() # should print a warning
    # g and g2 point to the same Graph object (singleton)
    g2.add((g.OBS["Laura"], URIRef("drinks"), URIRef("coffee")))
    print(g.serialize()) # both g and g2 have Laura
