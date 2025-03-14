"""
Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from typing import Type, Union, Tuple
from rdflib import Graph as G, Namespace, Literal, Node, URIRef
from rdflib.namespace import RDF, SKOS, DCTERMS, OWL, SDO

from utils import standardize_uri, cut_acronyms
import warnings

class OntologyMapping():
    """
    Metadata mapping between Ontology standards and the extracted dictionaries.
    Use this class for quick dictionary to RDF conversion.
    Stores the Observation Facilities namespace in the class variable OBS.
    Also stores other namespaces.
    """

    _OBS = Namespace("http://semanticweb.org/obspm/ontologies/2025/2/VO-schema/obsfacilities#")
    _GEO = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
    _WB = Namespace("http://http://www.ivoa.net/rdf/messenger#")

    # Mapping from dictionary keys to ontology properties.
    # Properties that are not mapped belong to the OBS namespace.
    _MAPPING = {
        "code": SKOS.notation, # for non-ontological external resources
        "uri": OWL.sameAs, # for ontological external resources
        "url": SDO.url, # facility-list, PDS, SPASE
        "type": RDF.type,
        "label": SKOS.prefLabel,
        "definition": SKOS.definition,
        "alt_label": SKOS.altLabel,
        "part_of": DCTERMS.isPartOf,
        "is_authoritative_for": _OBS.isAuthoritativeFor,
        "waveband": _OBS.waveband, # AAS
        "location": _GEO.location, # AAS, IAU-MPC, SPASE
        "address": SDO.address, # PDS
        # "city": SDO.addressLocality, #IAU-MPC
        "country": SDO.addressCountry, # PDS
        "latitude": _GEO.latitude, # IAU-MPC, SPASE
        "longitude": _GEO.longitude, # IAU-MPC, SPASE
    }
    #_REVERSE_MAPPING = {v: k for k, v in _MAPPING.items()}

    # Objects after a REFERENCE predicate will be an URI and not a Literal.
    # SELF_REF's object's URI are standardized, as they use OBS.
    _SELF_REF = [
        "type",
        "part_of",
        "community", # community of the list's source (see merge.py)
        "is_authoritative_for", # this source is authoritative for the specified communities
    ]

    # Do not standardize object's URI to keep the external reference URI.
    _EXT_REF = [
        "waveband", # see "http://http://www.ivoa.net/rdf/messenger#"
    ]

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

    @property
    def WB(self):
        return OntologyMapping._WB

    @property
    def REFERENCE(self):
        return self._EXT_REF + self._SELF_REF

    @property
    def EXT_REF(self):
        return self._EXT_REF

    @property
    def SELF_REF(self):
        return self._SELF_REF

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
        self.bind("obs", self.OM.OBS)
        self.bind("geo1", self.OM.GEO)
        self.bind("wb", self.OM.WB)
        if filename:
            Graph._graph.parse(filename)
            # init graph

    @property
    def graph(self):
        return Graph._graph

    @property
    def OM(self):
        return Graph._OM

    def __call__(self):
        return Graph._graph

    def __getattr__(self, attr):
        """
        For other attribute
        """
        return getattr(Graph._graph, attr)

    def get_namespace(self,
                      namespace: str) -> Namespace:
        """
        Get the namespace for a source and bind it to the graph
        if it is not binded yet.

        Keyword arguments:
        namespace -- corresponds to the source list's namespace (AAS, NAIF...)
        """
        if namespace == "OBS":
            return self.OM.OBS
        elif namespace == "GEO":
            return self.OM.GEO
        elif namespace == "WB":
            return self.WB # IVOA Messenger (WaveBand)
        namespace_uri = Namespace(str(self.OM.OBS)[:-1] + "/" + namespace + "#")
        # Bind namespace if not binded yet (override = False)
        self.graph.bind(namespace, namespace_uri, override = False)
        return namespace_uri

    def get_label_and_save_alt_labels(
            self,
            label: str,
            namespace: Type) -> str:
        """
        Returns the label with no alternate label or acronym (short label).
        Add the alt labels to the ontology (acronym and full label) if there
        was a change.

        Keyword arguments:
        label -- the label of an entity
        namespace -- the namespace of the label
        """
        short_label, acronym = cut_acronyms(label)
        short_label_uri = standardize_uri(short_label)
        if acronym:
            self.graph.add((namespace[short_label_uri],
                            SKOS.altLabel,
                            Literal(acronym)))
        if short_label != label:
            self.graph.add((namespace[short_label_uri],
                            SKOS.altLabel,
                            Literal(label)))
        return short_label

    def add(
            self,
            params: Tuple[str, str, str],
            source: Type = None):
        """
        Add a RDF triple to the graph.
        Override rdflib's Graph.add() method.
        For conversion from a dictionary, the object type has to be specified,
        as an object can be an ontological reference (URIRef) or a value.

        Keyword arguments:
        params -- a tuple (subj, predicate, obj)
            subj -- the subject of the triple.
            predicate -- the predicate of the triple.
            obj -- the object of the triple.
        source -- the origin of the added triple (ex: AasExtractor)
        """
        if len(params) != 3:
            raise ValueError("params must be a tuple of 3 elements:\
                    subj, pred, obj")
        subj = params[0]
        predicate = params[1]
        obj = params[2]

        # Get the namespace
        if source:
            namespace_subj = self.get_namespace(source.NAMESPACE)
            if predicate == "type":
                namespace_obj = self.OM.OBS # Observation Facility
            elif predicate == "waveband":
                namespace_obj = self.OM.WB
            else:
                namespace_obj = namespace_subj
        else:
            namespace_subj = self.OM.OBS
            namespace_obj = self.OM.OBS

        if type(subj) == str:
            # Convert subject to URI with _OBS
            subj = self.get_label_and_save_alt_labels(subj, namespace_subj)
            subj_uri = namespace_subj[standardize_uri(subj)]
        elif type(subj) == URIRef:
            subj_uri = subj
        else:
            raise TypeError(f"Subject can only be a str or an URIRef.")

        if predicate is None:
            raise ValueError(f"Predicate can not be None.")

        if type(predicate) == str:
            predicate_uri = self.OM.convert_attr(predicate)

        # Convert object(s) into a list
        if type(obj) in (list, tuple, set):
            objs = list(obj) # convert to list
        else:
            objs = [obj]
        for obj in objs:
            # Change object type for certain predicates
            if predicate == "label":
                obj = self.get_label_and_save_alt_labels(obj, namespace_obj)
            if predicate in self.OM.SELF_REF:
                obj = self.get_label_and_save_alt_labels(obj, namespace_obj)
                obj_value = namespace_obj[standardize_uri(obj)]
            elif predicate in self.OM.EXT_REF:
                 # Do not standardize an external URI
                obj_value = namespace_obj[obj]
            else:
                obj_value = Literal(obj)

            # Add to the graph
            self.graph.add((subj_uri, predicate_uri, obj_value))

        if source:
            source_uri = self.OM.OBS[standardize_uri(source.URI)]
            self.graph.add((subj_uri, self.OM.OBS["source"], source_uri))

if __name__ == "__main__":
    pass
