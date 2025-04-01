"""
Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import os
import warnings

from typing import Tuple
from rdflib import Graph as G, Namespace, Literal, URIRef, XSD
from rdflib.namespace import RDF, SKOS, DCTERMS, OWL, SDO, DCAT, FOAF
from data_updater.extractor.extractor import Extractor
from utils.utils import standardize_uri, cut_acronyms, get_datetime_from_iso, cut_language_from_string



class OntologyMapping():
    """
    Metadata mapping between Ontology standards and the extracted dictionaries.
    Use this class for quick dictionary to RDF conversion.
    Stores the Observation Facilities namespace in the class variable OBS.
    Also stores other namespaces.
    """

    _OBS = Namespace("https://voparis-ns.obspm.fr/rdf/obsfacilities#")
    _GEO = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
    _WB = Namespace("http://www.ivoa.net/rdf/messenger#")

    # Mapping from dictionary keys to ontology properties.
    # Properties that are not mapped belong to the OBS namespace.
    _MAPPING = {
        "code": {"pred": SKOS.notation,
                 "objtype": XSD.string}, # SKOS.exactMatch, # for non-ontological external resources
        "uri": {"pred": OWL.sameAs,
                "objtype": XSD.string}, # for ontological external resources
        "equivalent_class": {"pred": OWL.equivalentClass,
                             "objtype": None}, # for resources that are the same
        "url": {"pred": SDO.url,
                "objtype": XSD.string}, # facility-list, PDS, SPASE
        "ext_ref": {"pred": FOAF.page,
                    "objtype": XSD.string}, #SDO.about, # RDFS.seeAlso, # for corpus pages (wikipedia etc)
        "type": {"pred": RDF.type,
                 "objtype": URIRef},
        "label": {"pred": SKOS.prefLabel,
                  "objtype": XSD.string},
        "definition": {"pred": SKOS.definition,
                       "objtype": XSD.string},
        "alt_label": {"pred": SKOS.altLabel,
                      "objtype": XSD.string},
        "is_part_of": {"pred": DCTERMS.isPartOf,
                       "objtype": URIRef}, # PDS
        "has_part": {"pred": DCTERMS.hasPart,
                     "objtype": URIRef}, # PDS
        "is_authoritative_for": {"pred": _OBS.isAuthoritativeFor,
                                 "objtype": URIRef},
        "community": {"pred": _OBS.community,
                      "objtype": URIRef},
        "waveband": {"pred": _OBS.waveband,
                     "objtype": URIRef}, # AAS. Reference to IVOA's Messengers
        "location": {"pred": _GEO.location,
                     "objtype": XSD.string}, # AAS, IAU-MPC, SPASE
        "address": {"pred": SDO.address,
                    "objtype": XSD.string}, # PDS
        # "city": {"pred": SDO.addressLocality,
        #          "objtype": XSD.string}, #IAU-MPC
        "country": {"pred": SDO.addressCountry,
                    "objtype": XSD.string},# PDS
        "latitude": {"pred": _GEO.latitude,
                     "objtype": XSD.float}, # IAU-MPC, SPASE
        "longitude": {"pred": _GEO.longitude,
                      "objtype": XSD.float}, # IAU-MPC, SPASE
        "start_date": {"pred": DCAT.startDate,
                       "objtype": XSD.dateTime}, # SPASE
        "end_date": {"pred": DCAT.endDate,
                     "objtype": XSD.dateTime},
        "stop_date": {"pred": DCAT.endDate,
                      "objtype": XSD.dateTime},
        "launch_date": {"pred": _OBS.launch_date,
                        "objtype": XSD.dateTime} # Wikidata
    }

    # Objects after a REFERENCE predicate will be an URI and not a Literal.
    # SELF_REF's object's URI are standardized, as they use OBS.
    _SELF_REF = [
        "type",
        "is_part_of",
        "has_part",
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


    def convert_attr(
            self,
            attr: str):
            """
            Convert an attribute accordingly to the mapping.
            For example, if attribute is "definition", returns SKOS.definition.

            Keyword arguments:
            attr -- the attribute to convert
            """
            if type(attr) == str:
                attr = OntologyMapping._MAPPING.get(
                    attr,
                    self.OBS[attr])
                if type(attr) == dict:
                    attr = attr["pred"]
                return attr
            else:
                return attr
                # return OntologyMapping._REVERSE_MAPPING.get(attr, None)


    def __getattr__(
            self,
            attr):
        return getattr(Graph._graph, attr)


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


    def __init__(self,
                 filename = ""):
        """
        Initialise the graph. Bind basic namespaces to the graph.
        Return the namespaces already in the graph.

        Keyword arguments:
        filename -- the input ontology to parse
        """
        if Graph._graph is not None:
            if not Graph._warned:
                warnings.warn("Can not create another instance of MyGraph. Ignored.")
                Graph._warned = True
            return
        Graph._graph = G()
        self.bind("obs", self.OM.OBS)
        self.bind("geo1", self.OM.GEO)
        self.bind("wb", self.OM.WB)

        if os.path.exists(filename):
            Graph._graph.parse(filename)


    @property
    def graph(self):
        return Graph._graph


    @property
    def OM(self):
        return Graph._OM


    @property
    def OBS(self):
        return self.OM.OBS


    def __call__(self):
        return Graph._graph


    def __getattr__(self, attr):
        """
        For other attribute
        """
        return getattr(Graph._graph, attr)


    def convert_pred_and_obj(
            self,
            pred: str,
            obj: str,
            language: str,
            extractor: Extractor) -> Tuple:
        """
        Convert a predicate and an object accordingly to the mapping.
        For example, for "definition", predicate becomes SKOS.definition,
        and obj becomes Literal(obj, datatype = XSD.string, lang = language).

        Keyword arguments:
        pred -- the predicate to convert
        obj -- the object to convert
        language -- the language of the XSD.string of the object if any
        extractor -- the extractor used to extract the data
        """
        if type(pred) != str:
            return pred, obj

        pred_objtype = OntologyMapping._MAPPING.get(pred, None)
        if pred_objtype is None:
            # The predicate is not mapped, use the OBS namespace
            # and create the predicate.
            pred_uri = self.OM.OBS[pred]
            obj_uri = Literal(obj, lang = language)
            return pred_uri, obj_uri

        pred_uri = pred_objtype["pred"]
        objtype = pred_objtype["objtype"]

        if objtype == XSD.dateTime:
            # If the date is negative, it is not taken into account by
            # isoformat. We need to save it as a string instead.
            if obj[0] == '-':
                objtype = XSD.string

        if objtype != XSD.string:
            language = None
        elif language is not None:
            # Constraint in rdflib:
            # a new term can only have language or objtype
            objtype = None

        # Get and save alt labels
        if extractor:
            namespace_obj = self.get_namespace(extractor.NAMESPACE)
        else:
            namespace_obj = self.OM.OBS

        if pred == "label" or pred in self.OM.SELF_REF:
            obj = self.get_label_and_save_alt_labels(obj,
                                                     namespace_obj,
                                                     extractor = extractor,
                                                     language = language)

        # Convert obj to obj_uri using datatype
        if objtype != URIRef:
            if objtype == XSD.dateTime:
                obj = get_datetime_from_iso(obj)
            obj_uri = Literal(obj,
                              lang = language,
                              datatype = objtype)
        else:
            # objtype is URIRef
            if extractor:
                if pred == "type":
                    if (hasattr(extractor, "IS_ONTOLOGICAL")
                        and extractor.IS_ONTOLOGICAL):
                        # The object's namespace is the source's namespace
                        pass
                    else:
                        namespace_obj = self.OM.OBS
                elif pred == "waveband":
                    # waveband is in IVOA vocabulary so it has its own ns
                    namespace_obj = self.OM.WB
            else:
                namespace_obj = self.OM.OBS

            obj = self.get_label_and_save_alt_labels(obj,
                                                     namespace_obj,
                                                     extractor = extractor,
                                                     language = language)

            # standardize obj_uri
            obj = standardize_uri(obj)
            obj_uri = namespace_obj[obj]

        """
        if predicate == "label":
            obj = self.get_label_and_save_alt_labels(obj,
                                                        namespace_obj,
                                                        language = language)
        if predicate in self.OM.SELF_REF:
            obj = self.get_label_and_save_alt_labels(obj,
                                                        namespace_obj,
                                                        language = language)
            obj_value = namespace_obj[standardize_uri(obj)]
        elif predicate in self.OM.EXT_REF:
                # Do not standardize an external URI
            obj_value = namespace_obj[obj]
        else:
            obj_value = Literal(obj, lang = language, type = xsdtype)"
        """
        return pred_uri, obj_uri


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
            namespace: Namespace,
            extractor: Extractor,
            language: str = None) -> Tuple[str, str]:
        """
        Returns the label with no alternate label nor acronym (short label).
        Add the alt labels to the ontology (acronym and full label) if there
        was a change.

        Do not add acronym if the acronym is for the location (or other entities in the label)

        Keyword arguments:
        label -- the label of an entity
        namespace -- the namespace of the label
        language -- the language of the label if any
        """
        acronym_of_location = False
        if hasattr(extractor, "LOCATION_DELIMITER"):
            #label_without_location = cut_location(label,
            #                                      delimiter = extractor.LOCATION_DELIMITER,
            #                                      alt_labels = set())
            if label.count(extractor.LOCATION_DELIMITER) == 1:
                # if there is an acronym, it is the acronym of the location,
                # not of the entity.
                acronym_of_location = True

        short_label, acronym = cut_acronyms(label)
        short_label_uri = standardize_uri(short_label)

        if acronym and not acronym_of_location:
            self.graph.add((namespace[short_label_uri],
                            SKOS.altLabel,
                            Literal(acronym, lang = language)))
        # location's acronym may become the whole entity's acronym.
        if short_label != label:
            self.graph.add((namespace[short_label_uri],
                            SKOS.altLabel,
                            Literal(label, lang = language)))
        return short_label # Do not return an URI


    def add(
            self,
            params: Tuple[str, str, str],
            extractor: Extractor = None):
        """
        Add a RDF triple to the graph.
        Override rdflib's Graph.add() method.
        For conversion from a dictionary, the object type has to be specified,
        as an object can be an ontological reference (URIRef) or a value. If
        the object is a str that converts to a Literal, it will be parsed
        on '@' to detect the language.

        Keyword arguments:
        params -- a tuple (subj, predicate, obj)
            subj -- the subject of the triple.
            predicate -- the predicate of the triple.
            obj -- the object of the triple.
        extractor -- the extractor of the added triple (ex: AasExtractor)
        """
        if len(params) != 3:
            raise ValueError("params must be a tuple of 3 elements:\
                    subj, pred, obj")
        subj = params[0]
        predicate = params[1]
        obj = params[2]

        if not subj:
            # subj is "" or None
            return

        # Get the namespace of the subject
        if extractor:
            namespace_subj = self.get_namespace(extractor.NAMESPACE)
        else:
            namespace_subj = self.OM.OBS

        if type(subj) == str:
            # Convert subject to URI with _OBS
            # FIXME do we need to save alt labels for subj ?
            subj = self.get_label_and_save_alt_labels(subj,
                                                      namespace = namespace_subj,
                                                      extractor = extractor)
            subj_uri = namespace_subj[standardize_uri(subj)]
        elif type(subj) == URIRef:
            subj_uri = subj
        else:
            raise TypeError(f"Subject ({subj}) can only be a str or an URIRef. "
                            + f"Got a {type(subj)}.")

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
            # Ignore None and empty obj
            language = None
            if not obj:
                continue
            # Get the language of the obj
            if type(obj) == str:
                # language tag example: @en
                obj, language = cut_language_from_string(obj)
            # Change object type for certain predicates

            predicate_uri, obj_uri = self.convert_pred_and_obj(predicate,
                                                               obj,
                                                               language = language,
                                                               extractor = extractor)

            # Add to the graph
            self.graph.add((subj_uri, predicate_uri, obj_uri))

        if extractor:
            source_uri = self.OM.OBS[standardize_uri(extractor.URI)]
            self.graph.add((subj_uri, self.OM.OBS["source"], source_uri))


if __name__ == "__main__":
    pass
