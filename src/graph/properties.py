"""
Define attribute mapping and conversion methods
from string to URI with types.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
from typing import Any
from rdflib import Namespace, URIRef
from rdflib.namespace import RDF, OWL, XSD, DCAT, DCTERMS, SKOS, FOAF, SDO


class Properties():
    """
    Metadata mapping between Ontology standards and the extracted dictionaries.
    Use this class for quick dictionary to RDF conversion.
    Stores the Observation Facilities namespace in the class variable OBS.
    Also stores other namespaces.
    """

    _instances = {}

    def __new__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__new__(cls)
            cls._instances[cls] = instance
        return cls._instances[cls]


    _OBS = Namespace("https://voparis-ns.obspm.fr/rdf/obsfacilities#")
    _GEO = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
    _WB = Namespace("http://www.ivoa.net/rdf/messenger#")
    _IVOASEM = Namespace("http://www.ivoa.net/rdf/ivoasem#")


    # Mapping from dictionary keys to ontology properties.
    # Properties that are not mapped belong to the OBS namespace.
    _MAPPING = {
        "code": {"pred": SKOS.notation,
                 "objtype": XSD.string}, # for non-ontological external resources
        "uri": {"pred": OWL.sameAs,
                "objtype": XSD.string}, # for ontological external resources
        "exact_match": {"pred": SKOS.exactMatch,
                        "objtype": URIRef}, # for internal resources that are the same.
        "url": {"pred": SDO.url,
                "objtype": XSD.string}, # facility-list, PDS, SPASE
        "ext_ref": {"pred": FOAF.page,
                    "objtype": XSD.string}, #SDO.about, # RDFS.seeAlso, # for corpus pages (wikipedia etc)
        "type": {"pred": RDF.type,
                 "objtype": URIRef},
        "source": {"pred": _OBS.source,
                   "objtype": URIRef},
        "is_authoritative_for": {"pred": _OBS.isAuthoritativeFor,
                                 "objtype": URIRef},
        "label": {"pred": SKOS.prefLabel,
                  "objtype": XSD.string},
        "alt_label": {"pred": SKOS.altLabel,
                      "objtype": XSD.string},
        "description": {"pred": DCTERMS.description,
                       "objtype": XSD.string},
        "definition": {"pred": SKOS.definition,
                       "objtype": XSD.string},
        "is_part_of": {"pred": DCTERMS.isPartOf,
                       "objtype": URIRef}, # PDS
        "has_part": {"pred": DCTERMS.hasPart,
                     "objtype": URIRef}, # PDS
        "community": {"pred": _OBS.community,
                      "objtype": URIRef},
        "waveband": {"pred": _OBS.waveband,
                     "objtype": URIRef}, # AAS. Reference to IVOA's Messengers
        "location": {"pred": _GEO.location,
                     "objtype": XSD.string}, # AAS, IAU-MPC, SPASE
        "address": {"pred": SDO.address,
                    "objtype": XSD.string}, # PDS
        "city": {"pred": SDO.addressLocality,
                  "objtype": XSD.string}, #IAU-MPC
        "country": {"pred": SDO.addressCountry,
                    "objtype": XSD.string},# PDS
        "state": {"pred": SDO.State,
                  "objtype": XSD.string},
        "continent": {"pred": SDO.Continent,
                      "objtype": XSD.string},
        "latitude": {"pred": _GEO.latitude,
                     "objtype": XSD.float}, # IAU-MPC, SPASE
        "longitude": {"pred": _GEO.longitude,
                      "objtype": XSD.float}, # IAU-MPC, SPASE
        "altitude": {"pred": _GEO.altitude,
                      "objtype": XSD.float}, # SPASE
        "start_date": {"pred": DCAT.startDate,
                       "objtype": XSD.dateTime}, # SPASE
        "end_date": {"pred": DCAT.endDate,
                     "objtype": XSD.dateTime},
        "stop_date": {"pred": DCAT.endDate,
                      "objtype": XSD.dateTime},
        "launch_date": {"pred": _OBS.launch_date,
                        "objtype": XSD.dateTime}, # Wikidata
        "location_confidence": {"pred": _OBS.location_confidence,
                                "objtype": XSD.float},
        "type_confidence": {"pred": _OBS.type_confidence,
                            "objtype": XSD.float},
        "modified": {"pred": DCTERMS.modified,
                     "objtype": XSD.dateTime},
        "deprecated": {"pred": _IVOASEM.Deprecated,
                       "objtype": XSD.string}
    }


    # To get a simplified attribute name (for string representation purposes)
    _INVERSE_MAPPING = {value["pred"]: key for (key, value) in _MAPPING.items()}


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


    _STRING_REPR = [
        "label",
        "alt_label",
        "definition",
        "description",
        "country",
        "continent",
    ]


    _IDENTIFIERS = [
        "NAIF_ID",
        "MCP_ID",
        "COSPAR_ID",
        "NSSDC_ID",
        "NORAD_ID",
        "code",
    ]


    _LINKS = [
        "url",
        "uri",
        "ext_ref"
    ]


    _METADATA = [
        "modified",
        "deprecated",
        "location_confidence",
        "type_confidence"
    ]


    def __init__(self):
        pass


    @property
    def graph(self):
        raise ValueError()


    @property
    def OBS(self):
        return Properties._OBS


    @property
    def GEO(self):
        return Properties._GEO


    @property
    def WB(self):
        return Properties._WB


    @property
    def IVOASEM(self):
        return Properties._IVOASEM


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
            Convert an attribute to a URIRef predicate accordingly to the mapping.
            For example, if attribute is "definition", returns SKOS.definition.

            Args:
                attr: the attribute to convert to a URIRef.
            """
            if type(attr) == str:
                attr = Properties._MAPPING.get(
                    attr,
                    self.OBS[attr])
                if type(attr) == dict:
                    attr = attr["pred"]
                return attr
            else:
                return attr


    def __getattr__(
            self,
            attr: str) -> URIRef:
        return self.convert_attr(attr)


    def get_attr_name(
            self,
            attr: URIRef) -> str:
        """
        Get the attribute name (use for str representation purposes)

        Args:
            attr: URIRef attribute (predicate)
        """
        res = self._INVERSE_MAPPING.get(attr, None)
        if res is None:
            # Not in the mapping. Probably an OBS.
            res = attr[attr.rfind('#') + 1:]
        return res

    def get_type(
            self,
            attr: str | URIRef) -> Any:
        """
        Get the XSD.datatype that the property's object should have.

        Args:
            attr: the str or URIRef of the property
        """
        if type(attr) == URIRef:
            attr = self.get_attr_name(attr)
        if attr in self._MAPPING:
            return self._MAPPING[attr].get("objtype", None)
        else:
            return None