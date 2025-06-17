"""
Graph class that is used in the packages update & merge.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
from collections import defaultdict
import datetime
import os

from typing import Iterator, List, Tuple, Union
from rdflib import RDFS, Graph as G, Literal, Namespace, URIRef, XSD
from rdflib.namespace import RDF, SKOS, DCTERMS, OWL, SDO, DCAT, FOAF
from data_updater import entity_types
from data_updater.extractor.extractor import Extractor
from utils.performances import deprecated
from utils.utils import standardize_uri, cut_acronyms, get_datetime_from_iso, cut_language_from_string
from config import USERNAME # type: ignore


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
        "label": {"pred": SKOS.prefLabel,
                  "objtype": XSD.string},
        "description": {"pred": DCTERMS.description,
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
        "city": {"pred": SDO.addressLocality,
                  "objtype": XSD.string}, #IAU-MPC
        "country": {"pred": SDO.addressCountry,
                    "objtype": XSD.string},# PDS
        "continent": {"pred": SDO.Continent,
                      "objtype": XSD.string},
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
                        "objtype": XSD.dateTime}, # Wikidata
        "location_confidence": {"pred": _OBS.location_confidence,
                                "objtype": XSD.float},
        "type_confidence": {"pred": _OBS.type_confidence,
                            "objtype": XSD.float},
        "modified": {"pred": DCTERMS.modified,
                     "objtype": XSD.dateTime},
        "deprecated": {"pred": _IVOASEM.Deprecated,
                       "objtype": XSD.boolean},
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


    def __init__(self):
        pass


    @property
    def graph(self):
        raise ValueError()


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
    def IVOASEM(self):
        return OntologyMapping._IVOASEM


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
            attr: str) -> URIRef:
        return self.convert_attr(attr)
        #return getattr(self.graph, attr)
        #return getattr(Graph._graph, attr)


    def get_attr_name(
            self,
            attr: URIRef) -> str:
        """
        Get the attribute name (use for str representation purposes)

        Keyword arguments:
        attr -- URIRef attribute (predicate)
        """
        res = self._INVERSE_MAPPING.get(attr, None)
        if res is None:
            # Not in the mapping. Probably an OBS.
            res = attr[attr.rfind('#') + 1:]
        return res


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

    # Singleton graph
    # TODO: one graph per (list to fasten SparQL)
    _GRAPH = None
    _initialized = False

    def __new__(cls,
                filename: Union[str,list[str]] = ""):
        """
        Instanciate the graph singleton.
        """
        if cls._GRAPH is None:
            cls._GRAPH = super(Graph, cls).__new__(cls)
        return cls._GRAPH


    def __init__(self,
                 filename: Union[str, list[str]] = ""):
        """
        Instanciate the graph singleton.

        Keyword arguments:
        filename -- the input ontology's filename or filenames for a merge.
        """
        if Graph._initialized:
            return

        self._graph = G() # instanciate rdflib.Graph
        if filename:
            self.parse(filename)

        Graph._OM = OntologyMapping()

        # Bind namespaces
        self.bind("obs", self.OM.OBS)
        self.bind("geo1", self.OM.GEO)
        self.bind("wb", self.OM.WB)
        self.bind("ivoasem", self.OM.IVOASEM)

        # Initialize types
        obs_facility = standardize_uri(entity_types.OBSERVATION_FACILITY)
        for t in entity_types.ALL_TYPES - {entity_types.OBSERVATION_FACILITY}:
            t = standardize_uri(t)
            self.add((self.OBS[t],
                      RDFS.subClassOf,
                      self.OBS[obs_facility]))

        Graph._initialized = True


    @property
    def graph(self):
        return self._graph


    def parse(self,
              filename: Union[str, list[str]]):
        """
        Overrides rdflib.Graph's parse to extract the namespaces and
        save them in this object.
        Can parse one or more files.
        """
        if isinstance(filename, str):
            filenames = [filename]
        else:
            filenames = filename
        input_ontology_namespaces = []
        for filename in filenames:
            if os.path.exists(filename):
                self.graph.parse(filename)
                for prefix, namespace in self.graph.namespaces():
                    if prefix in Extractor.AVAILABLE_NAMESPACES:
                        input_ontology_namespaces.append(prefix)
            else:
                raise FileNotFoundError(f"File {filename} does not exist.")

        self._available_namespaces = input_ontology_namespaces


    def __getattr__(self, attr):
        if hasattr(self.graph, attr):
            return getattr(self.graph, attr)
        return None
        # return getattr(self.graph, attr)


    @property
    def OM(self):
        return Graph._OM


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
    def IVOASEM(self):
        return OntologyMapping._IVOASEM

    ##### Methods for merge #####


    def is_available(self,
                     namespace: str) -> bool:
        """
        Returns true if the namespace of a list is available in the graph.

        Keyword arguments:
        namespace -- the namespace to test
        """
        return namespace in self._available_namespaces


    def get_entities_from_list(self,
                               source: Extractor,
                               ent_type: Union[str, list[str]] = None,
                               no_equivalent_in: Extractor = None,
                               has_attr: list[str] = [],
                               limit: int = -1
                               ) -> Iterator[Tuple[URIRef]]:
        """
        Get all the entities that come from a list.
        If an entity is already in a synset, it will return the synset
        too.

        Keyword arguments:
        source -- the source extractor to get entities from
        no_equivalent_in -- the entities from source are not linked with
                        skos:exactMatch to any entity from this list,
                        and is not a member of a synonym set with an entity
                        of the other source.
        has_attr -- only return entities that have has_attr as a relation.
        limit -- limits the amout of results. -1 to get the whole list
        """

        if isinstance(source, Extractor):
            source = source.URI
            source = standardize_uri(source)
        else:
            raise TypeError(f"'source' ({source}) should be an Extractor. " +
                            f"Got {type(source)}.")
        has_attr_str = ""
        if has_attr:
            for attr in has_attr:
                attr = self.OM.convert_attr(attr)
                has_attr_str += f"\n?entity <{attr}> ?v ."

        ent_type_str = ""
        if ent_type:
            if type(ent_type) == str:
                ent_type_str += f"?entity a obs:{standardize_uri(ent_type)} ."
            elif type(ent_type) == list:
                ent_type_list = []
                for et in ent_type:
                    ent_type_list += f"{{ ?entity a obs:{et} . }}\n"
                ent_type_str += "\n UNION ".join(ent_type_list)

        if not no_equivalent_in:
            query = f"""
            SELECT ?entity ?synset
            WHERE {{
                {ent_type_str}
                ?entity obs:source obs:{source} .{has_attr_str}
                OPTIONAL {{
                    ?synset obs:hasMember ?entity .
                }}
            }}
            """
        else:
            no_equivalent_in = standardize_uri(no_equivalent_in.URI)
            query = f"""
            SELECT ?entity ?synset
            WHERE {{
                {ent_type_str}
                ?entity obs:source obs:{source} .{has_attr_str}
                OPTIONAL {{
                    ?synset obs:hasMember ?entity .
                }}
                FILTER NOT EXISTS {{
                    ?entity2 skos:exactMatch ?entity .
                    ?entity2 obs:source obs:{no_equivalent_in} .
                }}

            }}
            """
        if limit >= 0:
            query += f" LIMIT {limit}"
        return self.query(query)


    def count_synonym_sets(self) -> int:
        query = f"""SELECT ?synset WHERE {{
            ?synset a obs:SynonymSet .
        }}"""
        resp = self.query(query)
        return len(resp)



    def get_synsets(self) -> Iterator[URIRef]:
        """
        Get all synsets (used to initialize SynonymSets)
        """
        for synset_uri, _, _ in self.triples((None, RDF.type, self.OM.OBS["SynonymSet"])):
            yield synset_uri


    def get_members(self,
                    synset_uri: URIRef) -> Iterator[URIRef]:
        """
        Get members of a synonym set using the synonym set's URI.
        """
        for _, _, syn in self.triples((synset_uri, self.OM.convert_attr("hasMember"), None)):
            yield syn


    @deprecated
    def get_candidate_pair_uri(self,
                               member1: URIRef,
                               member2: URIRef) -> URIRef:
        """
        Get a candidate pair's URI from its two members that can be
        the uri of Entity or SynonymSet. Use this method to prevent
        duplicating a Candidate Pair if one was created in another run.

        Keyword arguments:
        member1 -- the first member of the pair (entity or synset URIRef)
        member2 -- the second member of the pair (entity or synset URIRef)
        """
        query = f"""
        SELECT ?candidate_pair
        WHERE {{
            ?candidate_pair a obs:CandidatePair .

            # Member 1
            {{
                ?candidate_pair obs:hasMember <{member1}> .
            }} UNION {{
                ?someSynonymSet1 a obs:SynonymSet .
                ?candidate_pair obs:hasMember ?someSynonymSet1 .
                ?someSynonymSet1 obs:hasMember <{member1}> .
            }} UNION {{
                <{member1}> a obs:SynonymSet .
                <{member1}> obs:hasMember ?member1 .
                ?candidate_pair obs:hasMember ?member1 .
            }}

            # Member 2
            {{
                ?candidate_pair obs:hasMember <{member2}> .
            }} UNION {{
                ?someSynonymSet2 a obs:SynonymSet .
                ?candidate_pair obs:hasMember ?someSynonymSet2 .
                ?someSynonymSet2 obs:hasMember <{member2}> .
            }} UNION {{
                <{member2}> a obs:SynonymSet .
                <{member2}> obs:hasMember ?member2 .
                ?candidate_pair obs:hasMember ?member2 .
            }}
        }}
        """
        return self.query(query)


    def get_graph_semantic_fields(self) -> List[str]:
        """
        Return all the descriptions in the graph. Use this to generate
        a corpus for statistical computations such as TfIdf.
        Returns definitions, descriptions & labels to string.
        """
        descr_by_entities = defaultdict(str)
        for entity, rel, desc in self.triples((None,
                                               None,# self.OBS["description"],
                                               None)):
            if rel in [self.OM.convert_attr("description"),
                       self.OM.convert_attr("definition"),
                       self.OM.convert_attr("label")]:
                descr_by_entities[entity] += " " + desc
        return list(descr_by_entities.values())

    ###### Methods for update #######

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
        if extractor and pred != "type":
            namespace_obj = self.get_namespace(extractor.NAMESPACE)
        else:
            # Non-extracted data & OBS types.
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
        elif namespace == "IVOASEM":
            return self.IVOASEM
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
            if obj is None or obj == "":
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


    def add_metadata(self,
                     description: str,
                     date: str = datetime.date.today().isoformat(),
                     author: str = USERNAME
                     ):
        """
        Add description, date and author to the ontology.
        The description is to precise whether the ontology was created with the
        update script (declare which lists were used) or the merge script
        (describe the merging strategy).

        Keyword arguments:
        description -- how the ontology is created
        date -- last modification date
        author -- name of the user creating this ontology
        """
        self.graph.add((OWL.Ontology, DCTERMS.description, Literal(description)))
        self.graph.add((OWL.Ontology, DCTERMS.modified, Literal(date, datatype = XSD.date)))
        self.graph.add((OWL.Ontology, DCTERMS.creator, Literal(author)))


if __name__ == "__main__":
    pass
