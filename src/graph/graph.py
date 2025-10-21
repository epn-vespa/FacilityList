"""
Graph class that is used in the packages update & merge.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
from collections import defaultdict
import datetime
import os

from typing import Iterator, List, Tuple
from rdflib import RDFS, Graph as G, Literal, Namespace, URIRef, XSD
from rdflib.namespace import SKOS, DCTERMS, OWL
from graph import entity_types
from graph.extractor.extractor_lists import ExtractorLists
from graph.extractor.extractor import Extractor
from graph.properties import Properties
from utils.string_utilities import standardize_uri, cut_acronyms, get_datetime_from_iso, cut_language_from_string
from config import USERNAME


class Graph(G):

    """
    Instanciate a rdflib Graph as a singleton to prevent multiple
    instantiation. Replace rdflib.Graph and reuses rdflib.Graph's attributes.
    This overrides graph's Graph to add other functions.

    Usage:
        from graph import Graph
        g = Graph() # instanciate
        obs = g.OBS # get Observation Facility namespace

        # Then, use g as a rdflib graph object:
        g.add((g.OBS["subj"], URIRef("predicate"), URIRef("obj")))
    """

    # Singleton graph
    _GRAPH = None
    _initialized = False

    def __new__(cls,
                filename: str | list[str] = ""):
        """
        Instanciate the graph singleton.
        """
        if cls._GRAPH is None:
            cls._GRAPH = super(Graph, cls).__new__(cls)
        return cls._GRAPH


    def __init__(self,
                 filename: str | list[str] = ""):
        """
        Instanciate the graph singleton.

        Args:
            filename: the input ontology's filename or filenames for a merge.
        """
        if Graph._initialized:
            return

        self._graph = G() # instanciate rdflib.Graph
        if filename:
            self.parse(filename)

        Graph._PROPERTIES = Properties()

        # Bind namespaces
        self.bind("obsf", self.PROPERTIES.OBS)
        self.bind("geo1", self.PROPERTIES.GEO)
        self.bind("wb", self.PROPERTIES.WB)
        self.bind("ivoasem", self.PROPERTIES.IVOASEM)

        # Initialize types
        obs_facility = standardize_uri(entity_types.OBSERVATION_FACILITY)
        for t in entity_types.ALL_TYPES - {entity_types.OBSERVATION_FACILITY}:
            t = standardize_uri(t)
            self.add((self.OBS[t],
                      RDFS.subClassOf,
                      self.OBS[obs_facility]))

        Graph._initialized = True


    def parse(self,
              filename: str | list[str]):
        """
        Overrides rdflib.Graph's parse to extract the namespaces and
        save them in this object.
        Can parse one or more files.

        Args:
            filename: input ontologie(s) to parse.
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
                    if prefix in ExtractorLists.AVAILABLE_NAMESPACES:
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
    def graph(self):
        return self._graph


    @property
    def PROPERTIES(self):
        return Graph._PROPERTIES


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
    def available_namespaces(self):
        return self._available_namespaces


    ##### Methods for mapping #####
    def is_available(self,
                     namespace: str) -> bool:
        """
        Returns true if the namespace of a list is available in the graph.

        Args:
            namespace: the namespace to test
        """
        return namespace in self._available_namespaces


    def get_entities_from_list(self,
                               source: Extractor,
                               ent_type: str | list[str] | set[str] = None,
                               no_equivalent_in: Extractor | list[Extractor] = None,
                               has_attr: list[str] = [],
                               limit: int = -1,
                               ignore_deprecated: bool = True
                               ) -> Iterator[Tuple[URIRef]]:
        """
        Get all the entities that come from a list.
        If an entity is already in a synset, it will return the synset
        too.

        Args:
            source: the source extractor to get entities from
            no_equivalent_in: the entities from source are not linked with
                              skos:exactMatch to any entity from this list,
                              and is not a member of a synonym set with an entity
                              of the other source.
            has_attr: only return entities that have has_attr as a relation.
            limit: limits the amout of results. -1 to get the whole list
            ignore_deprecated: if True, only entities that are not deprecated
                             will be returned. Default True.
        """

        if isinstance(source, Extractor):
            source = source.URI
            source = standardize_uri(source)
        else:
            raise TypeError("source must be an Extractor instance.")
        has_attr_str = ""
        if has_attr:
            for attr in has_attr:
                attr = self.PROPERTIES.convert_attr(attr)
                has_attr_str += f"\n?entity <{attr}> ?v ."

        ent_type_str = ""
        if ent_type:
            if type(ent_type) == str:
                ent_type_str += f"?entity a obsf:{standardize_uri(ent_type)} ."
            elif type(ent_type) in [list, set, tuple]:
                ent_type_list = []
                for et in ent_type:
                    ent_type_list.append(f"{{ ?entity a obsf:{standardize_uri(et)} . }}\n")
                ent_type_str += "\n UNION ".join(ent_type_list)

        ignore_deprecated_str = ""
        if ignore_deprecated:
            ignore_deprecated_str = f"FILTER NOT EXISTS {{ ?entity <{self.PROPERTIES.deprecated}> ':__' }}"
        no_equivalent_in_str = ""
        if no_equivalent_in:
            if type(no_equivalent_in) not in [list, set, tuple]:
                no_equivalent_in = [no_equivalent_in]
            for extractor in no_equivalent_in:
                no_equivalent_in_str += f"""
                FILTER NOT EXISTS {{ ?entity <{self.PROPERTIES.exact_match}> ?entity2 .
                ?entity2 <{self.PROPERTIES.source}> obsf:{extractor.URI} .}}"""
        limit_str = ""
        if limit >= 0:
            limit_str = f" LIMIT {limit}"
        query = f"""
        SELECT ?entity
        WHERE {{
            {ent_type_str}
            ?entity <{self.PROPERTIES.source}> obsf:{source} .
            {has_attr_str}
            {no_equivalent_in_str}
            {ignore_deprecated_str}
            {limit_str}
        }}
        """
        return self.query(query)


    def get_graph_semantic_fields(self,
                                  language: str | list[str] = None
                                  ) -> List[str]:
        """
        Return all the descriptions in the graph. Use this to generate
        a corpus for statistical computations such as TfIdf.
        Returns definitions, descriptions & labels to string.
        """

        fields = []
        for string_repr in self.PROPERTIES._STRING_REPR:
            fields.append(self.PROPERTIES.convert_attr(string_repr))

        descr_by_entities = defaultdict(str)

        for entity, pred, obj in self.triples((None, None, None)):
            if pred in fields:
                if isinstance(obj, Literal):
                    if obj.language is None or not language or obj.language in language:
                        descr_by_entities[entity] += " " + str(obj)

        return list(descr_by_entities.values())

    ###### Methods for update #######

    def convert_pred_and_obj(
            self,
            subj_uri: URIRef,
            pred: str,
            obj: str,
            language: str,
            extractor: Extractor) -> Tuple:
        """
        Convert a predicate and an object accordingly to the mapping.
        For example, for "definition", predicate becomes SKOS.definition,
        and obj becomes Literal(obj, datatype = XSD.string, lang = language).

        Args:
            pred: the predicate to convert
            obj: the object to convert
            language: the language of the XSD.string of the object if any
            extractor: the extractor used to extract the data
        """
        if type(pred) != str:
            return pred, obj

        pred_objtype = Properties._MAPPING.get(pred, None)
        if pred_objtype is None:
            # The predicate is not mapped, use the OBS namespace
            # and create the predicate.
            pred_uri = self.PROPERTIES.OBS[pred]
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
            namespace_obj = self.PROPERTIES.OBS

        if pred == "label":
            self.get_label_and_save_alt_labels(subj_uri,
                                               obj,
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
                        namespace_obj = self.PROPERTIES.OBS
                elif pred == "waveband":
                    # waveband is in IVOA vocabulary so it has its own ns
                    namespace_obj = self.PROPERTIES.WB
            else:
                namespace_obj = self.PROPERTIES.OBS

            # standardize obj_uri
            obj_uri = standardize_uri(obj)
            obj_uri = namespace_obj[obj_uri]

        return pred_uri, obj_uri


    def get_namespace(self,
                      namespace: str) -> Namespace:
        """
        Get the namespace for a source and bind it to the graph
        if it is not binded yet.

        Args:
            namespace: corresponds to the source list's namespace (AAS, NAIF...)
        """
        if namespace == "OBS":
            return self.PROPERTIES.OBS
        elif namespace == "GEO":
            return self.PROPERTIES.GEO
        elif namespace == "WB":
            return self.WB # IVOA Messenger (WaveBand)
        elif namespace == "IVOASEM":
            return self.IVOASEM
        namespace_uri = Namespace(str(self.PROPERTIES.OBS)[:-1] + "/" + namespace + "#")
        # Bind namespace if not binded yet (override = False)
        self.graph.bind(namespace, namespace_uri, override = False)
        return namespace_uri


    def get_label_and_save_alt_labels(
            self,
            uri: URIRef,
            label: str,
            extractor: Extractor,
            language: str = None) -> str:
        """
        Returns the label with no alternate label nor acronym (short label).
        Add the alt labels to the ontology (acronym and full label) if there
        was a change.

        Do not add acronym if the acronym is for the location (or other entities in the label)

        Args:
            uri: the URI of the entity
            label: the label of the entity
            namespace: the namespace of the label
            language: the language of the label if any
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
        # short_label_uri = standardize_uri(short_label)

        if acronym and not acronym_of_location:
            self.graph.add((uri,
                            SKOS.altLabel,
                            Literal(acronym, lang = language)))
        # location's acronym may become the whole entity's acronym.
        if short_label != label:
            self.graph.add((uri,
                            SKOS.altLabel,
                            Literal(short_label, lang = language)))
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

        Args:
            params: a tuple (subj, predicate, obj)
                subj: the subject of the triple.
                predicate: the predicate of the triple.
                obj: the object of the triple.
            extractor: the extractor of the added triple (ex: AasExtractor)
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
            namespace_subj = self.PROPERTIES.OBS

        if type(subj) == str:
            # Convert subject to URI with _OBS
            subj_uri = namespace_subj[standardize_uri(subj)]
        elif type(subj) == URIRef:
            subj_uri = subj
        else:
            raise TypeError(f"Subject ({subj}) can only be a str or an URIRef. "
                            + f"Got a {type(subj)}.")

        if predicate is None:
            raise ValueError(f"Predicate can not be None.")

        if type(predicate) == str:
            predicate_uri = self.PROPERTIES.convert_attr(predicate)
        elif type(predicate) == URIRef:
            predicate_uri = predicate
        else:
            raise TypeError(f"Predicate ({predicate}) can only be a str or an URIRef. "
                            + f"Got a {type(predicate)}.")

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
            predicate_uri, obj_uri = self.convert_pred_and_obj(
                subj_uri,
                predicate,
                obj,
                language = language,
                extractor = extractor)

            # Add to the graph
            self.graph.add((subj_uri, predicate_uri, obj_uri))

        if extractor:
            source_uri = self.PROPERTIES.OBS[standardize_uri(extractor.URI)]
            self.graph.add((subj_uri, self.PROPERTIES.OBS["source"], source_uri))


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

        Args:
            description: how the ontology is created
            date: last modification date
            author: name of the user creating this ontology
        """
        self.graph.remove((OWL.Ontology, DCTERMS.description, None))
        self.graph.remove((OWL.Ontology, DCTERMS.modified, None))
        self.graph.add((OWL.Ontology, DCTERMS.description, Literal(description)))
        self.graph.add((OWL.Ontology, DCTERMS.modified, Literal(date, datatype = XSD.date)))
        for s, p, o in self.graph.triples((OWL.Ontology, DCTERMS.creator, None)):
            self.graph.add((OWL.Ontology, DCTERMS.contributor, o))
        self.graph.remove((OWL.Ontology, DCTERMS.creator, None))
        self.graph.add((OWL.Ontology, DCTERMS.creator, Literal(author)))



if __name__ == "__main__":
    pass
