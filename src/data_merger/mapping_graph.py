"""
Class where to save the mapping graph, which is saved
in a distinct file from the output ontology.

Formerly adding SynonymPair entities to the output ontology,
created from a validated CandidatePair.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
from rdflib import Graph, URIRef, Namespace, Node, Literal, XSD, SKOS, RDFS
from pathlib import Path

from datetime import datetime


class MappingGraph():

    _OBS = Namespace("https://voparis-ns.obspm.fr/rdf/obsfacilities#")

    _SSSOM = Namespace("https://w3id.org/sssom/")

    _SEMAPV = Namespace("https://w3id.org/semapv/vocab/")


    # Singleton graph
    _graph = None
    _initialized = False

    def __new__(cls):
        """
        Instanciate the graph singleton.
        """
        if cls._graph is None:
            cls._graph = super(MappingGraph, cls).__new__(cls)
        return cls._graph


    def __init__(self):
        if MappingGraph._initialized:
            return
        self._graph = Graph()

        self._graph.bind("obs", self._OBS)
        self._graph.bind("sssom", self._SSSOM)
        self._graph.bind("semapv", self._SEMAPV)
        MappingGraph._initialized = True


    def bind_namespace(self,
                       namespace: str) -> Namespace:
        """
        Bind the namespace for a source to the graph if it is not binded yet.

        Keyword arguments:
        namespace -- corresponds to the source list's namespace (AAS, NAIF...)
        """
        print("namespace=", namespace)
        namespace_uri = Namespace(str(self._OBS)[:-1] + "/" + namespace + "#")

        # Bind namespace if not binded yet (override = False)
        self._graph.bind(namespace, namespace_uri, override = False)


    def add_mapping(self,
                    mapping_uri: URIRef,
                    entity1: URIRef,
                    entity2: URIRef,
                    scores: dict,
                    decisive_score_name: str,
                    justification_string: str = "",
                    is_human_validation: bool = False,
                    predicate: Node = SKOS.exactMatch):
        """
        mapping_uri: the CandidatePair's uri.
        entity1: URIRef to the first Entity or SynonymSet.
        entity2: URIRef to the first Entity or SynonymSet.
        scores: score dict from the Candidate Pair {score_name: score_value}.
        decisive_score: score value used to take the decision. 1.0 for discriminant criteria.
        mapping_tool: what tool generated this mapping.
        justification_string: written by reviewer or by a validation tool.
        is_human_validation: whether it was validated by a reviewer.
        predicate: relation added between the two entities.
        """
        self.bind_namespace(entity1.n3)
        self.bind_namespace(entity2.n3)
        decisive_score = scores[decisive_score_name]
        if is_human_validation:
            justification_source = self._SEMAPV.ManualMappingCuration
        else:
            justification_source = self._SEMAPV.LexicalMatching

        self._graph.add((mapping_uri, self._SSSOM.predicate_id, predicate))
        self._graph.add((mapping_uri, self._SSSOM.object_id,  entity1))
        self._graph.add((mapping_uri, self._SSSOM.subject_id, entity2))
        self._graph.add((mapping_uri, self._SSSOM.justification, justification_source))
        self._graph.add((mapping_uri, self._SSSOM.similarity_score, Literal(decisive_score, datatype=XSD.float)))
        self._graph.add((mapping_uri, self._SSSOM.similarity_measure, Literal(decisive_score_name, datatype=XSD.string)))
        self._graph.add((mapping_uri, RDFS.comment, Literal(justification_string, datatype=XSD.string)))
        self._graph.add((mapping_uri, self._SSSOM.mapping_tool, Literal("FacilityList/merge.py", datatype=XSD.string)))
        self._graph.add((mapping_uri, self._SSSOM.mapping_date, Literal(datetime.now(), datatype=XSD.dateTimeStamp)))

        for score_name, score_value in scores.items():
            if score_value < 0:
                continue # Discriminant scores that did not eliminate the candidate pair
            self._graph.add((mapping_uri, self._OBS[score_name], Literal(score_value, datatype=XSD.float)))


    def serialize(self,
                  output_dir: str,
                  execution_id: str):
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / (execution_id + "_mapping.ttl")
        self._graph.serialize(destination = path, format="ttl")