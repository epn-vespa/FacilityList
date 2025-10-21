"""
Singleton class of the SSSOM mapping graph, which is saved
in a distinct file from the output ontology.

Formerly adding SynonymPair entities to the output ontology,
created from a validated CandidatePair.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
from __version__ import __version__
import uuid
from typing import List
from rdflib import Graph, URIRef, Namespace, Node, Literal, XSD, RDFS, RDF
from pathlib import Path
from graph.properties import Properties
from config import USERNAME

from datetime import datetime


class MappingGraph():

    _OBS = Namespace("https://voparis-ns.obspm.fr/rdf/obsfacilities#")

    _SSSOM = Namespace("https://w3id.org/sssom/")

    _SEMAPV = Namespace("https://w3id.org/semapv/vocab/")


    # Score names' mapping types (subclasses of Mapping) (https://www.ebi.ac.uk/ols4/search?)
    MAPPING_PROCESSESSES = {
        "string_match": _SEMAPV.StringMatch,
        "label_match": _SEMAPV.StringMatch,
        "levenshtein_similarity": _SEMAPV.LevenshteinEditDistance,
        "acronym_probability": _SEMAPV.StringBasedSimilarityMeasure,
        "tfidf_cosine_similarity": _SEMAPV.LexicalSimilarityThresholdMatching,
        "llm_similarity": _SEMAPV.LexicalSimilarityThresholdMatching,
    }

    # Singleton graph
    _graph = None
    _initialized = False

    def __new__(cls,
                filename: str = None,
                strategy: str = ""):
        """
        Instanciate the graph singleton.
        """
        if cls._graph is None:
            cls._graph = super(MappingGraph, cls).__new__(cls)
        return cls._graph


    def __init__(self,
                 filename: str = None,
                 strategy: str = ""):
        if MappingGraph._initialized:
            return
        self._graph = Graph()
        if filename:
            self._graph.parse(filename)

        self._graph.bind("obs", self._OBS)
        self._graph.bind("sssom", self._SSSOM)
        self._graph.bind("semapv", self._SEMAPV)
        self.initialize_mapping_set(author = USERNAME,
                                    strategy = strategy)
        MappingGraph._initialized = True


    def bind_namespace(self,
                       namespace: str) -> Namespace:
        """
        Bind the namespace for a source to the graph if it is not binded yet.

        Args:
            namespace: corresponds to the source list's namespace (ex. aas)
        """
        namespace_uri = Namespace(str(self._OBS)[:-1] + "/" + namespace + "#")

        # Bind namespace if not binded yet (override = False)
        self._graph.bind(namespace, namespace_uri, override = False)


    def initialize_mapping_set(self,
                               author: str,
                               strategy: str):
        """
        Initialize a mapping set in the graph.

        https://mapping-commons.github.io/sssom/MappingSet/
        Those information could be added:
        time elapsed, input namespaces, type of entities, list of annotators (human or AI)...
        Also the mapping set could be initialized for every line of a strategy.
        """
        self._mapping_set = self._OBS[str(uuid.uuid4())]
        self._graph.add((self._mapping_set, RDF.type, self._SSSOM.MappingSet))
        self._graph.add((self._mapping_set, self._SSSOM.reviewer_label, Literal(author, datatype=XSD.string)))
        self._graph.add((self._mapping_set, self._SSSOM.mapping_set_description, Literal(strategy, datatype=XSD.string)))
        #self._graph.add((self._mapping_set, self._SSSOM.mapping_set_id, Literal(str(self._mapping_set).split('#')[-1], datatype=XSD.string)))
        self._graph.add((self._mapping_set, self._SSSOM.mapping_date, Literal(datetime.now(), datatype=XSD.dateTimeStamp)))
        self._graph.add((self._mapping_set, self._SSSOM.mapping_tool, Literal("https://doi.org/10.5281/zenodo.17199128", datatype=XSD.anyURI)))
        self._graph.add((self._mapping_set, self._SSSOM.mapping_tool_version, Literal(__version__, datatype=XSD.string)))


    def add_mapping(self,
                    entity1: URIRef,
                    entity2: URIRef,
                    entity1_source: URIRef,
                    entity2_source: URIRef,
                    score_value: float,
                    score_name: str,
                    scores: dict = None,
                    filters: list = None,
                    justification_string: str = "",
                    is_human_validation: bool = False,
                    no_validation: bool = False,
                    validator_name: str = "",
                    predicate: Node = Properties().exact_match,
                    subject_match_field: List[URIRef] | List[str] | URIRef | str = None,
                    object_match_field: List[URIRef] | List[str] | URIRef | str = None,
                    match_string: str = None,
                    ) -> None:
        """
        Add a mapping in the mapping's graph.

        Args:
            entity1: URIRef of the first entity.
            entity2: URIRef of the second entity.
            entity1_source: URIRef of the source of entity1.
            entity2_source: URIRef of the source of entity2.
            score_value: decisive score's value. 1.0 for discriminant criteria.
            score_name: decisive score's label. Will be used to save the similarity_measure.
            scores: score dict from the Candidate Pair {score_name: score_value}.
            filters: list of filters that were applied to the entity pair.
            justification_string: written by reviewer or by a validation tool.
            is_human_validation: whether it was validated by a reviewer.
            no_validation: if the candidate pair were not reviewed.
            validator_name: the name of the validator.
            predicate: relation added between the two entities.
            subject_match_field: attribute(s) of entity1 that were matched if called by attribute_merger.
            object_match_field: attribute(s) of entity2 that were matched if called by attribute_merger.
            match_string: the string that was matched between the two entities if called by attribute_merger.
        """
        assert type(entity1) is URIRef
        assert type(entity2) is URIRef
        assert type(entity1_source) is URIRef
        assert type(entity2_source) is URIRef
        self.bind_namespace(entity1.n3().rsplit('#', 1)[0] + '#')
        self.bind_namespace(entity2.n3().rsplit('#', 1)[0] + '#')
        if not score_value and scores and score_name in scores:
            decisive_score_value = scores[score_name]
        else:
            decisive_score_value = None
        if is_human_validation:
            justification_source = self._SEMAPV.ManualMappingCuration
        elif no_validation:
            justification_source = None
        else:
            justification_source = self._SEMAPV.LexicalMatching

        mapping_uri = self._OBS[str(uuid.uuid4())]

        self._graph.add((mapping_uri, RDF.type, self._SSSOM.Mapping))
        self._graph.add((mapping_uri, self._SSSOM.mapping_set_id, self._mapping_set))
        self._graph.add((mapping_uri, self._SSSOM.predicate_id, predicate))
        self._graph.add((mapping_uri, self._SSSOM.object_id,  entity1))
        self._graph.add((mapping_uri, self._SSSOM.subject_id, entity2))
        self._graph.add((mapping_uri, self._SSSOM.mapping_date, Literal(datetime.now(), datatype=XSD.dateTimeStamp)))
        if score_value:
            self._graph.add((mapping_uri, self._SSSOM.similarity_score, Literal(score_value, datatype=XSD.float)))
        elif decisive_score_value:
            self._graph.add((mapping_uri, self._SSSOM.similarity_score, Literal(decisive_score_value, datatype=XSD.float)))
        if score_name:
            similarity_measures = self.MAPPING_PROCESSESSES.get(score_name, Literal(score_name, datatype=XSD.string))
            if type(similarity_measures) is list:
                for similarity_measure in similarity_measures:
                    self._graph.add((mapping_uri, self._SSSOM.similarity_measure, similarity_measure))
            else:
                self._graph.add((mapping_uri, self._SSSOM.similarity_measure, similarity_measures))
        if not no_validation:
            self._graph.add((mapping_uri, self._SSSOM.justification, justification_source))
            if validator_name:
                self._graph.add((mapping_uri, self._SSSOM.reviewer_label, Literal(validator_name, datatype=XSD.string)))
            if justification_string:
                self._graph.add((mapping_uri, RDFS.comment, Literal(justification_string, datatype=XSD.string)))
        if scores:
            for score_name, score_value in scores.items():
                if score_value < 0:
                    continue # Discriminant scores that did not eliminate the candidate pair
                self._graph.add((mapping_uri, self._OBS[score_name], Literal(score_value, datatype=XSD.float)))
                score_type = self.MAPPING_PROCESSESSES.get(score_name, None)
                if score_type:
                    self._graph.add((mapping_uri, self._SSSOM.curation_rule, score_type))
        if subject_match_field:
            if type(subject_match_field) is not list:
                subject_match_field = [subject_match_field]
            for field in subject_match_field:
                if type(field) is URIRef:
                    self._graph.add((mapping_uri, self._SSSOM.subject_match_field, field))
                else:
                    self._graph.add((mapping_uri, self._SSSOM.subject_match_field, Properties().convert_attr(field)))
        if object_match_field:
            if type(object_match_field) is not list:
                object_match_field = [object_match_field]
            for field in object_match_field:
                if not field:
                    continue
                if type(field) is URIRef:
                    self._graph.add((mapping_uri, self._SSSOM.object_match_field, field))
                else:
                    self._graph.add((mapping_uri, self._SSSOM.object_match_field, Properties().convert_attr(field)))
        if match_string:
            self._graph.add((mapping_uri, self._SSSOM.match_string, Literal(match_string, datatype=XSD.string)))
        self._graph.add((mapping_uri, self._SSSOM.subject_source, entity1_source))
        self._graph.add((mapping_uri, self._SSSOM.object_source, entity2_source))
        # TODO filters


    def serialize(self,
                  output_dir: str,
                  execution_id: str):
        """
        Save the ontology into the execution folder and name it mapping.ttl.

        Args:
            output_dir: the output directory for this execution.
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / "mapping.ttl"
        self._graph.serialize(destination = path, format="ttl")
