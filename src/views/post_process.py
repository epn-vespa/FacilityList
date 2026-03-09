"""
Post-process a merged ontology (OntoPortal view).

- generate a definition with GPT (summarize entity or expand description
using search through papers to keep the description homogeneous in length)
- remove other definitions
- move attrs from platform to individual instruments (ex: waveband)
- remove some unwanted attrs
"""
from rdflib import RDF, URIRef
from graph.graph import Graph
from graph.entity import Entity
from graph.properties import Properties
from llm.llm_connection import LLMConnection
from config import SUMMARIZE_MODEL
properties = Properties()

class PostProcess():


    def __init__(self,
                 graph: Graph):
        self._graph = graph


    def __iter__(self):
        done = set()
        for uri, _, _ in self._graph.triples((None, RDF.type, None)):
            if uri in done:
                continue
            done.add(uri)
            yield uri


    def __call__(self):
        for uri in self.__iter__():
            self._remove_attrs(uri)
            self._gen_definition(uri)
            ...


    def _remove_attrs(self, uri: URIRef):
        """
        Remove unwanted attributes such as location details if the
        location confidence is not 1.
        """
        entity = Entity(uri)
        loc_conf = entity.get_values_for("location_confidence", unique = True)
        if loc_conf < 1:
            entity.remove_values("address")
            entity.remove_values("")
        


    def _gen_definition(self, uri: URIRef):
        """
        Generates an unique definition of the entity based
        on its attributes.

        Attrs:
            uri: the entity's URI
        """
        entity = Entity(uri)
        entity_str = entity.to_string(exclude = properties._LINKS + properties._EXT_REF + properties._METADATA)
        LLMConnection().generate("Generate a two-sentences resume of this entity: " + entity_str,
                                 model = SUMMARIZE_MODEL,
                                 num_predict = 100)
        