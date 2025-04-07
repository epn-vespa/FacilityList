"""
Represent and manage entity objects.
Has methods to get an entity's labels & other attributes from the graph.
The Entity can be used in a CandidatePair object or a SynonymSet object.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from collections import defaultdict
from typing import List, Set
from rdflib import URIRef

from data_merger.graph import Graph
from data_updater.graph import OntologyMapping


class Entity():

    # Save entities' uri to prevent multi instanciation
    entities = dict()


    def __new__(cls,
                uri: URIRef):
        # cls._uri = uri
        if uri in cls.entities:
            return cls.entities[uri]
        else:
            instance = super().__new__(cls)
            cls.entities[uri] = instance
            return instance


    def __init__(self,
                 uri: URIRef):
        self._uri = uri
        self._data = defaultdict(set)
        graph = Graph._graph
        for entity, property, value in graph.triples((self.uri, None, None)):
            self._data[property].add(value)


    def __reduce__(self):
        # When the object is pickled, we return the class and the necessary arguments
        return (self.__class__, (self.uri,))


    @property
    def uri(self):
        return self._uri


    def __str__(self):
        return str(self.uri)


    def __repr__(self):
        return str(self.uri)


    @property
    def data(self) -> dict:
        """
        Get the data dictionary for this entity.
        {property1: [value1, value2]}
        """
        return self._data


    def get_values_for(self,
                       property: str) -> Set:
        """
        Get values of the entity for a property.

        Keyword arguments:
        property -- the property name (ex: "label")
        """
        property = Graph._graph.OM.convert_attr(property)
        if property in self._data:
            return self._data[property]
        else:
            # No value for this property
            return set()


if __name__ == "__main__":
    pass