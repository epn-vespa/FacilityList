"""
Represent and manage entity objects.
Has methods to get an entity's labels & other attributes from the graph.
The Entity can be used in a CandidatePair object or a SynonymSet object.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from collections import defaultdict
from typing import Set
from rdflib import Literal, URIRef

from graph import Graph

class Entity:
    pass


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
        graph = Graph()
        for entity, property, value in graph.triples((self.uri, None, None)):
            if isinstance(value, Literal):
                self._data[property].add(value.value)
            else:
                self._data[property].add(str(value))


    def __repr__(self):
        return f"Entity@{self.uri}"


    def __eq__(self, entity: Entity):
        return self.uri == entity.uri


    def __hash__(self):
        return hash(self.uri)


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
                       property: str,
                       unique: bool = False) -> Set:
        """
        Get values of the entity for a property.

        Keyword arguments:
        property -- the property name (ex: "label")
        unique -- if there was more than one values,
                  return only the first non-None value.
        """
        property = Graph().OM.convert_attr(property)
        if property in self._data:
            res = self._data[property]
        else:
            # No value for this property
            res = set()
        if unique:
            if type(res) in [set, list, tuple]:
                if len(res):
                    return list(res)[0]
                else:
                    return None
            elif res:
                return res
            else:
                return None
        return res

if __name__ == "__main__":
    pass
