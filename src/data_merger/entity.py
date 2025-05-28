"""
Represent and manage entity objects.
Has methods to get an entity's labels & other attributes from the graph.
The Entity can be used in a CandidatePair object or a SynonymSet object.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from collections import defaultdict
from typing import Set, Union
from rdflib import Literal, URIRef

from graph import Graph
from utils.utils import cut_language_from_string

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
                if not value.language and type(value.value) == str:
                    # check in the string as some languages may have '-' but
                    # languages with '-' are not returned by rdflib's Literal
                    value_str, lang = cut_language_from_string(value.value)
                    if lang:
                        self._data[property].add((value_str, lang))
                        continue
                self._data[property].add((value.value, value.language))
            else:
                self._data[property].add((str(value), None))


    def __eq__(self, entity: Union[Entity, URIRef]):
        if type(entity) == URIRef:
            return self.uri == entity
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
        return f"Entity@{self.uri}"


    @property
    def data(self) -> dict:
        """
        Get the data dictionary for this entity.
        {property1: [value1, value2]}
        """
        return self._data


    def get_values_for(self,
                       property: str,
                       unique: bool = False,
                       language: str = None) -> Set:
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
            if type(res) == set:
                for value, lang in res:
                    if lang == None or language == None or lang == language:
                        return value
            elif type(res) == tuple:
                value, lang = res
                if lang == None or language == None or lang == language:
                    return value
            return None
        res_for_lang = set()
        for value, lang in res:
            if lang == None or language == None or lang == language:
                res_for_lang.add(value)
        return res_for_lang


    def to_string(self,
                  exclude: list[str] = ["code",
                                        "url"],
                  limit: int = 512) -> str:
        """
        Convert an entity's data dict into its string representation.
        Keys are sorted so that the generated string is always the same.

        Exclude entries from the data to ignore values that will not help LLM,
        such as any URL/URI, codes, etc.

        Keyword arguments:
        data -- the entity data dict
        exclude -- dict entries to exclude
        limit -- maximum string length for each attribute of the entity.
                 -1 for no limit.
        """
        res = ""
        label = self.get_values_for("label")
        if label:
            if type(label) == set:
                res = ", ".join(label)
            else:
                res = label + '. '
        for key, value in sorted(self.data.items()):
            key = Graph().OM.get_attr_name(key)
            if key in exclude:
                continue
            if key == "label":
                continue
            if key == "alt_label":
                key = "Also known as"
                # Only keep ten alt labels
                #res += f" {key}: {', '.join(sorted(value, key = lambda x: 1/len(x))[:10])}"
                #continue
            if type(value) not in [list, set, tuple]:
                value = [value]
            else:
                key = key.replace('_', ' ').capitalize()
            res += f" {key}: {', '.join([str(v) for v in value])[:limit]}."
        return res


if __name__ == "__main__":
    pass
