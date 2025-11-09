"""
Represent and manage entity objects.
Has methods to get an entity's labels & other attributes from the graph.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from collections import defaultdict
from typing import List, Set
from rdflib import Literal, URIRef
from rdflib.namespace import split_uri
from graph.mapping_graph import MappingGraph
from graph.extractor.extractor import Extractor

from graph.graph import Graph
from graph.properties import Properties
from utils.string_utilities import cut_language_from_string

class Entity:
    pass


class Entity():

    # Save entities' uri to prevent multi instanciation
    # {uri: Entity}
    entities = dict()

    def __new__(cls,
                uri: URIRef):
        if uri in cls.entities:
            return cls.entities[uri]
        else:
            instance = super().__new__(cls)
            cls.entities[uri] = instance
            return instance


    def __init__(self,
                 uri: URIRef):
        self._uri = URIRef(uri)
        self._data = defaultdict(set)
        graph = Graph()

        if not type(uri) is URIRef:
            raise TypeError(f"Expected URIRef, got {type(uri)}")
        for _, property, value in graph.triples((self.uri, None, None)):
            if isinstance(value, Literal):
                if not value.language and type(value.value) == str:
                    # check in the string as some languages may have '-' but
                    # languages with '-' are not returned by rdflib's Literal
                    value_str, lang = cut_language_from_string(value.value)
                    if lang:
                        self._data[property].add((value_str, lang))
                        continue
                self._data[property].add((value.value, value.language))
            elif isinstance(value, URIRef):
                # Entity
                self._data[property].add((value))
            else:
                self._data[property].add((str(value)))


    def __eq__(self,
               entity: Entity | URIRef) -> bool:
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


    def __getattr__(self, name):
        return self.get_values_for(name,
                                   unique = True)


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
                       languages: list[str] = None,
                       extend_to_synonyms: bool = True) -> Set:
        """
        Get values of the entity for a property.

        Args:
            property: the property name (ex: "label")
            unique: if there was more than one values,
                    return only the first non-None value.
            languages: only get fields labeled with those languages if known.
            extend_to_synonyms: will also get the entity's synonyms' features.
        """
        property = Graph().PROPERTIES.convert_attr(property)
        if property in self._data:
            res = self.data[property]
        else:
            # No value for this property
            res = set()

        if extend_to_synonyms and (not unique or not res):
            for syn in self.get_synonyms().copy():
                syn_values = Entity(syn).get_values_for(property,
                                                        unique = unique,
                                                        extend_to_synonyms = False,
                                                        languages = languages)
                if unique:
                    if syn_values is not None:
                        res.add(syn_values)
                else:
                    res.update(syn_values)
                if unique and res:
                    break

        if unique:
            if type(res) in [set, list]:
                for value in res:
                    lang = None
                    if type(value) == tuple and len(value) == 2:
                        value, lang = value
                    if lang == None or not languages or lang in languages:
                        return value
            elif type(res) == tuple:
                value, lang = res
                if lang == None or not languages or lang in languages:
                    return value
            return None
        res_for_lang = set()
        for value in res:
            lang = None
            if type(value) == tuple:
                if len(value) == 2:
                    value, lang = value
            if lang == None or not languages or lang in languages:
                res_for_lang.add(value)
        return res_for_lang


    def get_synonyms(self) -> List[URIRef]:
        """
        Get the URIs of the synonyms of this entity.
        """
        return self.data[Properties().exact_match]


    def has_synonym(self,
                    entity: Entity) -> bool:
        return entity in self.get_synonyms()


    def add_synonym(self,
                    entity: Entity,
                    score_value: float = None,
                    score_name: str = None,
                    scores: dict = None,
                    justification_string: str = "",
                    is_human_validation: bool = False,
                    no_validation: bool = False,
                    validator_name: str = "",
                    subject_match_field: List[URIRef] | List[str] | URIRef | str = None,
                    object_match_field: List[URIRef] | List[str] | URIRef | str = None,
                    match_string: str = None,
                    ) -> None:
        """
        Add a skos:exactMatch relation between an entity and
        all of its synonyms (mutually extending their synonym sets).
        Also add the URI of the synonyms into each entity's data dict.

        Args:
            entity: the new synonym of this entity.
            score_value: decisive score's value. If none, it will not create any SSSOM mapping.
            score_name: decisive score's label.
            scores: dict of {score: value}.
            justification_string: the justification for this mapping decision.
            is_human_validation: weither a human decided this mapping.
            no_validation: weither no validation was done.
            validator_name: name of the validator (human or AI).
            subject_match_field: attribute(s) of entity1 that were matched if called by attribute_merger.
            object_match_field: attribute(s) of entity2 that were matched if called by attribute_merger.
            match_string: the string that was matched between the two entities if called by attribute_merger.
        """
        if entity == self:
            print(f"Warning: adding {self.uri} as a synonym of itself.")
            return
        elif entity in self.get_synonyms():
            print(f"Warning: already mapped {self.uri} and {entity.uri}. Ignoring.")
            return
        properties = Properties()
        for synonym_uri in entity.get_synonyms():
            if synonym_uri == self.uri:
                continue
            Graph().add((self.uri, properties.exact_match, synonym_uri))
            Graph().add((synonym_uri, properties.exact_match, self.uri))
            self.data[properties.exact_match].add(synonym_uri)
            Entity(synonym_uri).data[properties.exact_match].add(self.uri)
        for synonym_uri in self.get_synonyms():

            if synonym_uri == entity.uri:
                continue
            Graph().add((entity.uri, properties.exact_match, synonym_uri))
            Graph().add((synonym_uri, properties.exact_match, entity.uri))
            entity.data[properties.exact_match].add(synonym_uri)
            Entity(synonym_uri).data[properties.exact_match].add(entity.uri)
        Graph().add((self.uri, properties.exact_match, entity.uri))
        Graph().add((entity.uri, properties.exact_match, self.uri))
        entity.data[properties.exact_match].add(self.uri)
        self.data[properties.exact_match].add(entity.uri)

        mapping_graph = MappingGraph()
        mapping_graph.add_mapping(self.uri,
                                  entity.uri,
                                  self.get_values_for("source", unique = True),
                                  entity.get_values_for("source", unique = True),
                                  score_value = score_value,
                                  score_name = score_name,
                                  scores = scores,
                                  justification_string = justification_string,
                                  is_human_validation = is_human_validation,
                                  no_validation = no_validation,
                                  validator_name = validator_name,
                                  subject_match_field = subject_match_field,
                                  object_match_field = object_match_field,
                                  match_string = match_string)


    def to_string(self,
                  exclude: list[str] = ["code",
                                        "url"],
                  include: list[str] = [],
                  limit: int = 512,
                  languages: list[str] = None,
                  use_keywords: bool = True) -> str:
        """
        Convert an entity's data dict into its string representation.
        Keys are sorted so that the generated string is always the same.

        Exclude entries from the data to ignore values that will not help LLM,
        such as any URL/URI, codes, etc.

        Args:
            data: the entity data dict
            exclude: dict entries to exclude
            include: dict entries to include (overwrites exclude)
            limit: maximum string length for each attribute of the entity.
                   -1 for no limit.
            languages: only get strings if they are in any of the languages.
        """
        res = ""
        label = self.get_values_for("label")
        if label:
            if type(label) == set:
                res = ', '.join(label)
                res += '. '
            else:
                res = label + '. '
        for key, value in sorted(self.data.items()):
            key = Graph().PROPERTIES.get_attr_name(key)
            value = self.get_values_for(key, languages = languages)
            if not include and key in exclude:
                continue
            if include and key not in include:
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
            values = []
            for v in value:
                values.append(str(v).rsplit('#')[-1]) # remove namespace
            if use_keywords:
                res += f" {key}: {', '.join([str(v) for v in values])[:limit]}."
            else:
                res += ' '.join([str(v) for v in values])[:limit]
        return res


    def __dict__(self):
        """
        Jsonify the entity.
        """
        jsonified = dict()
        for k, v in self._data.items():
            if not v:
                continue
            if type(k) != str:
                k = Properties().get_attr_name(k)
            if type(v) == URIRef:
                v = split_uri(v)[1]
            elif type(v) == set:
                v = list(v)
            if type(v) in [tuple, list]:
                for i, vv in enumerate(v):
                    if type(vv) == URIRef:
                        vv = split_uri(vv)[1]
                    elif type(vv) == tuple and len(vv) == 2:
                        # Remove None language
                        if vv[1] == None:
                            vv = vv[0]
                        else:
                            vv = vv[0] + '@' + vv[1]
                    v[i] = vv
            jsonified[k] = list(set(v)) # No duplicate
        for synonym in self.get_synonyms():
            if type(synonym) == URIRef:
                synonym = Entity(synonym)
            for k, v in synonym._data.items():
                if not v:
                    continue
                if type(k) != str:
                    k = Properties().get_attr_name(k)
                if type(v) == URIRef:
                    v = split_uri(v)[1]
                elif type(v) == set:
                    v = list(v)
                if type(v) in [tuple, list]:
                    for i, vv in enumerate(v):
                        if type(vv) == URIRef:
                            vv = split_uri(vv)[1]
                        elif type(vv) == tuple and len(vv) == 2:
                            # Remove None language
                            if vv[1] == None:
                                vv = vv[0]
                            else:
                                vv = vv[0] + '@' + vv[1]
                        v[i] = vv
                if k in jsonified:
                    jsonified[k] = set(jsonified[k]) # No duplicate
                    jsonified[k].update(set(v))
                    jsonified[k] = list(jsonified[k])
                else:
                    jsonified[k] = v
        return {self.get_values_for("label", unique = True): jsonified}


    def get_entities_from_list(extractors: Extractor | list[Extractor],
                               ent_type: str | list[str] | set[str] = {},
                               no_equivalent_in: Extractor | list[Extractor] = [],
                               has_attr: list[str] = [],
                               limit: int = -1,
                               ignore_deprecated: bool = True) -> list[Entity]:
        """
        SPARQL-free fast entity retriever.

        Retrieve all the entities that come from one or more extractors
        with some filters to refine the research.

        Args:
            extractors: the source extractor to get entities from
            no_equivalent_in: the entities from source are not linked with
                              skos:exactMatch to any entity from this list,
                              and is not a member of a synonym set with an entity
                              of the other source.
            has_attr: only return entities that have has_attr as a relation.
            limit: limits the amout of results. -1 to get the whole list
            ignore_deprecated: if True, only entities that are not deprecated
                             will be returned. Default True.
        """
        res = []
        if not Entity.entities:
            Entity.load_entities()
        if isinstance(extractors, Extractor):
            extractors = [extractors]
        extractors = [Properties().OBS[extractor.URI.lower()] for extractor in extractors]
        if type(ent_type) != set:
            ent_type = set(ent_type)
        ent_type = [Properties().OBS[et.lower()] for et in ent_type]
        if isinstance(no_equivalent_in, Extractor):
            no_equivalent_in = [no_equivalent_in]
        no_equivalent_in = [Properties().OBS[nei.URI.lower()] for nei in no_equivalent_in]
        for entity in Entity.entities.values():
            if limit == len(res):
                return res
            if not extractors or any(source in extractors for source in entity.get_values_for("source", unique = False)):
                if not ent_type or entity.get_values_for("type").intersection(ent_type):
                    if not has_attr or all(attr in entity._data for attr in has_attr):
                        if not ignore_deprecated or "deprecated" not in entity._data:
                            # no_equivalent_in check
                            equivalents = entity.get_values_for("exact_match")
                            if not equivalents:
                                res.append(entity)
                            else:
                                compatible = True
                                for equivalent in equivalents:
                                    eq = Entity(equivalent)
                                    if eq.get_values_for("source", unique = True) in no_equivalent_in:
                                        compatible = False
                                        break
                                if compatible:
                                    res.append(entity)
        return res


    def load_entities():
        """
        Load entities from graph and store them as objects
        in the Entity.entities variable.
        """
        # Load all entities from graph
        graph = Graph()
        if not Entity.entities:
            for s, p, o in graph.triples((None, graph.OBS["source"], None)):
                Entity(s)


if __name__ == "__main__":
    pass
