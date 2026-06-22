"""
Represent and manage entity objects.
Has methods to get an entity's labels & other attributes from the graph.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from collections import defaultdict
from typing import Any
from rdflib import Literal, URIRef, SKOS, BNode
from rdflib.namespace import split_uri
from graph.mapping_graph import MappingGraph
from graph.extractor.extractor import Extractor
from graph.entity_types import get_types_intersections

from graph.graph import Graph
from graph.value import Value, ValueSet
from graph.properties import Properties
from utils.string_utilities import cut_language_from_string

properties = Properties()


class Entity:
    pass


class Entity():

    # Save entities' uri to prevent multi instanciation
    # {uri: Entity}
    entities = dict()

    def __new__(cls,
                uri: URIRef):
        assert type(uri) == URIRef
        if uri in cls.entities:
            return cls.entities[uri]
        else:
            instance = super().__new__(cls)
            cls.entities[uri] = instance
            return instance


    def __init__(self,
                 uri: URIRef):
        self._uri = URIRef(uri)
        self._data = defaultdict(ValueSet)
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
                        self._data[property].add(Value(value_str, lang))
                        continue
                self._data[property].add(Value(value.value, value.language, value.datatype))
            elif isinstance(value, BNode):
                # Entity
                uri = value # BNode
                found_prov = False
                lang = None
                for _, _, value in graph.triples((uri, properties.label, None)):
                    # get value (as a Literal)
                    value_str, lang = cut_language_from_string(value.value)
                for _, _, prov in graph.triples((value, properties.provenance, None)):
                    self._data[property].add(Value(value = value,
                                                   language = lang,
                                                   datatype = properties.get_type(property),
                                                   provenance = prov,
                                                   uri = uri))
                    found_prov = True
                if not found_prov:
                    if property in properties._KEEP_PROVENANCE:
                        added = False
                        # get provenance of this entity
                        provs = set()
                        for _, _, prov in graph.triples((uri, properties.provenance, None)):
                            provs.add(prov)
                        self._data[property].add(Value(value = value_str,
                                                       language = lang,
                                                       provenance = provs))
                    else:
                        self._data[property].add(Value(value = value_str,
                                                       language = lang))
            elif isinstance(value, URIRef):
                    self._data[property].add(value) # URIRef #(Value(value = value,
                                             #       datatype = URIRef)))
            else:
                self._data[property].add(Value(str(value)))


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


    @data.setter
    def data(self, d: dict) -> None:
        """
        This setter convert the dict's keys to Graph node properties, and
        the dict's values to sets.
        """
        for k, v in d.copy().items():
            del d[k]
            k = properties.convert_attr(k)
            if not isinstance(v, ValueSet):
                v = ValueSet(v)
            d[k] = v
        self._data = d


    def get_values_for(self,
                       property: str,
                       unique: bool = False,
                       languages: list[str] = None,
                       extend_to_synonyms: bool = True,
                       extractors: list[str | URIRef] = [],
                       return_raw_value: bool = True) -> ValueSet[Value]:
        """
        Get values of the entity for a property.

        Args:
            property: the property name (ex: "label")
            unique: if there was more than one values,
                    return only the first non-None value.
            languages: only get fields labeled with those languages if known.
            extend_to_synonyms: will also get the entity's synonyms' features.
            extractors: return only values from this extractor. (TODO)
            return_raw_value: if True, return str, int etc else return the Value object.
        """
        property = Properties().convert_attr(property)
        if property in self.data:
            res = self.data[property]
        elif URIRef(property) in self.data:
            res = self.data.get(URIRef(property))
        else:
            # No value for this property
            res = ValueSet()

        if extend_to_synonyms and (not unique or not res):
            for syn in self.get_synonyms():
                syn_values = Entity(syn).get_values_for(property,
                                                        unique = unique,
                                                        extend_to_synonyms = False,
                                                        languages = languages,
                                                        extractors = extractors,
                                                        return_raw_value = return_raw_value)
                if unique:
                    if syn_values is not None:
                        res.add(syn_values)
                else:
                    res.update(syn_values)
                if unique and res:
                    break

        if unique:
            if type(res) in [set, list, tuple, ValueSet]:
                for value in sorted(res):
                    if return_raw_value and type(value) == Value:
                        value = value.value
                    return value
            return None
        res_for_lang = ValueSet()
        for value in res:
            lang = None
            #if type(value) == tuple:
            #    if len(value) == 2:
            #        value, lang = value
            if hasattr(value, "language"):
                lang = value.language
            if lang == None or not languages or lang in languages:
                # TODO also add a filter for extractors (provenance)
                if return_raw_value and type(value) == Value:
                    value = value.value
                res_for_lang.add(value)

        return res_for_lang


    def remove_values(self,
                      attr: str,
                      values: list[Value | Any] = [],
                      languages: list = [],
                      extractors: list[URIRef | str] = [] # TODO
                     ):
        """
        Remove all values for attr, or remove only the specified values
        for this attr if specified.

        Args:
            attr: attribute to remove
            values: values to remove. If none is set, will remove all values for attr.
            languages: remove values with languages.
            extractors: remove values with the provenance of a certain extractor.
        """
        if attr not in self._data:
            return
        if not values and not languages and not extractors:
            del self._data[attr]
        else:
            v = self._data[attr]
            for val in values:
                if not languages or val.language in languages:
                    if not extractors or val.provenance in extractors:
                        for vv in v.copy():
                            if vv == val:
                                self._data[attr].remove(vv)


    def remove_value(self,
                     attr: str,
                     value):
        """
        Remove one value in the specified attribute.

        Args:
            attr: the entity's attribute
            value: the value within this attribute
        """
        values = self.get_values_for(attr)
        if value in values:
            values.remove(value)


    def get_synonyms(self) -> list[URIRef]:
        """
        Get the URIs of the synonyms of this entity.
        """
        return self.data.get(Properties().exact_match, [])


    def has_synonym(self,
                    entity: Entity) -> bool:
        return entity in self.get_synonyms()


    def add_synonym(self,
                    entity: Entity,
                    extractor1: Extractor,
                    extractor2: Extractor,
                    score_value: float = None,
                    score_name: str = None,
                    scores: dict = None,
                    filters: list = None,
                    justification_string: str = "",
                    is_human_validation: bool = False,
                    no_validation: bool = False,
                    validator_name: str = "",
                    subject_match_field: list[URIRef] | list[str] | URIRef | str = None,
                    object_match_field: list[URIRef] | list[str] | URIRef | str = None,
                    match_string: str = None,
                    ) -> None:
        """
        Add a skos:exactMatch relation between an entity and
        all of its synonyms (mutually extending their synonym sets).
        Also add the URI of the synonyms into each entity's data dict.

        Args:
            entity: the new synonym of this entity.
            extractor1: the source extractor of this entity.
            extractor2: the source extractor of the synonym entity.
            score_value: decisive score's value. If none, it will not create any SSSOM mapping.
            score_name: decisive score's label.
            scores: dict of {score: value}.
            filters: list of Filters.
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
        uri1 = self.uri
        uri2 = entity.uri
        for synonym_uri in entity.get_synonyms():
            if synonym_uri == uri1:
                continue
            Graph().add((uri1, properties.exact_match, synonym_uri))
            Graph().add((synonym_uri, properties.exact_match, uri1))
            self.data[properties.exact_match].add(synonym_uri)
            Entity(synonym_uri).data[properties.exact_match].add(uri1)
        for synonym_uri in self.get_synonyms():
            if synonym_uri == uri2:
                continue
            Graph().add((uri2, properties.exact_match, synonym_uri))
            Graph().add((synonym_uri, properties.exact_match, uri2))
            entity.data[properties.exact_match].add(synonym_uri)
            Entity(synonym_uri).data[properties.exact_match].add(uri2)
        Graph().add((uri1, properties.exact_match, uri2))
        Graph().add((uri2, properties.exact_match, uri1))
        entity.data[properties.exact_match].add(uri1)
        self.data[properties.exact_match].add(uri2)

        mapping_graph = MappingGraph() # Should be already instantiated
        # URIs to be used

        mapping_graph.add_mapping(uri1,
                                  uri2,
                                  entity1_source = mapping_graph._OBS[extractor1.URI],
                                  entity2_source = mapping_graph._OBS[extractor2.URI],
                                  score_value = score_value,
                                  score_name = score_name,
                                  scores = scores,
                                  filters = filters,
                                  justification_string = justification_string,
                                  is_human_validation = is_human_validation,
                                  no_validation = no_validation,
                                  validator_name = validator_name,
                                  subject_match_field = subject_match_field,
                                  object_match_field = object_match_field,
                                  match_string = match_string)


    def add_broad_narrow_relation(self,
                                  entity: URIRef,
                                  extractor1: Extractor,
                                  extractor2: Extractor,
                                  score_value: float = None,
                                  score_name: str = None,
                                  scores: dict = None,
                                  filters = None,
                                  justification_string: str = "",
                                  is_human_validation: bool = False,
                                  no_validation: bool = False,
                                  validator_name: str = "",
                                  is_broad: bool = False) -> None:
        """
        Add broad or narrow relation type between the two entities.

        Args:
            entity: the new synonym of this entity.
            extractor1: the source extractor of this entity.
            extractor2: the source extractor of the synonym entity.
            score_value: decisive score's value. If none, it will not create any SSSOM mapping.
            score_name: decisive score's label.
            scores: dict of {score: value}.
            filters: list of Filters.
            justification_string: the justification for this mapping decision.
            is_human_validation: weither a human decided this mapping.
            no_validation: weither no validation was done.
            validator_name: name of the validator (human or AI).
            is_broad: if True, add broad relation (isPartOf),
                      else add narrow relation (hasPart)
        """
        mapping_graph = MappingGraph()
        # URIs to be used
        uri1 = self.uri
        for synonym_uri in self.get_synonyms():
            if extractor1.NAMESPACE == synonym_uri.split('#')[0].split('/')[-1]:
                uri1 = synonym_uri
                break
        uri2 = entity.uri
        for synonym_uri in entity.get_synonyms():
            if extractor2.NAMESPACE == synonym_uri.split('#')[0].split('/')[-1]:
                uri2 = synonym_uri
                break
        graph = Graph()
        if is_broad:
            #graph.add((uri1, DCTERMS.isPartOf, uri2))
            #graph.add((uri2, DCTERMS.hasPart, uri1))
            graph.add((uri1, SKOS.broadMatch, uri2))
            graph.add((uri2, SKOS.narrowMatch, uri1))
            entity.data[properties.has_part].add(uri1)
            self.data[properties.is_part_of].add(uri2)
        else:
            #graph.add((uri1, DCTERMS.hasPart, uri2))
            #graph.add((uri2, DCTERMS.isPartOf, uri1))
            graph.add((uri1, SKOS.narrowMatch, uri2))
            graph.add((uri2, SKOS.broadMatch, uri1))
            entity.data[properties.is_part_of].add(uri1)
            self.data[properties.has_part].add(uri2)
        predicate = SKOS.broadMatch if is_broad else SKOS.narrowMatch
        mapping_graph.add_mapping(entity1 = uri1,
                                  entity2 = uri2,
                                  entity1_source = mapping_graph._OBS[extractor1.URI],
                                  entity2_source = mapping_graph._OBS[extractor2.URI],
                                  score_value = score_value,
                                  score_name = score_name,
                                  scores = scores,
                                  filters = filters,
                                  justification_string = justification_string,
                                  is_human_validation = is_human_validation,
                                  no_validation = no_validation,
                                  validator_name = validator_name,
                                  predicate = predicate)


    def to_string(self,
                  exclude: list[str] = ["code",
                                        "url"],
                  include: list[str] = [],
                  limit: int = 512,
                  languages: list[str] = None,
                  use_keywords: bool = True,
                  get_label_of_uris: bool = False) -> str:
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
            use_keywords: weither to add keywords before the content or only
                          keep the content.
            get_label_of_uris: use labels instead of URIs for entities known
                               in this graph (self ref) ex: has_part.
        """
        res = ""
        label = self.get_values_for("label")
        alt_labels = self.get_values_for("alt_label")
        if label:
            if isinstance(label, set):
                #res = ', '.join(label)
                label = list(label)
                if len(label) > 1:
                    alt_labels.update(label[1:])
                res += "Main label: " + label[0]
                res += '. '
            else:
                res = label + '. '
        if alt_labels:
            key = "Also known as: "
            res += key + ', '.join(alt_labels)[:limit] + '. '
        for key, value in sorted(self.data.items()):
            key = properties.get_attr_name(key)
            value = self.get_values_for(key, languages = languages)
            if not include and key in exclude :
                continue
            if include and key not in include:
                continue
            if key == "label":
                continue
            if key == "alt_label":
                continue
                # Only keep ten alt labels
                #res += f" {key}: {', '.join(sorted(value, key = lambda x: 1/len(x))[:10])}"
                #continue
            if type(value) not in [list, set, tuple, ValueSet]:
                value = [value]
            values = []
            for v in value:
                if get_label_of_uris and key in properties._SELF_REF:
                    ref_entity = Entity(v)
                    v = ref_entity.label
                values.append(str(v).rsplit('#')[-1]) # remove namespace
            if use_keywords:
                key_str = key.replace('_', ' ').capitalize()
                res += f" {key_str}: {', '.join([str(v) for v in sorted(values)])[:limit]}."
            else:
                res += ' '.join([str(v) for v in sorted(values)])[:limit]
        return res


    def add_source_to_attributes(self):
        """
        Add source to Value objects that are in properties._KEEP_PROVENANCE.
        """
        for attr, values in self._data.items():
            attr = properties.get_attr_name(attr)
            if attr in properties._KEEP_PROVENANCE:
                for value in values:
                    value.provenance = self.source


    def __dict__(self, extend_to_synonyms: bool = True):
        """
        Jsonify the entity.
        """
        jsonified = dict()
        for k, v in self._data.items():
            if not v:
                continue
            if type(k) != str:
                k = properties.get_attr_name(k)
            if type(v) == URIRef:
                v = split_uri(v)[1]
            elif isinstance(v, set):
                v = list(v)
            if type(v) in [tuple, list]:
                for i, vv in enumerate(v):
                    if type(vv) == URIRef:
                        vv_str = split_uri(vv)[1]
                    elif type(vv) == Value:
                        vv_str = vv.value
                        if vv.language:
                            vv_str += '@' + vv.language
                    else:
                        vv_str = str(vv)
                    """
                    elif type(vv) == tuple and len(vv) == 2:
                        # Remove None language
                        if vv[1] == None:
                            vv = vv[0]
                        else:
                            vv = vv[0] + '@' + vv[1]
                    """
                    v[i] = vv_str
            jsonified[k] = list(ValueSet(v)) # No duplicate
        if extend_to_synonyms:
            for synonym in self.get_synonyms():
                if type(synonym) == URIRef:
                    synonym = Entity(synonym)
                for k, v in synonym._data.items():
                    if not v:
                        continue
                    if type(k) != str:
                        k = Properties().get_attr_name(k)
                    if isinstance(v, URIRef):
                        v = split_uri(v)[1]
                    elif isinstance(v, set):
                        v = list(v)
                    if type(v) in [tuple, list]:
                        for i, vv in enumerate(v):
                            if type(vv) == URIRef:
                                vv_str = split_uri(vv)[1]
                            elif type(vv) == Value:
                                vv_str = vv.value
                                if vv.language:
                                    vv_str += '@' + vv.language
                            else:
                                vv_str = str(vv)
                            """
                            elif type(vv) == tuple and len(vv) == 2:
                                # Remove None language
                                if vv[1] == None:
                                    vv = vv[0]
                                else:
                                    vv = vv[0] + '@' + vv[1]
                            """
                            v[i] = vv_str
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
                if not ent_type or get_types_intersections(entity.get_values_for("type"), ent_type):
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
            for s, p, o in graph.triples((None, properties.source, None)):
                Entity(s)


if __name__ == "__main__":
    pass
