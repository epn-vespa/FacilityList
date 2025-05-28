"""
Create and manage Synonym Sets.

Has methods to get a synset's labels & other attributes from the associated
entities in the graph.
The synonym set can be used in a CandidatePair object and can be used to link
entities that are synonyms.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from collections import defaultdict
from typing import List, Set, Union
import uuid

from rdflib import OWL, RDF, SKOS, URIRef, Literal

from data_updater.extractor.extractor import Extractor
from graph import Graph
from data_merger.entity import Entity
from utils.performances import timeit

class SynonymSet:
    pass

class SynonymSet():
    """
    A synonym set contains entities from different lists.
    It should also keep track of which facility lists
    have already been computed, even if there was no match.

    A synonym set will then contain a prefLabel: the label
    that has the more counts amongst the member entities,
    and some alt labels: all the labels of all entities.

    All member entities of a Synonym Set are linked together with
    an OWL.sameAs relation in the graph.

    This class also provides methods to get the source list(s)
    of a certain synonym of the set.

    """
    synonym_sets = dict()


    def __new__(cls,
                uri: URIRef = None,
                synonyms: Set[Entity] = set()):
        """
        Object factory for SynonymSet.

        Keyword arguments:
        synonyms -- the list of members of this synonym set if it is
               a newly created synonym set.
        uri -- the uri of the list if the synonym set was loaded from
               an existing ontology
        """
        # check if any entity is in any of the synonym sets
        # for uri_, synonym_set_ in cls.synonym_sets.copy().items():
        for uri_, synonym_set_ in cls.synonym_sets.items():
            for synonym_ in synonym_set_:
                if synonym_ in synonyms:
                    synonym_set_.add_synonyms(synonyms = synonyms)
                    return synonym_set_
        if uri:
            if uri in cls.synonym_sets:
                cls.synonym_sets[uri].add_synonyms(synonyms = synonyms)
                return cls.synonym_sets[uri]
            elif not synonyms:
                # Load synonym sets:
                synonyms = set(Graph().get_members(uri))
                instance = super().__new__(cls)
                cls.synonym_sets[uri] = instance
                return instance
        else:
            uri = str(uuid.uuid4())
        # Create the SynonymSet object
        instance = super().__new__(cls)
        cls.synonym_sets[uri] = instance
        return instance


    def __init__(self,
                 synonyms: Set[Entity] = set(),
                 uri: URIRef = None):
        """
        Keyword arguments:
        synonyms -- the set of synonyms' URIs of this synonym set.
        uri -- the URI of the list if the synonym set was loaded from
               an existing ontology
        """

        if not uri:
            uri = str(uuid.uuid4())
        elif type(uri) == str:
            uri = Graph().OM.OBS[uri]
        self._uri = uri

        self._synonyms = set()
        self._data = defaultdict(set)

        if synonyms:
            self.add_synonyms(synonyms)
        else:
            self.update_synonyms()

        self.init_data()


    def __repr__(self):
        return f"SynonymSet@{self.uri}"


    @property
    def uri(self):
        return self._uri


    @uri.setter
    def uri(self,
            uri: str):
        self._uri = uri


    @property
    def synonyms(self):
        """
        A set of entities.
        """
        return self._synonyms


    @synonyms.setter
    def synonyms(self,
                 synonyms: set[URIRef]):
        self._synonyms = synonyms


    @property
    def data(self):
        return self._data


    def to_string(self,
                  exclude: list[str] = ["code",
                                        "url"]) -> str:
        res = ""
        for synonym in self.synonyms:
            res += synonym.to_string(exclude = exclude) + ' '
        return res.rstrip()


    def init_data(self):
        """
        Add data from the graph's SynonymSet entity to this object's data.
        Use to load from the graph.
        """
        graph = Graph() # get singleton
        for entity, property, value in graph.triples((self.uri, None, None)):
            if entity in (graph.OBS["hasMember"],
                          RDF.type):
                continue
            if isinstance(value, Literal):
                self._data[property].add((value.value, value.language))
            else:
                self._data[property].add((str(value), None))


    def update_synonyms(self):
        """
        Update the synonym set by using its URI and members in the graph.
        """
        synonyms = set()
        graph = Graph()
        for _, _, synonym in graph.triples((self.uri,
                                            graph.OBS["hasMember"],
                                            None)):
            synonyms.add(Entity(synonym))
        # reload the data
        self._data = defaultdict(set)
        self._synonyms = set()
        self.add_synonyms(synonyms)


    def add_synonyms(self,
                     synonyms: Union[Union[Entity,
                                           SynonymSet],
                                     Set[Union[Entity,
                                               SynonymSet]]]):
        """
        Add synonyms into a synset. Also add all of their
        data in the _data of the synset (for example, their label
        and alt labels, their location, etc.)

        Keyword arguments:
        synonyms -- the new synonyms to add
        """
        if not isinstance(synonyms, set) and not isinstance(synonyms, list):
            # assert(isinstance(synonyms, Entity))
            if isinstance(synonyms, Entity):
                synonyms = {synonyms}
            elif isinstance(synonyms, SynonymSet):
                self.add_synonyms(synonyms.synonyms)
                return
            else:
                raise TypeError(f"synonyms must be a (set of) SynonymSet and/or Entity. Got {type(synonyms)}.")
        else:
            assert(all([isinstance(s, Entity) or isinstance(s, SynonymSet) for s in synonyms]))

        for synonym in synonyms:
            if isinstance(synonym, SynonymSet):
                self.add_synonyms(synonym.synonyms)
            else:
                self.synonyms.add(synonym)
                # Update the synonym set's data
                for relation, values in synonym.data.items():
                    # Value is a dict of values for an entity
                    self._data[relation].update(values)


    def has_member(self,
                   entity: Union[Entity, URIRef]) -> bool:
        """
        Check if an entity is in the synset.

        Keyword arguments:
        entity -- the entity to check
        """
        if type(entity) == URIRef:
            return entity in [syn.uri for syn in self.synonyms]
        elif type(entity) == Entity:
            return entity in self.synonyms
        else:
            raise TypeError(f"entity must be an Entity or URIRef. Got {type(entity)}")


    def save(self):
        """
        Save one synonym set's entities into the graph and add an
        owl:sameAs relation 'between all the entities in the
        synset.

        Keyword arguments:
        synset -- the synonym set to add.
        """
        graph = Graph()
        if type(self.uri) == URIRef:
            synset_uri = self.uri
        else:
            synset_uri = graph.OBS[self.uri]

        for member1 in self:
            # add member1 to synset
            graph.add((synset_uri, RDF.type, graph.OBS["SynonymSet"]))
            graph.add((synset_uri,
                       graph.OBS["hasMember"],
                       URIRef(member1.uri)))
            for member2 in self:
                if member1 == member2:
                    continue
                # add equivalence mapping relation
                graph.add((URIRef(member1.uri), SKOS.exactMatch, URIRef(member2.uri)))


    def get_values_for(self,
                       property: str,
                       unique: bool = False,
                       language: str = None) -> Set:
        """
        Get values of the synonym set for a property.

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


    def __iter__(self):
        return iter(self._synonyms)


    def __str__(self):
        return str(f"<Object {self.__class__}> {self.uri} {self._synonyms}")


    def __repr__(self):
        return str(self)


    def __eq__(self, synset):
        return self.uri == synset.uri


    def __hash__(self):
        res = 0
        for x in sorted(self.synonyms, key = lambda x: x.uri):
            res += x.__hash__()
        return res
        #return self._synset.__hash__()


class SynonymSetManager():
    """
    Manage synonym sets of the ontology. They should be
    saved with save_all() before calling graph.serialize()
    """

    _SSM = None

    def __new__(cls, *args, **kargs):
        if cls._SSM is None:
            cls._SSM = super(SynonymSetManager, cls).__new__(cls)
        return cls._SSM


    def __init__(self):
        self._synsets = set()

        # Load Synonym Sets from graph
        g = Graph()
        for synset_uri in g.get_synsets():
            members = g.get_members(synset_uri)
            entities = set()
            for member_uri in members:
                entities.add(Entity(uri = member_uri))
            self.add_synset(entities = entities,
                            uri = synset_uri)


    def __del__(self):
        del(self._synsets)


    def add_synset(self,
                   entities: Union[SynonymSet, Set[Entity]],
                   uri: URIRef = None):
        if uri:
            assert(type(uri) == URIRef)
        if isinstance(entities, SynonymSet):
            self._synsets.add(entities)
        elif isinstance(entities, set) and uri:
            self._synsets.add(SynonymSet(synonyms = entities, uri = uri))


    def add_synpair(self,
                    entity1: Union[Entity, SynonymSet],
                    entity2: Union[Entity, SynonymSet]):
        """
        Add a pair in the synonym set manager.
        If one of the entities is in any synonym set, then it
        adds the other one in it. If none of the entities are
        in a synset, it creates a new one with both entites.
        If both are synsets, it merges the synsets into one.

        Keyword arguments:
        entity1 -- first entity (or synset) to add
        entity2 -- second entity (or synset) to add
        """

        assert(type(entity1) in [Entity, SynonymSet])
        assert(type(entity2) in [Entity, SynonymSet])

        if isinstance(entity1, Entity) and isinstance(entity2, SynonymSet):
            entity2.add_synonyms(entity1)
            self._synsets.add(entity2)
        elif isinstance(entity1, SynonymSet) and isinstance(entity2, Entity):
            entity1.add_synonyms(entity2)
            self._synsets.add(entity1)
        # Check if there is a synonym set containing one of the entities.
        # There is no synset for any of entity1 or entity2
        # so we create it.
        elif isinstance(entity1, Entity) and isinstance(entity2, Entity):
            for synset in self._synsets:
                if entity1 in synset:
                    synset.add_synonyms(entity2)
                    return
                elif entity2 in synset:
                    synset.add_synonyms(entity1)
                    return
            synset = SynonymSet(synonyms = {entity1, entity2})
            self._synsets.add(synset)

        elif isinstance(entity1, SynonymSet) and isinstance(entity2, SynonymSet):
            entity1.add_synonyms(entity2.synonyms)
            self._synsets.add(entity1)
            self._synsets.remove(entity2)


    def get_synset_for_entity(self,
                              entity: URIRef,
                              synset_uri: URIRef = None) -> Union[SynonymSet, None]:
        """
        Get the synset the entity is in. If it is not in a synset,
        returns None.

        Keyword arguments:
        entity -- find the synset for this entity.
        """
        for synset in self._synsets:
            if synset.has_member(entity):
                return synset
        # Create a synset
        if synset_uri:
            if synset_uri:
                assert type(synset_uri) == URIRef
            g = Graph()
            members = set()
            for _, _, member in g.triples((synset_uri, g.OM.OBS["hasMember"], None)):
                members.add(Entity(member))
            synonym_set = SynonymSet(synonyms = members, uri = synset_uri)
            self.add_synset(synonym_set, uri = synset_uri)
            return synonym_set
        return None


    @timeit
    def get_mapped_entities(self,
                            list1: Extractor,
                            list2: Extractor) -> Set[Entity]:
        """
        Get entities that are already linked between list1 & list2,
        (meaning that they are in a synset with each other already).
        Return entities from both list1 & list2 as a set.

        This method can accelerate the SparQL query to remove entities
        that are already linked to the other list in CandidatePair.

        Keyword arguments:
        list1 -- source the entities must belong to
        list2 -- source the entities must belong to
        """
        result = set()
        for synset in SynonymSet.synonym_sets.values():
            synonym_pair = []
            for entity in synset:
                source = entity.get_values_for("source", unique = True)
                source = source[source.rfind("#")+1:]
                if source == list1.URI.lower() or source == list2.URI.lower():
                    synonym_pair.append(entity)
            if len(synonym_pair) >= 2:
                result.update(synonym_pair)
        return result


    def save_all(self):
        """
        Save all synonym set entities to the graph and add an
        owl:sameAs relation between all the entities in a
        synset.
        """
        # for synset in self._synsets:
        for synset in SynonymSet.synonym_sets.values():
            synset.save()


    def __iter__(self):
        return iter(self._synsets)


if __name__ == "__main__":
    pass