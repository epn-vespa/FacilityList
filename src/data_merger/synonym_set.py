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
from typing import Set, Union
import uuid

from rdflib import OWL, RDF, URIRef

from graph import Graph
from data_merger.entity import Entity


class SynonymSet():
    """
    A synonym set contains entities from different lists.
    It should also keep track of which facility lists
    have already been computed, even if there was no match.

    A synonym set will then contain a prefLabel: the label
    that has the more counts amongst the member entities,
    and some alt labels: all the labels of all entities.

    All member entities of a Synonym Set are linked together with
    an OWL.equivalentClass relation in the graph.

    This class also provides methods to get the source list(s)
    of a certain synonym of the set.

    """
    synonym_sets = dict()


    def __new__(cls,
                uri: str = "",
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
        for uri_, synonym_set_ in cls.synonym_sets.items():
            for synonym_ in synonym_set_:
                if synonym_ in synonyms:
                    synonym_set_.add_synonyms(synonyms)
                    return synonym_set_
        if uri:
            if uri in cls.synonym_sets:
                cls.add_synonyms
                cls.synonym_sets[uri]._synonyms.update(synonyms)
                return cls.synonym_sets[uri]
        else:
            uri = str(uuid.uuid4())
        instance = super().__new__(cls)
        cls.synonym_sets[uri] = instance
        return instance


    def __init__(self,
                 synonyms: Set[URIRef] = set(),
                 uri: str = "",):
        """
        Keyword arguments:
        synonyms -- the set of synonyms' URIs of this synonym set.
        uri -- the URI of the list if the synonym set was loaded from
               an existing ontology
        """

        if not uri:
            uri = str(uuid.uuid4())
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


    @property
    def data(self):
        return self._data


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
            self._data[property].update(value)


    def update_synonyms(self):
        """
        Update the synonym set by using its URI and members in the graph.
        """
        synonyms = set()
        graph = Graph()
        for _, _, synonym in graph.triples((self.uri,
                                            graph.OBS["hasMember"],
                                            None)):
            synonyms.add(synonym)
        # reload the data
        self._data = defaultdict(set)
        self._synonyms = set()
        self.add_synonyms(synonyms)


    def add_synonyms(self,
                     synonyms: Union[Entity, Set[Entity]]):
        """
        Add synonyms into a synset. Also add all of their
        data in the _data of the synset (for example, their label
        and alt labels, their location, etc.)

        Keyword arguments:
        synonyms -- the new synonyms to add
        """
        if not isinstance(synonyms, set) and not isinstance(synonyms, list):
            assert(isinstance(synonyms, Entity))
            synonyms = set(synonyms)
        else:
            assert(all([isinstance(s, Entity) for s in synonyms]))


        for synonym_uri in synonyms:
            synonym = synonym_uri #Entity(synonym_uri)
            self.synonyms.add(synonym)
            # Update the synonym set's data
            for relation, values in synonym.data.items():
                # Value is a dict of values for an entity
                self._data[relation].update(values)


    def has_member(self,
                   entity: Entity) -> bool:
        """
        Check if an entity is in the synset.

        Keyword arguments:
        entity -- the entity to check
        """
        return entity in self.synset


    def save(self):
        """
        Save one synonym set's entities into the graph and add an
        owl:equivalentClass relation 'between all the entities in the
        synset.

        Keyword arguments:
        synset -- the synonym set to add.
        """
        graph = Graph()
        synset_uri = graph.OBS[self.uri]
        for member1 in self:
            # add member1 to synset
            graph.add((URIRef(synset_uri), RDF.type, graph.OBS["SynonymSet"]))
            graph.add((URIRef(synset_uri),
                       graph.OBS["hasMember"],
                       URIRef(member1.uri)))
            for member2 in self:
                if member1 == member2:
                    continue
                # add equivalentClass relation
                graph.add((URIRef(member1.uri), OWL.equivalentClass, URIRef(member2.uri)))


    def get_values_for(self,
                       property: str,
                       unique: bool = False) -> Set:
        """
        Get values of the synonym set for a property.

        Keyword arguments:
        property -- the property name (ex: "label")
        unique -- if there was more than one values,
                  return only the first non-None value.
        """
        property = Graph().OM.convert_attr(property)
        if property in self._data:
            res = set(self._data[property])
        else:
            # No value for this property
            res = set()
        if unique:
            if hasattr(res, "len") and not isinstance(res, str):
                if len(res):
                    return list(res)[0]
                else:
                    return None
        return res


    def __iter__(self):
        for item in list(self._synonyms):
            yield(item)


    def __str__(self):
        return str(f"<Object {self.__class__}> {self.uri} {self._synonyms}")


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


    def __del__(self):
        del(self._synsets)


    def add_synset(self,
                   entity1: Union[Entity, SynonymSet],
                   entity2: Union[Entity, SynonymSet]):
        """
        Add a pair in the synonym set manager.
        If one of the entities is in any synonym set, then it
        adds the other one in it. If none of the entities are
        in a synset, it creates a new one with both entites.
        If both are synsets, it merges the synsets into one.

        Keyword arguments:
        entity1 -- first entity to add
        entity2 -- second entity to add
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
            synset = SynonymSet(synonyms = {entity1, entity2})
            self._synsets.add(synset)
        elif isinstance(entity1, SynonymSet) and isinstance(entity2, SynonymSet):
            entity1.add_synonyms(entity2.synonyms)
            self._synsets.add(entity1)
            self._synsets.remove(entity2)


    def get_synset_for_entity(self,
                              entity: URIRef) -> Union[SynonymSet, None]:
        """
        Get the synset the entity is in. If it is not in a synset,
        returns None.

        Keyword arguments:
        entity -- find the synset for this entity.
        """
        for synset in self._synsets:
            if synset.has_member(entity):
                return synset
        return None


    def save_all(self):
        """
        Save all synonym set entities to the graph and add an
        owl:equivalentClass relation between all the entities in a
        synset.
        """
        for synset in self._synsets:
            synset.save()


    def __iter__(self):
        for item in list(self._synsets):
            yield(item)



if __name__ == "__main__":
    pass