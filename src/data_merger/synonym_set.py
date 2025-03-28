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

from rdflib import OWL, RDF, URIRef

from data_merger.graph import Graph
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
                synonyms: List[Entity] = [],
                uri: str = None):
        """
        Object factory for SynonymSet.

        Keyword arguments:
        synonyms -- the list of members of this synonym set
        uri -- the uri of the list if the synonym set was loaded from
               an existing ontology
        """
        if uri:
            if uri in cls.synonym_sets:
                cls.synonym_sets[uri]._synset.update(synonyms)
                return cls.synonym_sets[uri]
            # check if any entity is in any of the synonym sets
            for uri_, synonym_set_ in cls.synonym_sets:
                for synonym_ in synonym_set_:
                    if synonym_ in synonyms:
                        synonym_set_.add_synonyms(synonyms)
                        return synonym_set_
        else:
            #uri = str(uuid.uuid4())
            instance = super().__new__(cls)
            #cls.synonym_sets[uri] = instance
            #instance._uri = uri
            #instance._synonyms = set(synonyms)
            #instance.init_data()
            return instance


    def __init__(self,
                 synonyms: List[Entity],
                 uri: str = None):
        if not uri:
            uri = str(uuid.uuid4())
        self._uri = uri
        self._data = defaultdict(set)
        for synonym in synonyms:
            assert(type(synonym) == Entity)
            for key, value in synonym.data.items():
                self._data[key].update(value)
        self._synonyms = synonyms
        self.init_data()


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


    def init_data(self):
        """
        Add data from the graph's SynonymSet entity to this object's data.
        """
        self._data = defaultdict(list)
        for entity, property, value in Graph._graph.triples((self.uri, None, None)):
            self._data[property].update(value)


    def add_synonyms(self,
                    synonyms: Union[Entity, List]):
        """
        Add a synonym in a synset. Also add all of its
        data in the _data of the synset (for example, its label
        and alt labels, its location, etc)

        Keyword arguments:
        synonym -- the new synonym to add
        """
        if not isinstance(synonyms, list):
            synonyms = [synonyms]

        for synonym in synonyms:
            self.synonyms.add(synonym)
            # Update the synonym set's data
            for key, value in synonym.data.items():
                self._data[key].add(value)


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
        graph = Graph._graph
        synset_uri = graph.OBS[self.uri]
        for member1 in self:
            # add member1 to synset
            graph.add((synset_uri, RDF.type, graph.OBS["SynonymSet"]))
            graph.add((synset_uri, graph.OBS["hasMember"], member1.uri))
            for member2 in self:
                if member1 == member2:
                    continue
                # add equivalentClass relation
                graph.add((member1.uri, OWL.equivalentClass, member2.uri))


    def get_values_for(self,
                       property: str) -> Set:
        """
        Get values of the synonym set for a property.

        Keyword arguments:
        property -- the property name (ex: "label")
        """
        property = Graph.OM.convert_attr(property)
        if property in self._data:
            return self._data[property]
        else:
            # No value for this property
            return set()


    def __iter__(self):
        for item in list(self._synonyms):
            yield(item)


    def __str__(self):
        return str(f"<Object {self.__class__}> {self.uri} {self._synonyms}")


    def __eq__(self, synset):
        return self.uri == synset.uri


    def __hash__(self):
        res = 0
        for x in self.synonyms:
            res += x.__hash__()
        return res
        #return self._synset.__hash__()


class SynonymSetManager():
    """
    Manage synonym sets of the ontology. They should be
    saved with save_all() before calling graph.serialize()
    """


    def __init__(self):
        self._synsets = set()


    def add_synset(self,
                   entity1: Entity,
                   entity2: Union[Entity, SynonymSet]) -> SynonymSet:
        """
        Add a pair in the synonym set manager.
        If one of the entities is in any synonym set, then it
        adds the other one in it. If none of the entities are
        in a synset, it creates a new one with both entites.

        Keyword arguments:
        entity1 -- first entity to add
        entity2 -- second entity to add
        """
        assert(type(entity1) == Entity)
        assert(type(entity2) in [Entity, SynonymSet])

        if isinstance(entity2, SynonymSet):
            entity2.add_synonym(entity1)
            self._synsets.add(entity2)
            return
        # Check if there is a synonym set containing one of the entities.
        # There is no synset for any of entity1 or entity2
        # so we create it.
        else:
            synset = SynonymSet([entity1, entity2])
            self._synsets.add(synset)
            return synset


    def get_synset_for_entity(self,
                              entity: URIRef) -> Union[URIRef, None]:
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