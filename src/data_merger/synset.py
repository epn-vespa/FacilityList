"""
Create and manage Synonym Sets.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from typing import List, Union
import uuid

from rdflib import OWL, RDF, URIRef

from data_merger.graph import Graph


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

    def __init__(self,
                 synonyms: List[URIRef],
                 uri: str = ""):
        self._synset = set(synonyms)

        if not uri:
            self._uri = str(uuid.uuid4())
        else:
            self._uri = uri


    @property
    def uri(self):
        return self._uri


    def add_synonym(self,
                    synonym: URIRef):
        """
        Add a synonym in a synset.

        Keyword arguments:
        synonym -- the new synonym to add
        """
        self._synset.add(synonym)


    def has_member(self,
                  entity: URIRef) -> bool:
        """
        Check if an entity is in the synset.

        Keyword arguments:
        entity -- the entity to check
        """
        return entity in self._synset


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
            graph.add((synset_uri, graph.OBS["hasMember"], member1))
            for member2 in self:
                if member1 == member2:
                    continue
                # add equivalentClass relation
                graph.add((member1, OWL.equivalentClass, member2))


    def __iter__(self):
        for item in list(self._synset):
            yield(item)


class SynonymSetManager():


    def __init__(self):
        self._synsets = []
        # TODO the synsets should be loaded when we load
        # a graph that already has synsets too.


    def add_synset(self,
                   entity1: URIRef,
                   entity2: URIRef) -> SynonymSet:
        """
        Add a pair in the synonym set manager.
        If one of the entities is in any synonym set, then it
        adds the other one in it. If none of the entities are
        in a synset, it creates a new one with both entites.

        Keyword arguments:
        entity1 -- first entity to add
        entity2 -- second entity to add
        """
        for synset in self._synsets:
            if synset.has_member(entity1):
                synset.add_synonym(entity2)
                return synset
            if synset.has_member(entity2):
                synset.add_synonym(entity1)
                return synset
        # There is no synset for any of entity1 or entity2
        # so we create it.
        synset = SynonymSet([entity1, entity2])
        self._synsets.append(synset)
        synset.save() # directly add the synset into the graph
        return synset


    def get_synset_for_entity(self,
                              entity: URIRef) -> Union[URIRef, None]:
        """
        Get the synset the entity is in. If it is not in a synset,
        returns None.

        Keyword arguments:
        entity -- find the synset for this entity.
        """
        for synset in self:
            if synset.has_member(entity):
                return synset
        return None


    @DeprecationWarning
    def save_all(self):
        """
        Save all synonym set entities to the graph and add an
        owl:equivalentClass relation between all the entities in a
        synset.
        """
        for synset in self._synsets:
            self.save_one(synset)


    def __iter__(self):
        for item in list(self._synsets):
            yield(item)


if __name__ == "__main__":
    pass