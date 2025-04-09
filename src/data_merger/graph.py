"""
Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import os
import warnings

from typing import Iterator, Tuple
from rdflib import URIRef
from data_updater.graph import Graph as G
from data_updater.extractor.extractor import Extractor
from utils.performances import timeit
from utils.utils import standardize_uri


class Graph(G):

    """
    Instanciate a rdflib Graph as a singleton to prevent multiple
    instantiation. Replace rdflib.Graph and reuses rdflib.Graph's attributes.
    This overrides data_updater's Graph to add other functions.

    Usage:
        from graph import Graph
        g = Graph() # instanciate
        obs = g.OBS # get Observation Facility namespace

        # Then, use g as a rdflib graph object:
        g.add((g.OBS["subj"], URIRef("predicate"), URIRef("obj")))
    """


    def __init__(self):
        """
        Initialize the graph.

        Keyword arguments:
        filename -- input ontology file
        """
        if Graph._graph is not None:
            if not Graph._warned:
                warnings.warn("Can not create another instance of MyGraph. Ignored.")
                Graph._warned = True
            return
        Graph._graph = G()


    def parse(self,
              filename: str):
        """
        Overrides rdflib.Graph's parse to extract the namespaces and
        save them in this object.
        """
        input_ontology_namespaces = []
        if os.path.exists(filename):
            Graph._graph.parse(filename)
            for prefix, namespace in Graph._graph.namespaces():
                if prefix in Extractor.AVAILABLE_NAMESPACES:
                    input_ontology_namespaces.append(prefix)
        else:
            raise FileNotFoundError(f"File {filename} does not exist.")

        self._available_namespaces = input_ontology_namespaces


    @property
    def graph(self):
        return Graph._graph


    def __call__(self):
        return Graph._graph


    def __getattr__(self, attr):
        """
        For other attribute
        """
        return getattr(Graph._graph, attr)


    """
    @property
    def OBS(self):
        return OntologyMapping._OBS


    @property
    def GEO(self):
        return OntologyMapping._GEO


    @property
    def WB(self):
        return OntologyMapping._WB
    """


    def is_available(self,
                     namespace: str) -> bool:
        """
        Returns true if the namespace of a list is available in the graph.

        Keyword arguments:
        namespace -- the namespace to test
        """
        return namespace in self._available_namespaces


    @timeit
    def get_entities_from_list(self,
                               source: Extractor,
                               no_equivalent_in: Extractor = None,
                               ) -> Iterator[Tuple[URIRef]]:
        """
        Get all the entities that come from a list.
        If an entity is already in a synset, it will return the synset
        too.

        Keyword arguments:
        source -- the source extractor to get entities from
        no_equivalent_in -- the entities from source are not linked with
                        owl:equivalentClass to any entity from this list,
                        and is not a member of a synonym set with an entity
                        of the other source.
        """

        if isinstance(source, Extractor):
            source = source.URI
            source = standardize_uri(source)
        else:
            raise TypeError(f"'source' ({source}) should be an Extractor. " +
                            f"Got {type(source)}.")

        if no_equivalent_in is None:
            # Get entities with their corresponding synonym sets
            # if they are a members of a synonym set already.
            query = f"""
            SELECT ?entity ?synset
            WHERE {{
                ?entity obs:source obs:{source} .
                OPTIONAL {{
                    ?synset obs:hasMember ?entity .
                    ?synset a obs:SynonymSet .
                }}
            }}
            """
        else:
            if isinstance(no_equivalent_in, Extractor):
                no_equivalent_in = no_equivalent_in.URI
                no_equivalent_in = standardize_uri(no_equivalent_in)
            else:
                raise TypeError(f"'no_equivalent_in' ({source}) should be an Extractor. "
                                + f"Got {type(source)}.")

            query = f"""
            SELECT ?entity ?synset
            WHERE {{
                ?entity obs:source obs:{source} .
                OPTIONAL {{
                    ?synset a obs:SynonymSet .
                    ?synset obs:hasMember ?entity .
                }}
                FILTER NOT EXISTS {{
                    ?entity owl:equivalentClass ?entity2 .
                    ?entity2 obs:source obs:{no_equivalent_in} .
                }}
                FILTER NOT EXISTS {{
                    ?synset a obs:SynonymSet .
                    ?synset obs:hasMember ?entity .
                    ?synset obs:hasMember ?entity3 .
                    ?entity3 obs:source obs:{no_equivalent_in} .
                }}
            }}
            """
        return self.query(query)

    # TODO function to get CandidatePairs from two entities
    def get_candidate_pair_uri(self,
                               entity1: URIRef,
                               entity2: URIRef) -> URIRef:
        """
        If there is a CandidatePair between two entities, get
        its URI to prevent duplicating the Candidate Pair.
        Entity1 and entity2 must be entities' URI and not Synonym Sets.

        Keyword arguments:
        entity1 -- the first member of the pair (use an entity not a synset)
        entity2 -- the second member of the pair (use an entity not a synset)
        """
        query = f"""
        SELECT ?candidate_pair
        WHERE {{
            ?candidate_pair a obs:CandidatePair .

            # Member 1
            {{
                ?candidate_pair obs:hasMember <{entity1}> .
            }} UNION {{
                ?someSynonymSet1 a obs:SynonymSet .
                ?candidate_pair obs:hasMember ?someSynonymSet1 .
                ?someSynonymSet1 obs:hasMember <{entity1}> .
            }} UNION {{
                <{entity1}> a obs:SynonymSet .
                <{entity1}> obs:hasMember ?member1 .
                ?candidate_pair obs:hasMember ?member1 .
            }}

            # Member 2
            {{
                ?candidate_pair obs:hasMember <{entity2}> .
            }} UNION {{
                ?someSynonymSet2 a obs:SynonymSet .
                ?candidate_pair obs:hasMember ?someSynonymSet2 .
                ?someSynonymSet2 obs:hasMember <{entity2}> .
            }} UNION {{
                <{entity2}> a obs:SynonymSet .
                <{entity2}> obs:hasMember ?member2 .
                ?candidate_pair obs:hasMember ?member2 .
            }}
        }}
        """
        return self.query(query)


    def get_descriptions(self) -> Iterator[str]:
        for _, _, desc in self._graph.triples((None, self.OBS["description"], None)):
            yield desc


if __name__ == "__main__":
    pass