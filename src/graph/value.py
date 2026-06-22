"""
Similar to literals in rdflib, but is specifically designed
for reification using Blank Nodes.

Used in entities' values dict.
"""

from rdflib import Literal, Node, RDF, URIRef, PROV, BNode, XSD
from graph.properties import Properties

properties = Properties()

class Value():

    def __init__(self,
                 value: any,
                 language: str = None,
                 datatype = XSD.string,
                 provenance: set[URIRef | str] = None,
                 uri: URIRef = None):
        """
        Args:
            provenance: the list's URI.
            uri: the old URI of this BNode.
        """
        self._value = value
        self._language = language
        self._datatype = datatype
        self._provenance = set()
        if type(provenance) not in (set, tuple, list):
            provenance = {provenance}
        for p in provenance:
            if not p:
                continue # No provenance
            if type(p) != URIRef:
                p = properties.OBS[p] # EX obs:aas_list
            self._provenance.add(p)
        if provenance:
            if not uri:
                uri = BNode(_prefix = properties.OBS)
            self._uri = uri


    @property
    def value(self):
        return self._value


    @property
    def language(self):
        return self._language


    @property
    def datatype(self):
        return self._datatype


    @property
    def provenance(self):
        return self._provenance


    @property
    def uri(self):
        return self._get_value_node()


    def get_literal(self):
        # Can not use datatype with lang
        if self.language:
            return Literal(self.value,
                           lang = self.language)
        else:
            return Literal(self.value,
                           datatype = self.datatype)


    def get_value_node(self, graph) -> Node:
        """
        Get the node to add to the graph (Literal or SKOSXL String).

        Returns:
            the Node of this value (a Literal or a reified node containing the Literal and other information)
        """
        if self.provenance:
            # reification: create the BNode's URI. FIXME it may not be unique! (ex: magnetometers).
            # Maybe add entity's URI in the URI of the BNode ? Or use an actual BNode ?
            # uri = properties.OBS["skosxl-" + str(self) + '-' +  str(self.provenance).split('#')[-1]]
            uri = self._uri
            if "Calern Observatory" in str(uri) or "Calern Observatory" in str(self.get_literal()):
                print("uri=", uri, type(uri))
                print(str(self.get_literal()), type(self.get_literal()))
            graph.add((uri, properties.SKOSXL.literalForm, self.get_literal()))
            graph.add((uri, RDF.type, properties.SKOSXL.Label))
            for provenance in self.provenance:
                graph.add((uri, PROV.wasInformedBy, provenance))
            # Here it must be linked to the entity with the relation SKOSXL-prefLabel.
            return uri
        else:
            return self.get_literal()


    def __lt__(self, value):
        # to sort values
        return str(self) < str(value)


    def __eq__(self, value):
        if hasattr(value, "value"):
            value = value.value
        if hasattr(value, "provenance"):
            provenance = value.provenance
        else:
            provenance = self.provenance
        return self.value == value and self.provenance == provenance


    def __hash__(self):
        return hash(str(self))


    def __str__(self):
        return str(self.value)


    def __repr__(self):
        return f"Value@{str(self)}"