"""
Post-process a merged ontology (OntoPortal view).

- generate a definition with GPT (summarize entity or expand description
using search through papers to keep the description homogeneous in length)
- remove other definitions
- move attrs from platform to individual instruments (ex: waveband)
- remove some unwanted attrs
- re-organize the skos:notation, skos:altLabel and skos:sameAs values while
reificating them by source of reference (IAUMPC, NSSDC, Wikidata...)
"""
from argparse import ArgumentParser
from rdflib import RDF, URIRef, Literal, SKOS, DCTERMS
from graph.graph import Graph
from graph.entity import Entity
from graph.properties import Properties
from llm.llm_connection import LLMConnection
import config
properties = Properties()

class PostProcess():


    def __init__(self,
                 graph: Graph):
        self._graph = graph
        self._labels_by_uris = dict()


    def __iter__(self):
        done = set()
        for uri, _, _ in self._graph.triples((None, RDF.type, None)):
            if uri in done:
                continue
            done.add(uri)
            yield uri


    def __call__(self):
        config.configure_ollama()
        i = 0
        for uri in self.__iter__():
            self._remove_attrs(uri) # Remove attrs that are generated automatically to prevent generating errors
            self._gen_label(uri) # Must call before _gen_definition
            self._gen_definition(uri)
            i += 1
            #if i == 10:
            #    exit(0)


    def _remove_attrs(self, uri: URIRef):
        """
        Remove unwanted attributes:
        - location_confidence
        - location details if the location_confidence is not 1.
        """
        entity = Entity(uri)
        loc_conf = entity.get_values_for("location_confidence", unique = True)
        if loc_conf is not None:
            entity.remove_values("location_confidence")
            if loc_conf < 1:
                entity.remove_values("address")
                entity.remove_values("country")
                entity.remove_values("state")
                entity.remove_values("continent")


    def _gen_label(self, uri: URIRef):
        """
        Generate a label for the entity based on its attributes.

        Attrs:
            uri: the entity's URI
        """
        entity = Entity(uri)
        entity_str = entity.to_string(exclude = properties._LINKS + properties._EXT_REF + properties._METADATA)
        prompt = "Generate one label following this format: " + \
                 "aperture (if known) or waveband (if known) label(if known, no acronym if possible) at/on broader_label(ex:spacecraft carrying this entity, or observatory, only if it is an entity within an observatory, a telescope or a spacecraft.)" + \
                 "Example: 3.6m Bernart Lyot telescope at Midi-Pyrénées Observatory\n" + \
                 "Example: Near Infrared Camera and Multi-Object Spectrometer on Hubble Space Telescope\n" + \
                 "Example: TWINS Mission => Two Wide-angle Imaging Neutral-atom Spectrometers Mission\n" + \
                 "Always extend the full name of acronyms from the entity's information. Remove every acronym when you know what they mean from the entity's information, else keep the acronym." + \
                 "If the entity is called 'Madrid', and is an observatory, but there is no information about its name, name it 'Madrid Observatory'." + \
                 "If there are two entities in one, use 'and' and cite their full names." + \
                 "If the Observatory is part of an University, ignore the name of the University if it is not also the name of the observatory." + \
                 "Entity to generate label from:" + entity_str

        prompt = f"""Task: Generate ONE label describing the entity.

Output format:
[aperture OR waveband if known] Full Entity Name [at/on Host Entity][, Country]

Rules:

1. Always expand acronyms if their meaning is known.
2. Do not keep acronyms in the final label if the full name is known.
3. If the meaning of an acronym is unknown, keep the acronym.
4. Use:
   - "on" if the entity is an instrument on a telescope or spacecraft.
   - "at" if the entity is located in an observatory.
5. If the entity is an observatory named only by a city (example: Madrid),
   output: "[City] Observatory".
6. If two entities are combined, join them with "and" and use their full names.
7. If an observatory belongs to a university, ignore the university name
   unless it is part of the observatory name.
8. Add aperture (example: 3.6m) or waveband (example: infrared) only if known.
9. If a telescope is named only by its aperture (example: "3.6m", "1.5 m"),
   output: "[aperture] Telescope".
   If an observatory is known, output: "[aperture] Telescope at [Observatory]".
10. If the country of the observatory or telescope is known with certainty,
   append ", Country" at the end of the label. If it is a base on Antarctica,
   append ", Antarctica" instead.
   Do not guess the country. If the country is unknown, do not add it.

Output only the label. Do not add explanations.

Examples:

Input:
Vysokaya Dubrava Magnetometer
is part of: V.dubr.-ground-observatory
location: Russia

Output:
Magnetometer at Vysokaya Dubrava Observatory, Russia

Input:
NICMOS instrument on Hubble Space Telescope
alt label: Near Infrared Camera and Multi-Object Spectrometer

Output:
Near Infrared Camera and Multi-Object Spectrometer on Hubble Space Telescope

Input:
Bernard Lyot Telescope
Aperture: 3.6m
Country: France
is part of: Midi-Pyrénées Observatory

Output:
3.6m Bernard Lyot Telescope at Midi-Pyrénées Observatory, France

Input:
Madrid
Country: Spain

Output:
Madrid Observatory, Spain

Input:
1.80m
is part of: La Silla Observatory

Output:
1.80m Telescope at La Silla Observatory

Input:
NASA's James Webb Telescope

Output:
James Webb Telescope

Entity:
{entity_str}
"""
        label = LLMConnection().generate(prompt,
                                         model = config.SUMMARIZE_MODEL,
                                         num_predict = 100,
                                         from_cache = True,
                                         cache_key = str(entity.uri) + ":label")
        print(entity.data)
        new_label = label.split("\n")[0].strip()
        old_labels = entity.get_values_for("label", return_language = True)
        for old_label in old_labels:
            lang = None
            if len(old_label) == 2:
                old_label, lang = old_label
                self._graph.add((uri, SKOS.altLabel, Literal(old_label, lang = lang)))
            print("old_label=", old_label, "lang=", lang)
        print("new label=", label)
        self._graph.remove((uri, SKOS.prefLabel, None))
        self._graph.add((uri, SKOS.prefLabel, Literal(new_label)))


    def _gen_definition(self, uri: URIRef) -> str:
        """
        Generate a short definition of the entity based on its attributes.

        Attrs:
            uri: the entity's URI
        """
        entity = Entity(uri)
        entity_str = entity.to_string(exclude = properties._LINKS + properties._EXT_REF + properties._METADATA + [properties.type, properties.latitude, properties.longitude, properties.notation])
        prompt = f"""Generate a short definition of this entity that summarizes its most important features (excluding its adress, alternate names, database identifiers, URLs and URIs, and lat/long information). Do not invent information.
No bullet points. Maximum 50 words, shorter if not enough information are displayed in this entity's description.

Input:
TRAPPIST-NORTH
also known as: Q2524921
is part of: TRAPPIST (TRAnsiting Planets and PlanetesImals Small Telescope project)
description: TRAPPIST-NORTH (Oukaimeden Observatory, Morocco) and TRAPPIST-SOUTH (La Silla Observatory, Chile) are twin telescopes operated by the Space Center of the University of Liège (Belgium).
location: Atlas Mountain (Morocco)
country: Morocco
continent: Africa
altitude: 2750m

Output:
Located in the Atlas Mountains at the Oukaimeden Observatory in Morocco, 2750 meters above sea level, the TRAPPIST-North telescope is a twin telescope of TRAPPIST-South located at the La Silla Observatory in Chile, both operated from Liège, Belgium, by the Space Center of the University of Liège.

Input:
Australia Observatory.
country: Australia.

Output:
Astronomical Observatory in Australia.

Input:

Entity to define and summarize: {entity_str}"""
        definition = LLMConnection().generate(prompt = prompt,
                                              model = config.SUMMARIZE_MODEL,
                                              num_predict = 100,
                                              from_cache = True,
                                              cache_key = entity.uri + ":definition")
        self._graph.add((uri, SKOS.definition, Literal(definition)))
        self._graph.remove((uri, SKOS.definition, None))
        self._graph.remove((uri, DCTERMS.description, None))
        self._graph.add((uri, SKOS.definition, Literal(definition)))


def main(input_graph: str):
    graph = Graph()
    graph.parse(input_graph)
    PostProcess(input_graph)()
    output_graph = input_graph.remove_prefix(".ttl") + "_post_processed.ttl"
    graph.serialize(destination = output_graph, format = "ttl")


if __name__ == "__main__":
    parser = ArgumentParser(prog = "post_process",
                            description = "Post process a merged ontology by summarizing definitions, heterogenize labels, moving fields to their proper place.")
    parser.add_argument("-i",
                        "--input-graph",
                        dest = "input_graph",
                        required = True,
                        help = "Merged graph (by the merge_uris script)")

    args = parser.parse_args()
    main(args.input_graph)

