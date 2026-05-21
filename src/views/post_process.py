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
from graph import entity_types
from collections import defaultdict
from utils.dict_utilities import _majority_vote_rounding
from utils.performances import timeall
import config
import json
import atexit
import re
properties = Properties()

class PostProcess():


    def __init__(self,
                 graph: Graph):
        self._graph = graph
        self._labels_by_uris = dict()


    def __iter__(self):
        """
        Iterate over URIs of entities in the graph by starting with all broaders (level 1) then go down the hierarchy.
        """
        # done = set()
        all_uri = set()
        uri_by_broader = defaultdict(list)
        for uri, _, broader in self._graph.triples((None, properties.is_part_of, None)):
            #if uri in done:
            #    continue
            print("broader=", broader, "uri=", uri)
            uri_by_broader[broader].append(uri)
            #yield uri
        for uri, _, _ in self._graph.triples((None, properties.type, None)):
            all_uri.add(uri)
            #if uri in done:
            #    continue
            #done.add(uri)
        print(uri_by_broader)
        broader_uris = uri_by_broader.keys() - sum(uri_by_broader.values(), [])
        print(len(broader_uris))

        narrower_level = defaultdict(lambda: defaultdict(list)) # {1: {broader: [narrowers1] of the next level},
                                                 #  2: {narrower1: [narrowers2], narrower1: [narrowers2]}}
        print("broader_uris=", broader_uris)
        for uri in broader_uris:
            narrower_level[1][uri] = uri_by_broader[uri]
            if uri in all_uri:
                yield uri
                all_uri.remove(uri)
        print("narrower level=", narrower_level)

        depth = 0
        while len(all_uri) > 0 and depth < 10:
            print("loop", depth)
            depth += 1

            if not narrower_level[depth]:
                print("Stop at depth", depth)
                break
            for _, uris in narrower_level[depth].items():
                for uri in uris:
                    narrower_level[depth + 1][uri] = uri_by_broader[uri]
                    if uri in all_uri:
                        yield uri
                        all_uri.remove(uri)

        # Finally, yield all entities that do not have narrower nor broader
        for uri in all_uri:
            yield uri


    @timeall
    def __call__(self):
        config.configure_ollama()
        atexit.register(self._save_label_warnings)
        i = 0
        for uri in self.__iter__():
            print("processing URI:", uri)
            self._remove_attrs(uri) # Remove attrs that are generated automatically to prevent generating errors
            self._gen_label(uri) # Must call before _gen_definition
            self._gen_definition(uri)
            i += 1
        print("total generated labels:", i)


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

    label_warnings = defaultdict(dict)

    def _add_label_warning(self,
                           entity_uri,
                           llm_generated_label,
                           warning_type,
                           warning_message):
        if type(entity_uri) == Entity:
            entity_uri = str(entity_uri.uri)
        self.label_warnings[entity_uri][warning_type] = {"message": warning_message,
                                                         "llm_generated_label": llm_generated_label
                                                        }

    def _save_label_warnings(self):
        with open(config.CACHE_DIR / "labels_warning.json", "w") as file:
            json.dump(self.label_warnings, file)


    def _check_observatory_format(self, entity: Entity,
                                  label: str) -> tuple[list[int], str]:
        """
        Observatory format must contain a comma and a country name.
        """
        country = entity.country
        for c in country:
            if not label.endswith(f", {c}"):
                print("Error no country match", c, label)
                self._add_label_warning(entity,
                                        label,
                                        warning_type = "missing_country",
                                        warning_message = f"Observatory's country ({c}) not found in label.")
                return [label + ", " + country]
        if not ', ' in label:
            self._add_label_warning(entity,
                                    label,
                                    warning_type = "missing_country",
                                    warning_message = f"Observatory (ground ?) with no country.")


    def _check_telescope_format(self, entity: Entity,
                                label: str,
                                entity_type: list,
                                called_from_narrower: bool = False) -> tuple[list[int], str]:
        """
        Telescope format should start with their aperture (in meter).
        If called from narrower (ex: instrument), the aperture can appear after.
        """
        r = r"^\d+(\.\d+)?m "
        if called_from_narrower:
            r = r[1:]
        if not re.match(re.compile(r), label):
            no_meter = re.findall(r"\d ?(cm|inches|inch|mm|km|foot|feet)", label, flags = re.IGNORECASE)
            for n in no_meter:
                self._add_label_warning(entity,
                                        label,
                                        warning_type = "aperture_format",
                                        warning_message = f"Aperture should be expressed in m with no space (ex: 4.33m), not {n}. Please convert it.")
                return []
            if entity.aperture:
                warning_message = f"Telescope's label does not start with the aperture."
            self._add_label_warning(entity,
                                    label,
                                    warning_type = "missing_aperture",
                                    warning_message = f"Telescope without aperture.")

        self._check_broader(entity,
                            label,
                            entity_type)


    """
    def _check_instrument(self,
                          entity: Entity,
                          label: str):
        self._check_broader(entity, label, [entity_types.Instrument])
        broaders = entity.get_values_for("is_part_of", extend_to_synonyms=True)

        for broader in broaders:
            if type(broader) == entity_types.Telescope:
                self._check_telescope_format(broader,
                                             label,
                                             broader.get_values_for("type"),
                                             called_from_narrower = True)
            if not " on " + broader.label in label:
                self._add_label_warning(entity,
                                        label,
                                        warning_type = "broader_mismatch",
                                        warning_message = f"Broader's label ({broader.label}) is not in the instrument's label with 'on'.")
    """


    def _check_broader(self,
                       entity: Entity,
                       label: str,
                       entity_type: list):
        """
        Verify that the entity needs to have a broader, and if so,
        verify that its broader's label is present in it.

        Args:
            entity: the entity to check
            label: the LLM generated label for this entity
            entity_type: list of the entity's types
        """
        broaders = entity.get_values_for("is_part_of", extend_to_synonyms=True)

        if broaders:
            matched_platform = False
            has_platform_as_broader = False
            for broader in broaders:
                broader_types = broader.get_values_for("type", extend_to_synonyms = True)
                for broader_type in broader_types:
                    if type(broader_type) == URIRef:
                        broader_type = entity_types.uri_to_type(broader_type)
                    if any (t == entity_types.Platform for t in broader.get_values_for("type")):
                        # Ignore mission
                        if entity_types.INVESTIGATION in broader.get_values_for("type"):
                            continue
                        #if entity_types.GROUND_OBSERVATORY in broader.get_values_for("type"):
                        has_platform_as_broader = True
                        if " at " + broader.label in label: # and not matched_platform:
                            matched_platform = True
                            broader_str = " at " + broader.label
                            break
                        elif " on " + broader.label in label:
                            matched_platform = True
                            broader_str = " on " + broader.label
                            break

            if has_platform_as_broader and not matched_platform:
                # Observatory not referenced in the label of narrower entity
                self._add_label_warning(entity, label, warning_type = "broader_ref_missing", warning_message = "This entity seems to be located on a broader entity, but it is missing.")
        else:
            if not entity_types.HAS_NO_BROADER.intersection(entity_type):
                # The entity should have a broader, but it does not have any.
                self._add_label_warning(entity, label, warning_type = "orphan_entity", warning_message = f"This entity seems to be an orphan (no broader), but its type ({entity_type}) indicates that it might be part of a broader entity.")



        # Check that the entity's broader label is in the entity.
        # /!\ not working for spacecraft that are part of an investigations.
        for b in broaders:
            broader_label = b.label
            broader_type = b.get_values_for("type")
            if broader_label not in label:
                self._add_label_warning(entity,
                                        label,
                                        warning_type = "broader_missing",
                                        warning_message = f"Broader entity ({broader_label}) is not in the label.")
            preposition = set()
            for b_type in broader_type:
                if type(b_type) == URIRef:
                    b_type = entity_types.uri_to_type(b_type)
                if issubclass(b_type, entity_types.GroundFacility):
                    preposition.add(" at ")
                if issubclass(b_type, entity_types.SpaceFacility):
                    preposition.add(" on ")
            broader_regex = "(" + "|".join(preposition) + ")"
            broader_format_ok = re.findall(re.compile(broader_regex), label)
            if not broader_format_ok:
                self._add_label_warning(entity,
                                        label,
                                        warning_type = "broader_format",
                                        warning_message = f"Broader format should be ' at ' (ground observatory) or ' on ' (telescope, spacecraft)")

    def _check_country(self,
                       entity: Entity,
                       label: str,
                       entity_type: list):
        
        matched_country = False
        country_match = re.findall(r", [A-Za-z]+( [A-Za-z]+){0,3}$", label)
        if country_match:
            matched_country = True
        country = entity.country
        if country:
            if not label.endswith(", " + country):
                for ent_type in entity_type:
                    if issubclass(ent_type, entity_types.GroundFacility):
                        self._add_label_warning(entity, label, warning_type = "country_missing", warning_message = f"Country ({country}) is known but does not exist in the label")
                        country_str = ", " + country
                        break
            elif not country in country_match:
                self._add_label_warning(entity, label, warning_type = "country_mismatch", warning_message = f"Country in the entity's metadata ({country}) does not match the label's country ({country_match})")
        if not country_match:
            for ent_type in entity_type:
                if issubclass(ent_type, entity_types.GroundFacility) and ent_type in entity_types.HAS_NO_BROADER:
                    self._add_label_warning(entity, label, warning_type = "country_missing", warning_message = f"Country not in the entity, therefore it could not be added to the entity's label.")

    def _check_acronyms(self,
                        entity: Entity,
                        label: str):
        """
        Try to detect remaining acronyms in the label.

        Args:
            entity: the entity to check
            label: the LLM generated label for this entity
        """
        upper_word = ""
        for l in label:
            if l.isalpha() and l.upper() == l:
                upper_word += l
            else:
                if len(upper_word) > 1 and len(upper_word) < 5:
                    self._add_label_warning(entity, label, warning_type = "acronym_detected", warning_message = f"An acronym may be in this entity label: {upper_word}. Ignore if this acronym should not be extended.")
                upper_word = ""


    def _check_agencies(self,
                        uri: URIRef,
                        label: str):
        """
        Check whether the agency name (acronym or extended label) appears
        in the label.
        """
        agencies = [
                    "US Department of Defense", "DOD",
                    "Canadian Space Agency", "CSA",
                    "National Aeronautics and Space Administration", "NASA",
                    "Japan Aerospace Exploration Agency", "JAXA",
                    "Italian Space Agency", "ASI",
                    "European Space Agency", "ESA",
                    "National Astronomical Observatory of Japan", "NAOJ",
                    "Institute of Space and Astronautical Science", "ISAS",
                    "French Space Agency", "CNES",
                    "German Aerospace Center", "DLR",
                    "US Department of Energy", "DOE",
                    "US National Science Foundation", "NSF"
                   ]
        agency_regex = r"\b(" + '|'.join(agencies) + ")\b"
        agencies = re.findall(agency_regex, label)
        if agencies:
            print(agencies)


    def _check_llm_label(self,
                         uri: URIRef | Entity,
                         llm_generated_label: str) -> list[str]:
        """
        Check the format of the label generated with the LLM. If it does not match rules, it
        will print a warning and save this warning in a warning file.

        Args:
            uri: the entity's URI
            llm_generated_label: label generated by LLM that will be checked with rules.
        Returns:
            int: all status
            str: a new recommanded label following the rules (when possible)
        """

        """
        error_status = []
        # Error status that can still recommand a label: 1
        # Error status that make a label recommandation impossible: 2
        recommanded_label = llm_generated_label

        country_str = ""
        apeture_str = ""
        broader_str = ""

        if type(uri) == URIRef:
            entity = Entity(uri)
        elif type(uri) == Entity:
            entity = uri
        else:
            raise TypeError(f"uri should be an instance of URIRef or Entity. Got {type(uri)} ({uri})")
        entity_type = entity.get_values_for("type")
        print(entity_type)
        print(entity_types.ALL_TYPES)
        entity_type = [entity_types.ALL_TYPES[str(t)] for t in entity_type]

        aperture = entity.get_values_for("aperture")
        if aperture:
            matched = False
            for ap in aperture:
                if llm_generated_label.startswith(ap):
                    matched = True
            no_meter = re.findall(r"\d\b?(cm|inch|mm|km)", llm_generated_label)
            if no_meter:
                for unit in no_meter:
                    error_status.append(2)
                    self._add_label_warning(uri, llm_generated_label, warning_type = "malformated_aperture", warning_message = f"Aperture should be converted to meters. Ex: 2.42m. Got {unit}")
            if not matched:
                print("Not matched")
                error_status.append(1)
                self._add_label_warning(uri, llm_generated_label, warning_type = "aperture_known", warning_message = "Missing aperture while known")
                best_aperture = _majority_vote_rounding(aperture)
                best_aperture = str(best_aperture) + "m"
                recommanded_label = best_aperture + " " + recommanded_label
        else:
            if entity_types.Telescope in entity_type:
                error_status.append(2)
                self._add_label_warning(uri, llm_generated_label, warning_type = "aperture_unknown", warning_message = "Found a telescope with no aperture in its metadata")
        broaders = entity.get_values_for("is_part_of", extend_to_synonyms=True)

        if broaders:
            matched_platform = False
            has_platform_as_broader = False
            matched_broader_country = False
            for broader in broaders:
                broader = Entity(broader)
                if any (entity_types.ALL_TYPES[t] in entity_types.Platform for t in broader.get_values_for("type")):
                    # Ignore mission
                    if entity_types.INVESTIGATION in broader.get_values_for("type"):
                        continue
                    #if entity_types.GROUND_OBSERVATORY in broader.get_values_for("type"):
                    has_platform_as_broader = True
                    if " at " + broader.label in llm_generated_label: # and not matched_platform:
                        matched_platform = True
                        broader_str = " at " + broader.label
                        break
                    elif " on " + broader.label in llm_generated_label:
                        matched_platform = True
                        broader_str = " on " + broader.label
                        break
                    # + verify that there is country
                    #if ", " + broader.country in llm_generated_label and not matched_broader_country:
                    #    matched_broader_country = True
                        # Problem: Spain for Iaga

            if has_platform_as_broader and not matched_platform:
                # Observatory not referenced in the label of narrower entity
                self._add_label_warning(uri, llm_generated_label, warning_type = "broader_ref_missing", warning_message = "This entity seems to be located on a broader entity, but it is missing.")
        else:
            print(entity._data)
            if not entity_types.HAS_NO_BROADER.intersection(entity_type):
                # The entity should have a broader, but it does not have any.
                self._add_label_warning(uri, llm_generated_label, warning_type = "orphan_entity", warning_message = f"This entity seems to be an orphan (no broader), but its type ({entity_type}) indicates that it might be part of a broader entity.")

        matched_country = False
        country_match = re.findall(r", [A-Za-z]+( [A-Za-z]+){0,3}$", llm_generated_label)
        if country_match:
            matched_country = True
        country = entity.country
        if country:
            if not llm_generated_label.endswith(", " + country):
                for ent_type in entity_type:
                    if issubclass(ent_type, entity_types.GroundFacility):
                        self._add_label_warning(uri, llm_generated_label, warning_type = "country_missing", warning_message = f"Country ({country}) is known but does not exist in the label")
                        country_str = ", " + country
                        break
            elif not country in country_match:
                self._add_label_warning(uri, llm_generated_label, warning_type = "country_mismatch", warning_message = f"Country in the entity's metadata ({country}) does not match the label's country ({country_match})")
        if not country_match:
            for ent_type in entity_type:
                if issubclass(ent_type, entity_types.GroundFacility) and ent_type in entity_types.HAS_NO_BROADER:
                    self._add_label_warning(uri, llm_generated_label, warning_type = "country_missing", warning_message = f"Country not in the entity, therefore it could not be added to the entity's label.")

        return error_status, recommanded_label
        """

        if type(uri) == URIRef:
            entity = Entity(uri)
        elif type(uri) == Entity:
            entity = uri
        else:
            raise TypeError(f"uri should be an instance of URIRef or Entity. Got {type(uri)} ({uri})")

        # Verify that for every kind of entity, there are no acronyms
        self._check_agencies(entity, llm_generated_label)
        return
        self._check_acronyms(entity, llm_generated_label)

        entity_type = entity.get_values_for("type", extend_to_synonyms = True)
        entity_type = [entity_types.ALL_TYPES[str(t)] for t in entity_type]

        if entity_types.Telescope in entity_type:
            # Telescope
            self._check_telescope_format(entity,
                                         llm_generated_label,
                                         entity_type)
        elif entity_types.GroundObservatory in entity_type:
            # Ground obsevatory
            self._check_country(entity,
                                llm_generated_label,
                                entity_type)
        elif entity_types.Instrument in entity_type:
            # Instrument
            self._check_broader(entity,
                                llm_generated_label,
                                entity_type)
        elif entity_types.Airborne in entity_type:
            # Airborne
            pass # TODO
        elif entity_types.Investigation in entity_type:
            # Investigation
            pass # TODO
        elif entity_types.Spacecraft in entity_type:
            # Spacecraft
            pass # TODO
        elif entity_types.SpaceFacility in entity_type:
            # Space facility
            pass # TODO
        elif entity_types.GroundFacility in entity_type:
            # Ground facility
            pass # TODO
        elif entity_types.Platform in entity_type:
            # Platform
            pass # TODO
        elif entity_types.Survey in entity_type:
            # Survey
            pass # TODO
        return set(self.label_warnings.get(str(entity.uri), {}))


    def _gen_label(self, uri: URIRef):
        """
        Generate a label for the entity based on its attributes.

        Args:
            uri: the entity's URI
        """
        entity = Entity(uri)
        entity_str = entity.to_string(exclude = properties._LINKS + properties._EXT_REF + properties._METADATA + [properties.has_part],
                                      get_label_of_uris = True)
        prompt = "Generate one label following this format: " + \
                 "aperture (if known) or waveband (if known) label (if known, no acronym if possible) at/on broader_label(ex:spacecraft carrying this entity, or observatory, only if it is an entity within an observatory, a telescope or a spacecraft.)" + \
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
   - "part of" if the entity is part of a mission or investigation.
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

