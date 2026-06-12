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
from rdflib import URIRef, Literal, SKOS, DCTERMS
from graph.graph import Graph
from graph.entity import Entity
from graph.properties import Properties
from llm.llm_connection import LLMConnection
from graph import entity_types
from collections import defaultdict
from utils.dict_utilities import majority_voting_merge
from utils.performances import timeall
from utils.string_utilities import standardize_uri
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
            uri_by_broader[broader].append(uri)
            #yield uri
        for uri, _, _ in self._graph.triples((None, properties.type, None)):
            all_uri.add(uri)
            #if uri in done:
            #    continue
            #done.add(uri)
        broader_uris = uri_by_broader.keys() - sum(uri_by_broader.values(), [])

        narrower_level = defaultdict(lambda: defaultdict(list)) # {1: {broader: [narrowers1] of the next level},
                                                 #  2: {narrower1: [narrowers2], narrower1: [narrowers2]}}
        for uri in broader_uris:
            narrower_level[1][uri] = uri_by_broader[uri]
            if uri in all_uri:
                yield uri
                all_uri.remove(uri)

        depth = 0
        while len(all_uri) > 0 and depth < 10:
            depth += 1

            if not narrower_level[depth]:
                break
            for _, uris in narrower_level[depth].items():
                # Labels should be generated after their broader's label.
                for uri in uris:
                    narrower_level[depth + 1][uri] = uri_by_broader[uri]
                    if uri in all_uri:
                        yield uri
                        all_uri.remove(uri)

        # Finally, yield all entities that do not have narrower nor broader
        for uri in all_uri:
            yield uri

    uris_by_label = dict()

    @timeall
    def __call__(self):
        config.configure_ollama()
        atexit.register(self._save_label_warnings)
        i = 0
        in_scope = set()
        for uri in self.__iter__():
            self._remove_attrs_before_gen(uri) # Remove attrs that are generated automatically to prevent generating errors
            self._gen_label(uri) # Must call before _gen_definition
            self._gen_definition(uri)
            self._remove_attrs_after_gen(uri)
            in_scope.add(uri)
            i += 1
        self._remove_out_of_scope_references(in_scope)
        self.replace_uri()


    def _remove_attrs_before_gen(self, uri: URIRef):
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


    def _remove_attrs_after_gen(self, uri: URIRef):
        entity = Entity(uri)
        TO_REMOVE = ["address",
                     "source_type",
                     "location_confidence",
                     "type_confidence",
                     "continent_code",
                     #"source",
                     "discipline",
                     #"funding_agency",
                     #"waveband",
                     #"observed_object",
                     #"code", #ext id of resources
                     ]
        for attr in TO_REMOVE:
            entity.remove_values(attr)


    def _remove_out_of_scope_references(self,
                                        in_scope: set):
        """
        Remove values hasPart & isPartOf for referenced entities that are not included in this view (community view)

        Attrs:
            in_scope: entities that are in scope
        """
        relations = [properties.has_part, properties.is_part_of]
        for relation in relations:
            relation = properties.convert_attr(relation)
            for uri1, _, uri2 in self._graph.triples((None, relation, None)):
                # if str(uri1).startswith(str(properties.OBS)[:-1]) or
                if str(uri2).startswith(str(properties.OBS)[:-1]) and uri2 not in in_scope:
                    self._graph.remove((uri1, relation, uri2))

                    #for uri in (uri1, uri2):
                    entity = Entity(uri1)
                    for value in entity.get_values_for(relation).copy():
                        # if str(value).startswith(str(properties.OBS)[:-1]):
                        if value == uri2:
                            # Old namespace
                            entity.remove_value(relation, value)


    label_warnings = defaultdict(dict)

    def _add_label_warning(self,
                           entity_uri,
                           llm_generated_label,
                           warning_type,
                           warning_message,
                           recommanded_action):
        if type(entity_uri) == Entity:
            entity_uri = str(entity_uri.uri)
        self.label_warnings[entity_uri][warning_type] = {"message": warning_message,
                                                         "llm_generated_label": llm_generated_label,
                                                         "recommanded_action": recommanded_action
                                                        }

    def _save_label_warnings(self, filename = config.CACHE_DIR / "labels_warning.json"):
        with open(filename, "w") as file:
            json.dump(self.label_warnings, file, indent = 2)


    def _check_observatory_format(self, entity: Entity,
                                  label: str) -> tuple[list[int], str]:
        """
        Observatory format must contain a comma and a country name.
        """
        country = entity.country
        for c in country:
            if not label.endswith(f", {c}"):
                self._add_label_warning(entity,
                                        label,
                                        warning_type = "missing_country",
                                        warning_message = f"Observatory's country ({c}) not found in label.",
                                        recommanded_action = f"add ',{c}")
                return [label + ", " + country]
        if not ', ' in label:
            self._add_label_warning(entity,
                                    label,
                                    warning_type = "missing_country",
                                    warning_message = f"Observatory (ground ?) with no country.",
                                    recommanded_action = f"Check this entity's country and add it to its metadata and label.")


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
                                        warning_message = f"Aperture should be expressed in m with no space (ex: 4.33m), not {n}. Please convert it.",
                                        recommanded_action = f"Convert {n} to meters in the label (and aperture if not in meter).")
                return []
            if entity.aperture:
                warning_message = f"Telescope's label does not start with the aperture."
            aperture = entity.get_values_for("aperture", unique = True)
            if aperture:
                recommanded_action = f"Add {aperture}m in front of the entity's label (check whether this value is in meters)"
            else:
                recommanded_action = f"Check why this telescope has no aperture."
            self._add_label_warning(entity,
                                    label,
                                    warning_type = "aperture_missing",
                                    warning_message = f"Telescope without aperture.",
                                    recommanded_action = recommanded_action)

        self._check_broader(entity,
                            label,
                            entity_type)


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
        broaders = [Entity(broader) if type(broader) != Entity else broader for broader in broaders]

        if broaders:
            matched_platform = False
            has_platform_as_broader = False
            broader_str = ""
            for broader in broaders:
                broader_types = broader.get_values_for("type", extend_to_synonyms = True)
                for broader_type in broader_types:
                    if type(broader_type) == URIRef:
                        broader_type = entity_types.uri_to_type(broader_type)
                    if issubclass(broader_type, entity_types.Platform):
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
                self._add_label_warning(entity,
                                        label,
                                        warning_type = "broader_missing",
                                        warning_message = "This entity seems to be located on a broader entity, but it is missing in the label.",
                                        recommanded_action = f"Add 'at/on {broader.label}' in the label.")
        else:
            if not entity_types.HAS_NO_BROADER.intersection(entity_type):
                # The entity should have a broader, but it does not have any.
                self._add_label_warning(entity,
                                        label,
                                        warning_type = "orphan_entity",
                                        warning_message = f"This entity seems to be an orphan (no broader), but its type ({entity_type}) indicates that it might be part of a broader entity.",
                                        recommanded_action = f"Check that this is correct.")


    def _check_country(self,
                       entity: Entity,
                       label: str,
                       entity_type: list):
        matched_country = False
        country_match = re.findall(r", ([A-Za-z]+(?: [A-Za-z]+){0,3})$", label)
        if country_match:
            matched_country = True
        country = entity.country
        if country:
            if ',' in country:
                country = country.split(',')[0].strip() # Taiwan, Province of China & Korea, Republic of & Bolivia, Plurinational State of & Venezuela, Bolivarian Republic of
                if country == "Korea":
                    country = "South Korea"
            if not label.endswith(", " + country):
                for ent_type in entity_type:
                    if issubclass(ent_type, entity_types.GroundFacility):
                        self._add_label_warning(entity,
                                                label,
                                                warning_type = "country_mismatch",
                                                warning_message = f"Country ({country}) in the entity's metadata does not match the label's country ({','.join(country_match)}) or is malformated",
                                                recommanded_action = f"Make sure to end the entity by ', {country}'")
                        break
        else: # if not country_match:
            # No country in entity
            for ent_type in entity_type:
                if issubclass(ent_type, entity_types.GroundFacility) and ent_type in entity_types.HAS_NO_BROADER:
                    self._add_label_warning(entity,
                                            label,
                                            warning_type = "country_missing",
                                            warning_message = f"Country not in the entity, therefore it could not be added to the entity's label.",
                                            recommanded_action = f"Check for the entity's country if it is a ground observatory and add it to its label and metadata.")

    AGENCIES = [
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
        for l in label + ' ':
            if l.isalpha() and l.upper() == l:
                upper_word += l
            else:
                if len(upper_word) > 1 and len(upper_word) < 6:
                    if upper_word not in self.AGENCIES:
                        self._add_label_warning(entity,
                                                label,
                                                warning_type = "acronym_detected",
                                                warning_message = f"An acronym remains this entity's label: {upper_word}.",
                                                recommanded_action = f"Expand the acronym or ignore if this acronym should not be expanded.")
                upper_word = ""


    def _check_agencies(self,
                        entity: Entity,
                        label: str):
        """
        Check whether the agency name (acronym or extended label) appears
        in the label.
        """
        agency_regex = r"\b(" + '|'.join(self.AGENCIES) + r")\b"
        agencies = re.findall(agency_regex, label)

        broader = entity.broader
        if broader:
            broader_label = broader.label
        else:
            broader_label = ""
        for agency in agencies:
            # if in broader, ignore
            index = self.AGENCIES.index(agency)
            if index % 2 == 0:
                index += 1
            else:
                index -= 1
            agency_alt_name = self.AGENCIES[index]
            if agency in broader_label or agency_alt_name in broader_label:
                pass
            else:
                self._add_label_warning(entity,
                                        label,
                                        warning_type = "agency_in_label",
                                        warning_message = f"Agency name should preferably not be in the label: {agency}, unless it is necessary.",
                                        recommanded_action = f"Remove {agency} from the label, unless it will create ambiguity with another entity's label.")


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
        if type(uri) == URIRef:
            entity = Entity(uri)
        elif type(uri) == Entity:
            entity = uri
        else:
            raise TypeError(f"uri should be an instance of URIRef or Entity. Got {type(uri)} ({uri})")


        if llm_generated_label in self.uris_by_label:
            uri2 = self.uris_by_label[llm_generated_label]
            self._add_label_warning(entity_uri = uri,
                                    llm_generated_label = llm_generated_label,
                                    warning_type = "duplicate_label",
                                    warning_message = f"Duplicate label between '{uri}' and '{uri2}.",# They will be merged into '{uri2}' at this step.",
                                    recommanded_action = "Verify that both entities are the same.")
            # majority_voting_merge(dicts = [Entity(uri2).data, Entity(uri).data])
            # TODO merge them in further steps, and change their reference to the new URI in other entities too.
        else:
            self.uris_by_label[llm_generated_label] = uri

        # Verify that for every kind of entity, there are no acronyms
        self._check_agencies(entity, llm_generated_label)
        self._check_acronyms(entity, llm_generated_label)

        entity_type = entity.get_values_for("type", extend_to_synonyms = True)
        entity_type = [entity_types.uri_to_type(t) for t in entity_type]

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


    def _gen_label(self, uri: URIRef) -> str:
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
        self._graph.remove((uri, SKOS.prefLabel, None))
        self._graph.add((uri, SKOS.prefLabel, Literal(new_label)))
        self._check_llm_label(uri, label)


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
                                              cache_key = str(entity.uri) + ":definition")
        self._graph.add((uri, SKOS.definition, Literal(definition)))
        self._graph.remove((uri, SKOS.definition, None))
        self._graph.remove((uri, DCTERMS.description, None))
        self._graph.add((uri, SKOS.definition, Literal(definition)))


    def replace_uri(self):
        """
        Replace URIs by newly generated labels.
        """
        uri_by_label = dict()
        replaced = dict() # To replace self links
        for uri, _, label in self._graph.triples((None, SKOS.prefLabel, None)):
            label = standardize_uri(label)
            label = properties.OBS[label] # Use the old namespace at that point (obsf)
            if label not in uri_by_label:
                uri_by_label[label] = uri
            else:
                uri2 = uri_by_label[label]
                print(f"Warning: merging {uri} into {uri2} as they have the same label: {label}.")
                majority_voting_merge([Entity(uri2).data, Entity(uri).data])
                self._graph.remove((uri, None, None))
            replaced[uri] = label
        for subj, pred, obj in self._graph.triples((None, None, None)):
            new_subj, new_obj = subj, obj
            if subj in replaced:
                new_subj = replaced[subj]
            if obj in replaced:
                new_obj = replaced[obj]
            if new_subj != subj or new_obj != obj:
                self._graph.remove((subj, pred, obj))
                self._graph.add((new_subj, pred, new_obj))


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

