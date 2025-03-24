"""
WikidataExtractor collects the WD entities and stores data into a dictionary.
The output dictionary is compatible with the ontology mapping (see graph.py).

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""


from datetime import datetime
from datetime import UTC
import json
import os
import ssl
import sys

from SPARQLWrapper import SPARQLWrapper, JSON
from extractor.cache import VersionManager, CacheManager
from extractor.extractor import Extractor
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from pathlib import Path

import certifi
import urllib


class WikidataExtractor(Extractor):
    URL = "https://wikidata.org/wiki/" # + Q123

    # URI to save this source as an entity (obs:wikidata_list)
    URI = "wikidata_list"

    # URI to save entities from this source
    NAMESPACE = "wikidata"

    # Folder name to save cache/ and data/
    CACHE = "Wikidata/"

    # If is ontological, all superclasses will use the wikidata namespace
    IS_ONTOLOGICAL = True

    # Default type used for all unknown types in this resource
    DEFAULT_TYPE = "observation facility"

    # Enpoint for requesting wikidata
    _ENDPOINT_URL = "https://query.wikidata.org/sparql"

    # File to save the controls for wikidata entities
    _CONTROL_FILE = "wikidata_entities_control_file_latest.json"

    # We should update this per user:
    _USER_AGENT = "semantics@ivoa.net - PADC/Observatoire de Paris - Python/%s.%s" % (
        sys.version_info[0], sys.version_info[1])

    _QUERY_PREFIX = """
    PREFIX schema: <http://schema.org/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    """

    _SELECT_COUNT = """
    SELECT
        ( COUNT ( DISTINCT ?itemURI ) as ?count )
    """

    _SELECT_MAIN_SIMPLE = """
    SELECT DISTINCT
    ?itemURI
    ?itemLabel
    ?modifiedDate
    """

    _WHERE_SIMPLE = """
    WHERE
    {
    ?itemURI rdfs:label ?itemLabel filter (lang(?itemLabel) = "en") . # get itemLabel only for lang = @en
    ?itemURI schema:dateModified ?modifiedDate . # get last modification date

    # Filter on classes:
    {?itemURI wdt:P31/wdt:P279* wd:Q40218 .} # spacecraft
    UNION {?itemURI wdt:P31/wdt:P279* wd:Q5916 .} # spaceflight
    UNION {?itemURI wdt:P31/wdt:P279* wd:Q62832 .} # observatory

    # Filter out unwanted classes:
    MINUS { ?itemURI wdt:P31 wd:Q752783. }  # human spaceflight
    MINUS { ?itemURI wdt:P31 wd:Q209363. }  # weather satellite
    MINUS { ?itemURI wdt:P31 wd:Q149918. }  # communications satellite
    MINUS { ?itemURI wdt:P31 wd:Q113255208. }  # spacecraft series
    MINUS { ?itemURI wdt:P31 wd:Q209363. }  # weather satellite
    MINUS { ?itemURI wdt:P31 wd:Q466421. }  # reconnaissance satellite
    MINUS { ?itemURI wdt:P31 wd:Q2741214. }  # KH-7 Gambit
    MINUS { ?itemURI wdt:P31 wd:Q973887. }  # military satellite
    MINUS { ?itemURI wdt:P31 wd:Q512399. }  # unmanned spaceflight
    MINUS { ?itemURI wdt:P31 wd:Q61937849. }  # geophysical observatory
    MINUS { ?itemURI wdt:P31 wd:Q1365207. }  # bird observatory
    MINUS { ?itemURI wdt:P31 wd:Q95945728. }  # technology demonstration spacecraft
    MINUS { ?itemURI wdt:P31 wd:Q2566071. }  # manned weather station
    MINUS { ?itemURI wdt:P31 wd:Q1009523. }  # Automated Transfer Vehicle
    MINUS { ?itemURI wdt:P31 wd:Q14514346. }  # satellite program
    MINUS { ?itemURI wdt:P31 wd:Q7572593. }  # space launch
    MINUS { ?itemURI wdt:P31 wd:Q153257. }  # Automated Transfer Vehicle
    MINUS { ?itemURI wdt:P31 wd:Q109743523. }  # Cargo Dragon
    MINUS { ?itemURI wdt:P31 wd:Q236448. }  # Dragon
    MINUS { ?itemURI wdt:P31 wd:Q105095031. } # Crew Dragon
    MINUS { ?itemURI wdt:P31 wd:Q18812508. }  # space station module
    MINUS { ?itemURI wdt:P31 wd:Q117384805. }  # spacecraft family
    MINUS { ?itemURI wdt:P31 wd:Q190107. }  # weather station
    MINUS { ?itemURI wdt:P31 wd:Q127899. }  # Multi-Purpose Logistics Module
    MINUS { ?itemURI wdt:P31 wd:Q117384800. }  # spacecraft model
    MINUS { ?itemURI wdt:P31 wd:Q1778118. }  # volcano observatory
    MINUS { ?itemURI wdt:P31 wd:Q110218336. }  # atmospheric observatory
    MINUS { ?itemURI wdt:P31 wd:Q7865636. }  # seismological station
    MINUS { ?itemURI wdt:P31 wd:Q4538275. }  # Yantar-4K2
    MINUS { ?itemURI wdt:P31 wd:Q7103282. }  # Orlets
    MINUS { ?itemURI wdt:P31 wd:Q1812673. }  # US-KMO
    MINUS { ?itemURI wdt:P31 wd:Q147802. }  # Kosmos
    MINUS { ?itemURI wdt:P31 wd:Q14907192. }  # Tsikada
    MINUS { ?itemURI wdt:P31 wd:Q300807. }  # DS-U3-S
    MINUS { ?itemURI wdt:P31 wd:Q14752541. }  # Molniya-1
    MINUS { ?itemURI wdt:P31 wd:Q18201623. }  # Expedition to the ISS
    MINUS { ?itemURI wdt:P31 wd:Q7248527. }  # Progress-M
    MINUS { ?itemURI wdt:P31 wd:Q7800236. }  # Tianlian I
    MINUS { ?itemURI wdt:P31 ?x. ?x wdt:P361 wd:Q4303731. }  # Sentinel program
    MINUS { ?itemURI wdt:P361 wd:Q4303731. }  # Sentinel program
    MINUS { ?itemURI wdt:P31 wd:Q127924. }  # Cygnus
    MINUS { ?itemURI wdt:P31 wd:Q1024445. }  # Boeing Starliner
    MINUS { ?itemURI wdt:P31 wd:Q109358230. }  # proposed commercial space station
    MINUS { ?itemURI wdt:P31 wd:Q402330. }  # cargo spacecraft
    MINUS { ?itemURI wdt:P31 wd:Q28803027. }  # Nusat
    MINUS { ?itemURI wdt:P361 wd:Q750900. }  # SPOT
    MINUS { ?itemURI wdt:P31 wd:Q7248542. }  # Progress 7K-TG
    MINUS { ?itemURI wdt:P31 wd:Q5514330. }  # GPS block III
    MINUS { ?itemURI wdt:P31 wd:Q1956962. }  # Soyouz 7K-OK
    MINUS { ?itemURI wdt:P361 wd:Q48750500. }  # Pleiades Neo
    MINUS { ?itemURI wdt:P31 wd:Q819651. }  # MetOp
    MINUS { ?itemURI wdt:P31 wd:Q53214907. }  # Falcon 9 booster
    MINUS { ?itemURI wdt:P31 wd:Q107452222. }  # Mandrake 2
    MINUS { ?itemURI wdt:P31 wd:Q11150949. }  # Haiyang-2
    MINUS { ?itemURI wdt:P31 wd:Q256812. }  # Corona
    MINUS { ?itemURI wdt:P31 wd:Q15927648. }  # FORMOSAT-7
    MINUS { ?itemURI wdt:P31 wd:Q1318674. }  # testbed
    MINUS { ?itemURI wdt:P31 wd:Q56650652. }  # Starship
    MINUS { ?itemURI wdt:P31 wd:Q455647. }  # amateur radio satellite
    MINUS { ?itemURI wdt:P279 wd:Q1277959. }  # satellite flare
    MINUS { ?itemURI wdt:P31 wd:Q211727. }  # Orion rentry capsule
    MINUS { ?itemURI wdt:P361 wd:Q1109103. }  # RADARSAT constellation
    MINUS { ?itemURI wdt:P361 wd:Q589786. }  # Yaogan satellites
    MINUS { ?itemURI schema:description ?desc. ?itemURI wdt:P4839 ?wolfstr. FILTER CONTAINS (?wolfstr, ?desc) }  # remove bare records from Geek surine
    }
    """

    # Query to get control pages
    _QUERY_CONTROL = _QUERY_PREFIX + _SELECT_MAIN_SIMPLE + _WHERE_SIMPLE

    def extract(self) -> dict:
        """
        Extract wikidata content into a dictionary.
        """
        print("Extracting wikidata entities...")
        controls = self._get_controls()

        # Remove manually excluded entities from the exclusion file.
        self._apply_exclusions(controls)

        print(f"Found {len(controls["results"])} entities.")

        # get newer versions' Wikidata URIs
        latest = VersionManager.get_newer_keys(last_version_file = self._CONTROL_FILE,
                                               new_version = controls,
                                               list_name = self.CACHE)

        # TODO save those files in data/ or cache/

        # Dictionary to save entities that were succesfully downloaded & saved
        result = dict()
        print("Ready to update", len(latest), "entities.")
        for wikidata_uri in tqdm(latest):
            data = self._extract_entity(wikidata_uri, result)
            if data:
                # Downloaded page successfully.
                # Refresh the version at each loop to keep track of what
                # has worked in case of crash.
                VersionManager.refresh(last_version_file = self._CONTROL_FILE,
                                       new_version = {wikidata_uri: controls["results"][wikidata_uri]},
                                       list_name = self.CACHE)
                result[data["label"]] = data

        # Also get versions that were not refreshed
        older = controls["results"].keys() - latest
        print("No need to update", len(older), "entities.")

        """
        for wikidata_uri in tqdm(older):
            data = self._extract_entity(wikidata_uri)
            results[data["label"]] = data
        return results
        """
        # Paralellize entity extraction if the cache already has .json files
        # for most wikidata_uri.
        # FIXME this may crash if there is a control_file_latest.json
        # but there are no cached json files (too many requests). It should not
        # be executed with a multithread in this case.
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(self._extract_entity, wikidata_uri, result):
                    wikidata_uri for wikidata_uri in older}
            for future in tqdm(as_completed(futures), total = len(futures)):
                data = future.result()
                result[data["label"]] = data
        return result

    def _get_results(self,
                     query: str) -> dict:
        """
        Send a request to the enpoint url.

        Keyword arguments:
        query -- the SPARQL query
        """
        sparql = SPARQLWrapper(self._ENDPOINT_URL,
                               agent = self._USER_AGENT)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON) # TODO another format ?
        # Use certifi certificates
        context = ssl.create_default_context(cafile = certifi.where())
        sparql.urlopener = lambda request: urllib.request.urlopen(request, context = context)
        return sparql.query().convert()


    def _apply_exclusions(self,
                          control_data: dict):
        """
        Load the exclusion json file and remove its entities
        from the control data.

        Keyword arguments:
        control_data -- the wikidata control dictionary {uri: control_data}
        """
        exclusion_file = str(Path(__file__).parent.parent.parent.parent / "data" / self.CACHE / "wikidata_excluded_entities.json")
        if os.path.exists(exclusion_file):
            with open(exclusion_file, 'r', encoding='utf-8') as file:
                exclusion_data = json.load(file)
        else:
            exclusion_data = dict()

        entities = control_data["results"]

        control_data["results"] = {key: value for key, value in entities.items()
                                   if key in set(entities)- set(exclusion_data)}


    def _properties_to_dict(self,
                            entity_response: str,
                            result: dict) -> dict:
        """
        Transform the response of a request for a Wikidata entity
        into a dictionary compatible with the ontology merger.

        Some relations point to a Wikidata URI that can be resolved and added
        to the result dict.
        (like: sub_class_of, has_part, is_part_of, unit)

        Keyword arguments:
        entity_response -- json string of the wikidata response of an entity
        result -- dictionary of all data.
        """
        if not entity_response:
            return dict()
        entities = json.loads(entity_response)
        entities = entities["entities"]

        data = dict()
        for key, value in entities.items():
            code = key
            main_label = ""

            # Label & alt labels
            labels = value["labels"]
            alt_labels = set()
            for language in labels.values():
                label = language["value"]
                lan = language["language"]
                if lan == "en":
                    main_label = label
                else:
                    alt_labels.add(label + '@' + lan)
            aliases = value["aliases"]
            for language in aliases.values():
                for alias in language:
                    alt_labels.add(alias["value"] + '@' + alias["language"])
            if alt_labels:
                data["alt_label"] = alt_labels

            # Description
            descriptions = value["descriptions"]
            for desc in descriptions.values():
                if desc["language"] == "en": # TODO add other descs with @language ?
                    data["description"] = desc["value"]

            data["label"] = main_label
            data["uri"] = f"https://wikidata.org/wiki/{code}"
            data["code"] = code

            # English wikipedia page
            sitelinks = value['sitelinks']
            if 'enwiki' in sitelinks:
                data["ext_ref"] = sitelinks['enwiki']['url']

            # Other properties
            property_items = value["claims"]
            property_ids = {
                "P31": "type", # "instance_of"
                "P247": "COSPAR_ID",
                "P8913": "NAIF_ID",
                "P2956": "ObsCode_MPC_ID",
                "P527": "has_part",
                "P361": "is_part_of",
                #"P17": "country", # can be found using coordinate_location
                #"P276": "location", # same
                "P625": "coordinate_location",
                "P1619": "start_date", # date of official opening
                "P571": "start_date", # time when an entity begins to exist. Not same as P1619.
                # "P2044": "altitude", # elevation above see level
                "P619": "launch_date", # UTC date of spacecraft launch
                "P1427": "launch_place", # start point
                "P856": "url", # official website
                #"P137": "operator", # operator
                #"P397": "orbites", # parent astronomical body
                #"P1096": "orbital_eccentricity", # orbital eccentricity
                #"P2045": "orbital_inclination", # orbital inclination
                #"P2146": "orbital_period", # orbital period
                #"P2233": "semimajor_axis_of_orbit", # semi-major axis of an orbit
                #"P2243": "apoapsis",
                #"P2244": "periapsis",
            }

            for property_id in property_items.keys():
                if property_id in property_ids.keys():
                    # Only keep the ids mentioned above
                    property_name = property_ids[property_id]
                    property_data = property_items[property_id]
                    for prop in property_data:
                        property_value = ""
                        try:
                            datatype = prop["mainsnak"]["datatype"]
                            value = prop["mainsnak"]["datavalue"]["value"]
                            if (type(value) == dict
                                and "id" in value
                                and value['id'] == "Q797476"):
                                # property_name = spatial_events[prop["mainsnak"]["datavalue"]["value"]["id"]]
                                property_name = "launch_date"
                                property_value = prop["qualifiers"]["P585"][0]["datavalue"]["value"]["time"]
                            elif datatype == "external-id":
                                property_value = prop["mainsnak"]["datavalue"]["value"]
                            elif datatype == "wikibase-item":
                                property_value = self._get_label(prop["mainsnak"]["datavalue"]["value"]["id"], result)
                            elif datatype == "quantity":
                                property = prop["mainsnak"]["datavalue"]["value"]
                                property_value = property["amount"]
                                if "unit" in property:
                                    property_value += " " + self._get_label(property["unit"])
                                    # do not add the unit as an entity, only get its label
                            elif datatype == "time":
                                property_value = prop["mainsnak"]["datavalue"]["value"]["time"]
                            elif datatype == "globe-coordinate":
                                data["latitude"] = prop["mainsnak"]["datavalue"]["value"]["latitude"]
                                data["longitude"] = prop["mainsnak"]["datavalue"]["value"]["longitude"]
                            elif datatype == "url":
                                property_value = prop["mainsnak"]["datavalue"]["value"]
                            else:
                                print(f"Warning {property_id}({property_name}) not parsed. datatype: {datatype}")
                                property_value = None

                            if property_value:
                                if property_name in data:
                                    data[property_name].append(property_value)
                                else:
                                    data[property_name] = [property_value]
                        except KeyError:
                            # Malformated WikiData json (missing keys)
                            continue
        return data


    LABEL_BY_WIKIDATA_ITEM = dict()
    def _get_label(self,
                   wikidata_item: str,
                   result: dict = None) -> str:
        """
        Returns the label of a Wikidata entity and add the wikidata entity
        into the result dict (only add its label and uri).

        Keyword arguments:
        wikidata_item -- the URI of the Wikidata entity (Qxxxxxxx)
        result -- the result dict. If set, we add a data inside of it.
        """
        label = ""
        if wikidata_item in self.LABEL_BY_WIKIDATA_ITEM:
            label = self.LABEL_BY_WIKIDATA_ITEM[wikidata_item]
            return label # the dict for this data is already in result dict
        else:
            wikidata_url_json = f"https://www.wikidata.org/wiki/Special:EntityData/{wikidata_item}.json"
            content = CacheManager.get_page(wikidata_url_json,
                                            list_name = self.CACHE,
                                            from_cache = False)
            entities = json.loads(content)

            entities = entities["entities"]

            alt_labels = []
            for key, value in entities.items():
                if label:
                    break
                # Label & alt labels
                labels = value["labels"]
                for language in labels.values():
                    lab = language["value"]
                    lan = language["language"]
                    if lan == "en":
                        label = lab
                        break
                    else:
                        alt_labels.append(lab)
            if not label and alt_labels:
                # If no English label, return the first other label.
                label = alt_labels[0]

        # Prevent requesting wikidata multiple time for
        # the same item
        self.LABEL_BY_WIKIDATA_ITEM[wikidata_item] = label

        # Add the entity into result dict if set
        if result is not None:
            result[label] = {"label": label,
                             "code": wikidata_item,
                             "url": f"https://wikidata.org/wiki/{wikidata_item}"}
        return label



    def _get_controls(self) -> dict:
        """
        Get the controls for every entity with their last update time.
        """

        # control_data contains:
        # - the processing date (for subsequent processing, checking for updates in Wikidata)
        # - the result count
        # - the result data as a dictionary {itemURI: {"label": itemLabel, "modified_date": modifiedDate}}
        control_data = {
            "processing_date": datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "previous_date": None,
            "results_count": 0,
            "results": dict()
        }

        query_control = self._QUERY_CONTROL
        try:
            query_result = self._get_results(query_control)
        except Exception as e:
            raise(e)

        bindings = query_result["results"]["bindings"]

        for binding in bindings:
            item_uri, item_label, modified_date = [binding[k]["value"]
                                                   for k in ['itemURI', 'itemLabel', 'modifiedDate']]

            # Add data to results dict
            control_data["results"][item_uri] = dict([
                ("label", item_label),
                ("modified_date", modified_date)
                ])

        control_data["results_count"] = len(bindings)

        return control_data


    def _extract_entity(self,
                        wikidata_uri: str,
                        result: dict) -> dict:
        """
        Connect to the https://www.wikidata.org/wiki/Special:EntityData endpoint
        to get the JSON response from the Wikidata item.
        Returns None if the request failed.

        Keyword arguments:
        wikidata_uri -- Wikidata URI (Qxxxxxxx) to retrieve
        result -- the result dictionary
        """
        wikidata_item = wikidata_uri.split('/')[-1]
        wikidata_url_json = f"https://www.wikidata.org/wiki/Special:EntityData/{wikidata_item}.json"
        content = CacheManager.get_page(wikidata_url_json,
                                        list_name = self.CACHE,
                                        from_cache = False)
        # CacheManager.save_cache(content, wikidata_url_json)

        # Save cache for latest versions.
        # Cache will be used when we want to merge with wikidata but
        # there is no new version to download.
        # Select the wikidata properties to keep and organize
        # them in the data dict.
        return self._properties_to_dict(content, result)


    def __init__(self):
        pass


if __name__ == "__main__":
    ex = WikidataExtractor()
    ex.extract()