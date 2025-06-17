"""
NaifExtractor scraps the NAIF webpage and stores data into a dictionary.
The output dictionary is compatible with the ontology mapping (see graph.py).

Troubleshooting:
    Some data have the same identifier in NAIF. Sometimes, they refer to the
    same entity, sometimes not. We have a hand-made list for those that are
    the same, and those that are note. We need to merge those entities and
    use an altLabel.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
from bs4 import BeautifulSoup
from data_updater import entity_types
from data_updater.extractor.cache import CacheManager
from data_updater.extractor.extractor import Extractor
from rdflib import Graph

from config import DATA_DIR # type: ignore


class NaifExtractor(Extractor):
    URL = "https://naif.jpl.nasa.gov/pub/naif/toolkit_docs/FORTRAN/req/naif_ids.html"

    # URI to save this source as an entity
    URI = "NAIF_list"

    # URI to save entities from this source
    NAMESPACE = "naif"

    # Folder name to save cache/ and data/
    CACHE = "NAIF/"

    # Default type used for all unknown types in this resource
    DEFAULT_TYPE = entity_types.SPACECRAFT

    TYPES = {"spacecraft": entity_types.SPACECRAFT,
             "ground station": entity_types.GROUND_OBSERVATORY}

    # List's types.
    # For merging strategies. Prevent merging data from lists
    # that do not have types in common
    POSSIBLE_TYPES = {entity_types.GROUND_OBSERVATORY,
                      entity_types.SPACECRAFT}

    # No need to disambiguate the type with LLM.
    # Useful for merging strategy: when the type is ambiguous,
    # it is recommanded to not discriminate on types.
    # 1: always known.
    # 0.5: partially known (see individuals)
    # 0: never known.
    TYPE_KNOWN = 1

    def __init__(self):
        pass


    def __str__(self):
        return self.NAMESPACE


    def extract(self,
                from_cache: bool = True) -> dict:
        """
        Extract the page content into a dictionary.
        """
        content = CacheManager.get_page(NaifExtractor.URL,
                                        from_cache = from_cache,
                                        list_name = self.CACHE)

        if not content:
            return dict()

        soup = BeautifulSoup(content, "html.parser")

        categories = dict()

        h3_tags = soup.find_all("h3")
        h3_spacecraft = None
        h3_ground_stations = None

        # Generate list of lines for Spacecraft & Ground Stations categories
        for h3 in h3_tags:
            if h3.text.strip() == "Spacecraft":
                h3_spacecraft = h3
                pre_spacecraft = h3_spacecraft.find_next("pre").text
                categories["spacecraft"] = pre_spacecraft.split('\n')[3:]
            elif h3.text.strip() == "Ground Stations.":
                h3_ground_stations = h3
                pre_ground_stations = h3_ground_stations.find_next("pre").text
                categories["ground station"] = pre_ground_stations.split('\n')[3:]

        result = dict()
        for cat, lines in categories.items():
            for line in lines:
                rows = line.split("'")
                if len(rows) < 3:
                    continue
                code = rows[0].strip()
                label = rows[1].strip()
                result[label] = {"code": code,
                                 "label": label,
                                 "type": NaifExtractor.TYPES[cat],
                                 "type_confidence": 1}

        # Delete duplicate identifiers and add alt labels
        # only when they are the same entity (see naif-sc-codes.ttl)
        self._merge_identifiers(result)
        return result


    def _merge_identifiers(self,
                           result: dict):
        """
        Merge entities when they have the same id and are
        the same (see naif-sc-codes.ttl)

        Keyword arguments:
        result: the result dict with all entities' subdicts.
        """
        g = Graph()
        naif_file = DATA_DIR / self.CACHE / "naif-sc-codes.ttl"
        g.parse(str(naif_file))

        # Query to get entities that share the same identifier
        # but should be distinct entities:
        query = """
        SELECT ?naifId ?entity ?label

        WHERE {
            ?entity rdfs:subClassOf ?naifId ;
                    rdfs:label      ?label .
        }
        """
        # response = g.query(query)


        # Query to get entities that have different identifiers
        # but are the same entity:
        query = """
        SELECT ?entity ?label ?altlabel

        WHERE{
            ?entity rdfs:label ?label ;
                    skos:altLabel ?altlabel ;
        }
        """
        response = g.query(query)
        for entity, label, alt_label in response:
            label = str(label)
            alt_label = str(alt_label)
            if label in result.keys():
                if "alt_label" in result[label]:
                    result[label]["alt_label"].append(alt_label)
                else:
                    result[label]["alt_label"] = [alt_label]
                # Remove the duplicate entity from the result
                if alt_label in result:
                    result.pop(alt_label)
        # TODO add skos:exactMatch, dcat:startDate, skos:endDate to the results too ?
        # (they are in the file) only if they can not be extracted automatically.
        # exactMatch example:
        # https://nssdc.gsfc.nasa.gov/nmc/spacecraft/display.action?id=1989-033B
        # for MAGELLAN


if __name__ == "__main__":
    pass