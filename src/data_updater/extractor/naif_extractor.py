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
from extractor.cache import CacheManager
from rdflib import Graph


class NaifExtractor():
    URL = "https://naif.jpl.nasa.gov/pub/naif/toolkit_docs/FORTRAN/req/naif_ids.html"

    # URI to save this source as an entity
    URI = "NAIF_list"

    # URI to save entities from this source
    NAMESPACE = "naif"

    # Folder name to save cache/ and data/
    CACHE = "NAIF/"

    # Default type used for all unknown types in this resource
    DEFAULT_TYPE = "spacecraft"


    def __init__(self):
        pass


    def extract(self) -> dict:
        """
        Extract the page content into a dictionary.
        """
        content = CacheManager.get_page(NaifExtractor.URL,
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
                                "type": cat}

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
        g.parse("naif-sc-codes.ttl")

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