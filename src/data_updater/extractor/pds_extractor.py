"""
PdsExtractor scraps the NASA's PDS webpage and stores data into a dictionary.
The output dictionary is compatible with the ontology mapping (see graph.py).

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
from bs4 import BeautifulSoup
from extractor.cache import CacheManager
from xml.etree import ElementTree as ET
import re
from lxml import etree, objectify

class PdsExtractor():
    # List of documents to scrap
    # URL = "https://pds.nasa.gov/data/pds4/context-pds4/facility/"
    URL = "https://pds.nasa.gov/data/pds4/context-pds4/"

    # URI to save this source as an entity
    URI = "NASA-PDS_list"

    # URI to save entities from this source
    NAMESPACE = "pds"

    # Default type used for all unknown types in this resource
    DEFAULT_TYPE = "observation facility"

    # Mapping between PDS xml files and our dictionary format
    FACILITY_ATTRS = {"logical_identifier": "code",
                      "name": "label"}

    # List context products types to be retreived and the applicable subtypes
    # examples:
    # {URL}/telescope/*.*
    # {URL}/facility/observatory.*
    CONTEXT_TYPES = {"telescope": ["all"],
                     "facility": ["observatory"],
                     "instrument_host": ["lander", "rover", "spacecraft"],
                     "investigation": ["mission"]}

    if __name__ == "__main__":
        pass

    def extract(self) -> dict:
        """
        Extract the page content into a dictionary.
        """
        result = dict()

        # Dictionary to save the internal references and replace
        # them by our ontology's ID in the result dict
        # (used with hasPart & isPartOf)
        pds_references_by_id = dict()

        for context_type in PdsExtractor.CONTEXT_TYPES:
            url = PdsExtractor.URL + context_type
            # content = CacheManager.get_page(PdsExtractor.URL)
            links = self._get_links_pds(url)

            links = [link for link in links
                     if link.endswith(".xml") and not link.startswith("Collection")]

            links = self._last_versions(links)

            #namespace = "http://pds.nasa.gov/pds4/pds/v1"

            #namespaces = {'pds': 'http://pds.nasa.gov/pds4/pds/v1'}

            for href in links:
                data = dict()

                # cat (type)
                cat = href.split('.')[0]
                if "all" in PdsExtractor.CONTEXT_TYPES[context_type]:
                    # keep all subtypes from this type
                    cat = context_type
                elif cat not in PdsExtractor.CONTEXT_TYPES[context_type]:
                    # ignore this subtype
                    continue

                # Download XML file for href
                resource_url = PdsExtractor.URL + context_type + '/' + href
                content = CacheManager.get_page(resource_url)

                # Remove default namespace for lookup
                content = re.sub(r'xmlns="[^"]+"', '', content)

                # Parse XML file
                #root = ET.fromstring(content)

                # Parse XML file using lxml
                root = ET.fromstring(content.encode("utf-8"))#, etree.XMLParser())

                # Internal references
                has_part = []
                is_part_of = []
                internal_references = root.findall(f".//Internal_Reference")
                for internal_reference in internal_references:
                    lid_reference = internal_reference.find("lid_reference")
                    if lid_reference is None or not lid_reference.text:
                        continue
                    # lid_reference = lid_reference.text.split(':')[-1]
                    reference_type = internal_reference.find("reference_type")
                    # TODO add the has_part & is_part_of with the other reference in result before returning result
                    if reference_type.text in [
                            "investigation_to_instrument_host",
                            "facility_to_telescope"]:
                        has_part.append(lid_reference.text)
                    elif reference_type.text in [
                            "instrument_host_to_investigation",
                            "telescope_to_facility"]:
                        is_part_of.append(lid_reference.text)
                if has_part:
                    data["has_part"] = has_part
                if is_part_of:
                    data["is_part_of"] = is_part_of

                # Facility's tags
                facility = root.find(f".//{cat.title()}") # ex: ".//Facility"
                if facility is None:
                    facility = root.findall('*')[-1] # last div is the facility
                tags = facility.findall('*')
                for tag in tags:
                    if tag.text is None:
                        continue # empty tag
                    tag_str = tag.tag
                    tag_str = PdsExtractor.FACILITY_ATTRS.get(tag_str, tag_str)
                    data[tag_str] = re.sub("[\n ]+", " ", tag.text.strip())

                data["url"] = resource_url

                # label
                label = root.find(".//title")
                # label = root.xpath("//title")
                if label is not None:
                    label = label.text.strip()
                    if "label" not in data:
                        data["label"] = label

                # code
                logical_identifier = root.find(".//logical_identifier")
                if logical_identifier is not None:
                    code = logical_identifier.text.strip()
                    data["code"] = code
                    pds_references_by_id[code] = label

                result[data["label"]] = data

        # If the PDS identifier does not exists in the
        # extracted data, create a new entity with this
        # identifier.
        pds_missing_ids = dict()
        for key, value in result.items():
            if "has_part" in value:
                for i, part in enumerate(value["has_part"]):
                    if part in pds_references_by_id:
                        value["has_part"][i] = pds_references_by_id[part]
                    else:
                        # Create the entity from the missing id.
                        if part not in pds_missing_ids:
                            data = self._create_entity_from_missing_id(part)
                            if data["label"] == "individual.none":
                                # Some entities refer to None
                                value["has_part"][i] = None
                                continue
                            pds_missing_ids[part] = data
                        value["has_part"][i] = pds_missing_ids[part]["label"]
            if "is_part_of" in value:
                for i, part in enumerate(value["is_part_of"]):
                    if part in pds_references_by_id:
                        value["is_part_of"][i] = pds_references_by_id[part]
                    else:
                        # Create the entity from the missing id.
                        if part not in pds_missing_ids:
                            data = self._create_entity_from_missing_id(part)
                            if data["label"] == "individual.none":
                                # Some entities refer to None
                                value["is_part_of"][i] = None
                                continue
                            pds_missing_ids[part] = data
                        value["is_part_of"][i] = pds_missing_ids[part]["label"]

        # If a PDS id is missing, add an artificial entity for this code
        for key, value in pds_missing_ids.items():
            result[value["label"]] = value

        return result

    def _create_entity_from_missing_id(self,
                                       identifier: str) -> dict:
        """
        Creates a data dictionary for an identifier that does not exist in the
        PDS database but should still be added to the ontology.

        Keyword arguments:
        identifier -- the ID of the entity in PDS
        """
        data = dict()
        cut = identifier.split(":")
        label = re.sub(r"[_]", " ", cut[-1])
        cat = cut[-2]
        data = {"label": label,
                "code": identifier,
                "type": cat}
        return data

    def _get_links_pds(self,
                       pds_url_f: str) -> list:
        """The get_links_pds function processes the provided pds_url
        and extracts the list of xml file links to context products.

        Keyword arguments:
        pds_url_f -- input PDS url page
        """

        # get pds_url_f
        content = CacheManager.get_page(pds_url_f)

        # parse content with BeautifulSoup
        soup = BeautifulSoup(content, "html.parser")

        # fetch the first <div> with id = 'files'
        files_div = soup.find('div', id='files')

        # In the first table of files_div, fetch the list of <a> nodes with non-null href attribute
        files_table = files_div.table
        links = files_table.find_all('a', href=True)

        # Build list of links, using the list of href attributes
        result = [ link["href"] for link in links ]

        # Before returning the result, the list is
        # transformed into a set and back to a list.
        # The removes the duplicated entries.
        return list(set(result))


    def _last_versions(self,
                       id_list: list) -> list:
        """
        Sort a list of pds ids and keep only the latest version
        if there are multiple versions.

        Keyword arguments:
        id_list -- list of XML files
        """
        new_list = []

        # prepare temporary a dict with name as key and a dict in value,
        # version as key and id in value.
        # Example:
        # telescope_a_1.0.xml
        # telescope_b_1.0.xml
        # telescope_b_1.1.xml
        # will result in:
        # names = {
        #   "telescope_a": {
        #       "1.0": "telescope_a_1.0.xml"
        #   },
        #   "telescope_b": {
        #       "1.0": "telescope_b_1.0.xml"
        #       "1.1": "telescope_b_1.1.xml"
        #   }
        # }
        names = {}
        for item in id_list:
            # we need to split name and version
            pieces = item.split('_')
            name = "_".join(pieces[:-1])
            version = pieces[-1].replace(".xml", "")
            if name in names.keys():
                names[name][version] = item
            else:
                names[name] = {version: item}
        for name, versions in names.items():
            max_version = max(versions.keys())
            new_list.append(versions[max_version])
        return new_list

if __name__ == "__main__":
    pass