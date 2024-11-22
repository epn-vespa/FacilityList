# Python3 code implementing web scraping using Beautifulsoup
import requests
from bs4 import BeautifulSoup
import json
from lxml import objectify
import sys

# List context products types to be retreived (and the applicable subtypes)
CONTEXT_TYPES = {
    "telescope": ["all"],
    "facility": ["observatory"],
    "instrument_host": ["lander", "rover", "spacecraft"],
    "investigation": ["mission"]
}


def get_links_pds(pds_url_f: str) -> list:
    """The get_links_pds function processes the provided pds_url
    and extracts the list of xml file links to context products.

    :param pds_url_f: input PDS url page
    """

    # get pds_url_f
    resp = requests.get(pds_url_f)

    # parse content with BeautifulSoup
    parser = 'html.parser'
    soup = BeautifulSoup(resp.content, parser, from_encoding=resp.encoding)

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


def last_versions(id_list):
    """sort a list of pds ids and keep only the latest version"""
    new_list = []

    # prepare temporary a dict with name as key and a dict in value, version as key and id in value.
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


def pds_classname(classname):
    """transfom a lower cased pds class_name to standard camel cased pds Class_Name"""
    return "_".join(item.capitalize() for item in classname.split("_"))


def get_lid_reference(internal_ref_obj):
    """get the LID reference out of an internal reference object"""
    if "lid_reference" in internal_ref_obj.__dict__.keys():
        lid_reference = internal_ref_obj.lid_reference.text
    elif "lidvid_reference" in internal_ref_obj.__dict__.keys():
        lid_reference = internal_ref_obj.lidvid_reference.text.split("::")[0]
    else:
        raise AttributeError(f"No lid reference found in {internal_ref_obj}")
    return lid_reference


def extract_data_pds(context_type="all"):
    """This function extracts the content of PDS products
    wit the selected context type.

    :param context_type: context to extract data from
    """

    _context_types = list(CONTEXT_TYPES.keys())
    # special case where the context type is set to "all":
    # context types is set to the list of CONTEXT_TYPES keys.
    context_types = _context_types
    if context_type != "all":
        if context_type in _context_types:
            context_types = [context_type]
        else:
            ValueError(f"Context type must be one of the following: {','.join(_context_types)}, or all")

    for context_type in context_types:
        print(f"Extracting PDS {context_type}")
        # PDS web page to be scrapped
        pds_url = f'https://pds.nasa.gov/data/pds4/context-pds4/{context_type}/'

        # links is the list of files
        links = get_links_pds(pds_url)
        # we rebuild a list, which elements are:
        # - ending with ".xml"
        # - not starting with "Collection"
        xml_links = [link for link in links
                     if link.endswith(".xml") and not link.startswith("Collection")]
        print(xml_links)

        xml_links = last_versions(xml_links)
        print(xml_links)
        print(f"Found {len(xml_links)} records to process.")

        # results is a dict with keys = logical_identifier
        results = {}

        for link in xml_links:

            link_url = f'{pds_url}{link}'
            print(f"Processing URL: {link_url}")

            resp = requests.get(link_url)

            # Use lxml.objectify to facilitate access to known XML entities
            xml_data = objectify.XML(resp.content)

            # checking if the context subtype is applicable
            context_type_area = xml_data.__dict__[pds_classname(context_type)]
            subtype = xml_data.__dict__.get("type", None)
            if subtype is not None:
                if subtype.text.lower() in CONTEXT_TYPES[context_type] or "all" in CONTEXT_TYPES[context_type]:
                    # subtype should be processed:
                    pds_class = f"{context_type}.{subtype}"
                else:
                    # if subtype is not applicable, go to next iteration
                    print(f"skipping record ({subtype}).")
                    continue
            else:
                pds_class = context_type

            logical_identifier = xml_data.Identification_Area.logical_identifier.text

            # create result dict and fill with title, link_url, pds_class and logical_identifier
            this_result = {
                "link_url": link_url,
                "pds_class": pds_class,
                "title": xml_data.Identification_Area.title.text,
                "logical_identifier": logical_identifier,
                "version_id": xml_data.Identification_Area.version_id.text
            }

            # if there is an `Alias_List` there should be 'alternate_id' and 'alternate_name'
            # then feed values into aliases list
            aliases = set()
            if (alias := xml_data.Identification_Area.find('.//{*}alternate_id')) is not None:
                aliases.add(alias.text)
            if (alias := xml_data.Identification_Area.find('.//{*}alternate_title')) is not None:
                aliases.add(alias.text)

            if len(aliases) > 0:
                this_result["aliases"] = list(aliases)

            # if there is an Internal_Reference, use this for has_part or is_part_of relations
            has_part = []
            is_part_of = []

            if "Reference_List" in xml_data.__dict__.keys():
                if "Internal_Reference" in xml_data.Reference_List.__dict__.keys():
                    for internal_ref in xml_data.Reference_List.Internal_Reference:
                        if internal_ref.reference_type.text in [
                            "investigation_to_instrument_host",
                            "facility_to_telescope",
                        ]:
                            has_part.append(get_lid_reference(internal_ref))
                        if internal_ref.reference_type.text in [
                            "instrument_host_to_investigation",
                            "telescope_to_facility"
                        ]:
                            is_part_of.append(get_lid_reference(internal_ref))

            if len(has_part) > 0:
                this_result["has_part"] = has_part
            if len(is_part_of) > 0:
                this_result["is_part_of"] = is_part_of

            # if there is a naif_host_id, keep it
            if "naif_host_id" in context_type_area.__dict__.keys():
                this_result["naif_host_id"] = context_type_area.naif_host_id.text

            print("result:", this_result)
            results[logical_identifier] = this_result

        print(results)
        with open("pds-list_facility.json", "w") as f:
            f.write(json.dumps(results, indent=4))


if __name__ == "__main__":
    if len(sys.argv) > 1:
        extract_data_pds(sys.argv[1])
    else:
        extract_data_pds()

