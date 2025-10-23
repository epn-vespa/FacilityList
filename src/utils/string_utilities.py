"""
Define utility functions to manipulate string data.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import re

from typing import Tuple
from collections import defaultdict
from urllib.parse import quote
from utils.acronymous import proba_acronym_of
from graph.extractor.extractor import Extractor


def standardize_uri(label: str) -> str:
    """
    Creates a valid uri string from a label using lowercase and hyphens
    between words.

    Args:
        label: the label of the entity.
    """
    label = label.lower()
    label = re.sub(r"[^\w\s\.]", ' ', label)
    label = re.sub(r"\s+", ' ', label) # Remove multiple spaces
    label = label.split(' ')
    label = '-'.join([l for l in label if l])
    label = quote(label)
    return label


def get_extractor_from_namespace(namespace: str) -> Extractor:
    """
    Get the list name from an URI.
    Example: ".../pds#..." -> "pds"

    Args:
        namespace: the namespace URI of an entity.
    """
    namespace = namespace.split('/')[-1].split('#')[-1].split(':')[-1]
    return namespace


def cut_acronyms(label: str) -> Tuple[str]:
    """
    Acronyms are alternate names that are between parentheses.
    Returns:
        (name without acronyms, last acronym*)
        *if the last acronym is at the end of the label.

    Args:
        label: a label containing acronyms between parentheses.
    """
    label = label.strip()
    # label, acronym_aka = cut_aka(label)
    acronyms = list(re.finditer(r"\([^(]+?\)", label))
    if not acronyms:
        return label, ""
    full_name_without_acronyms = ""
    acronym_str = ""
    prev_acronym_idx = 0
    for acronym in acronyms:
        name = label[prev_acronym_idx:acronym.start()-1].strip()
        prev_acronym_idx = acronym.end()+1
        acronym_str = acronym.group()[1:-1].strip() # remove ()
        full_name_without_acronyms += name + " "

    full_name_without_acronyms += label[prev_acronym_idx:]
    if label[-1] != ')':
        acronym_str = "" # Acronym for the whole string (last word)
    if len(acronyms) > 1:
        # If there are more than one acronym, impossible to detect which
        # acronym is the right one
        acronym_str = ""
    # Return full name without acronyms + the last acronym
    # Compute the probability of the acronym string to be an acronym
    # of the label without its acronyms.

    if proba_acronym_of(acronym_str, full_name_without_acronyms) != 1:
        acronym_str = ""
    return clean_string(full_name_without_acronyms), acronym_str


def cut_aka(label: str) -> Tuple[str]:
    """
    Delete stopwords like 'aka' from the label.
    Return the label without the aka and its aka.

    Args:
        label:the label to delete akas and get the alternate name from.
    """
    stopwords = '|'.join(["aka",
                          "a.k.a.",
                          "also known as",
                          "formerly the",
                          "formerly"])
    exp = re.compile(f"\\b({stopwords})\\b")

    alt_label = "" # Alt label for the whole entity
    for match in re.finditer(exp, label.lower()):
        start_index = match.start()
        end_index = match.end()
        if label[start_index-1] != '(': # The aka is not between ()
            after_aka = label[end_index:]
            # The aka end after a ','
            comma = after_aka.find(',')
            if comma > 0: # not the last substring
                aka = after_aka[0:comma]
                after_aka = after_aka[comma:]
            else:
                aka = after_aka
                after_aka = "" # end
                alt_label = aka # if end of string, is alt label of entity
            # label = f"{label[:start_index]} ({aka}) {after_aka}"
            label = f"{label[:start_index]} {after_aka}"

        else:
            # Find the aka for alt_label (if at the end of the string)
            if label[end_index:].count(')') == 1 and label.endswith(')'):
                alt_label = label[end_index:-1]
                label = label[:start_index-1]

    label = re.sub(exp, "", label)
    label = re.sub(r" +", " ", label)
    label = re.sub(r"\( ", "(", label)
    label = clean_string(label)

    # If the altLabel is in the location of the entity
    # it is not an altLabel of the entity.
    if alt_label and " at " not in label:
        return label, clean_string(alt_label)
    #if label[-1] == ')':
    #    return label.strip(), aka# last is a )
    return label, ""


def get_aperture(label: str) -> Tuple[str, set[str]]:
    """
    Get the aperture of the facility from the label (in AAS & SPASE).
    Return label without apertures and apertures string converted to meters.

    Troubleshooting:
        MM (or mm) is not for millimeter but for magnometer (see SPASE).

    Args:
        label: the label to extract the size from.
    """
    aperture_lst = []
    apertures = re.findall(r"(\d+)([\.\,]\d+)?( )?(-)?(cm|m|km|centimeter|meter|kilometer|CM|M|KM| ?inche?s?)\b", label.lower())
    if apertures:
        for s in apertures:
            aperture_lst.append(''.join(s))
    result = set()
    ms = set() # meter values
    inches = defaultdict(set)
    for aperture in aperture_lst:
        # Remove apertures from label and convert to meters.
        # Merge apertures that have rounded identical values after conversion
        label = re.sub(aperture, "", label).strip()
        aperture = aperture.lower()
        value = extract_number(aperture)
        if aperture.endswith("inch") or aperture.endswith("inches"):
            value = convert_to_meters(value, "inch")
            inches[round(value, ndigits = 1)].add(round(value, ndigits=2))
            inches[round(value, ndigits = 0)].add(round(value, ndigits=2))
            inches[value.__trunc__()].add(round(value, ndigits=2))
        elif aperture.endswith("cm"):
            value = convert_to_meters(value, "cm")
            ms.add(value)
        else:
            ms.add(round(value, ndigits = 2))
        result.add(round(value, ndigits = 2))
    # label without apertures & apertures list

    # If there were cm or m and inches, remove the inches value
    # if they were close enough after rounding or truncation.
    for value in ms:
        if value in inches:
            for value2 in inches[value]:
                result.remove(value2)
    result = {str(r) + 'm' for r in result}
    return clean_string(label), result


def extract_number(string: str) -> float:
    """
    Extract a number (float or int) from a string.
    Only for positive numbers.

    Args:
        string: extract number from this string
    """
    value = re.findall(r"(\d+)([\.\,]\d+)?", string)
    value = float(''.join(value[0]))
    return value


def convert_to_meters(value: float | str,
                      unit: str = "") -> float:
    """
    Convert an unit to meters.

    Args:
        value: string with value+unit or only float value
        unit: if unit is not in value, use this parameter
    """
    if type(value) == str:
        value_float = extract_number(value)
        if not unit:
            unit = value.removeprefix(value_float).strip()
    else:
        value_float = value
    if unit.lower() in ["inch", "inches"]:
        return value_float * 0.0254
    elif unit.lower() == "cm":
        return value_float / 100
    return value_float # Could not convert


def cut_part_of(label: str):
    """
    In AAS, some entities have "part of the" in their label.
    We want to cut them out and create a relation isPartOf.
    Sometimes, the "part of the" keyword is after the location,
    sometimes it is before, so we must be careful about which
    one is part of something.

    Returns:
        the label without the part of & the part of.

    Args:
        label: the label to cut.
    """
    part_of_keyword = "part of the"
    part_of_begin = label.lower().find(part_of_keyword)
    if part_of_begin == -1:
        part_of_keyword = "part of" # there are no cases like this in AAS
        part_of_begin = label.lower().find(part_of_keyword)
        if part_of_begin == -1:
            return label, ""
    before_part_of = label[:part_of_begin].strip()
    parenthesis_opened = False
    # The parenthesis opened before the "part of" keyword
    if before_part_of and before_part_of[-1] == '(':
        parenthesis_opened = True
        before_part_of = before_part_of[:-1].strip()
    after_part_of = label[part_of_begin + len(part_of_keyword):].strip()
    if parenthesis_opened:
        part_of_end = after_part_of.find(')')
        part_of = after_part_of[:part_of_end].strip()
        after_part_of = after_part_of[part_of_end+1:].strip()
    else:
        part_of = after_part_of
        after_part_of = ""
    label_without_part_of = before_part_of + ' ' + after_part_of
    return clean_string(label_without_part_of), clean_string(part_of)


def cut_location(label: str,
                 delimiter: str) -> Tuple[str]:
    """
    Get the location of an entity by splitting it on a
    certain delimiter and add new alternate labels.
    Add alternate labels in alt_labels for:
    - the entity without the location,
    - the entity without the location and acronyms,
    - the entity's acronym without the location.
    Then, call clean_string to remove the first "the " in the location.

    Args:
        label: the label of an entity
        delimiter: the delimiter (" at ", ","...)
        second_delimiter: if there are more than one locations
        alt_labels: the set of alternate labels to add to
    """
    location = ""
    label_without_location = label
    if label.count(delimiter) == 1:
        label_without_location, location = [a.strip() for a in label.split(delimiter, maxsplit = 1)]
    # More than one location (example AAS: 'Yunnan Astronomical Observatory (YAO); Lijiang Observatory')
    # locations = [l.strip() for l in location.split(second_delimiter)]
    return label_without_location, location


def clean_string(text: str) -> str:
    """
    Removes all \n, \t and double spaces from a string.
    Remove final '.'

    Args:
        string: the string to clean
    """
    text = text.replace("\r", " ")
    text = re.sub(r"\t", " ", text)
    text = re.sub(r"\n+", "\n", text)
    text = re.sub(r"\r", " ", text)
    text = re.sub(r" +", " ", text).strip()
    if text and text[-1] == '.':
        text = text[:-1]
    return text.strip()


def remove_parenthesis(text: str) -> str:
    """
    Remove parenthesis and the text inbetween for any text.
    Use to standardize labels.

    /!\ If a closing parenthesis is missing, will remove
    the text until the end from the opened parenthesis.
    Use with care: some in-parenthesis in labels might be relevant
    (like North, South to specify a site of an observatory for example).

    Args:
        text: any label. If it has no parenthesis, will return the same text.
    """
    if '(' in text:
        clean_text = text
        while '(' in clean_text:
            # Repeat for each parenthesis
            old_clean_text = clean_text
            begin = clean_text.find('(')
            end = clean_text.find(')')
            clean_text = old_clean_text[0:begin]
            if end != -1:
                clean_text += ' ' + old_clean_text[end + 1:]
        clean_text = re.sub(' +', ' ', clean_text).strip()
        return clean_text
    return text


def remove_punct(text: str) -> str:
    """
    Remove all punctuation from a string.
    Keep only letters and numbers.

    Args:
        text: the text to remove punctuation from
    """
    text = re.sub(r"[^a-zA-Z0-9 ]+", ' ', text)
    return text


def cut_language_from_string(text: str) -> Tuple[str, str]:
    """
    Cut the language tag on '@' if there is a language tag.
    Returns the text without language tag and the language tag.
    Language tag example: @en
    The language tag should be at the end of the string.

    Args:
        text: will be split on '@' into a text and its language
    """
    lang = re.findall(r"@[^ @]+$", text)
    if lang:
        lang = lang[0][1:] # remove @
        text = text[:-len(lang) - 1]
    else:
        lang = ""
    return text, lang


def has_cospar_nssdc_id(text: str) -> Tuple[bool, list[str], list[str]]:
    """
    Return True if the provided label contains a COSPAR id
    or NSSDC id, return the matched NSSDC id and
    the year, associated with the launch date of a spacecraft.

    Args:
        text: string that may contain an NSSDC or COSPAR id.
    """
    pattern = r"\b(?:19|20)[0-9][0-9]-[A-Z0-9]{4,5}\b"
    cospar_ids = re.findall(pattern, text)
    if not cospar_ids:
        return False, None, None
    launch_dates = []
    for cospar_id in cospar_ids:
        year = cospar_id.split('-')[0]
        launch_dates.append(get_datetime_from_iso(year))
    return True, cospar_ids, launch_dates


def get_datetime_from_iso(datetime_str: str) -> str:
    """
    Fix datetime string :
        month 00 day 00 -> 1st of January
        & remove '+' sign
    Also complete the incomplete ISO dates (only year, year-month).

    Args:
        datetime_str: the ISO datetime string
    """
    if datetime_str.startswith('+'): # datetime module
        datetime_str = datetime_str[1:]
    datetime_str = datetime_str.replace("-00T", "-01T").replace("-00-", "-01-")
    if re.match(r"^\d\d\d\d$", datetime_str):
        datetime_str += "-01-01T00:00:00"
    elif re.match(r"^\d\d\d\d-\d\d$", datetime_str):
        datetime_str += "-01T00:00:00"
    elif re.match(r"^\d\d\d\d-\d\d-\d\d$", datetime_str):
        datetime_str += "T00:00:00"

    return datetime_str

if __name__ == "__main__":
    pass