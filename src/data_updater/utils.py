from typing import Tuple, Set
from urllib.parse import quote
import re

def standardize_uri(label: str) -> str:
    """
    Creates a valid uri string from a label using lowercase and hyphens
    between words.

    Keyword arguments:
    label -- the label of the entity.
    """
    label = label.lower()
    # label = del_aka(label)
    label = re.sub(r"[^\w\s\.]", ' ', label)
    label = re.sub(r"\s+", ' ', label) # Remove multiple spaces
    label = label.split(' ')
    label = '-'.join([l for l in label if l])
    label = quote(label)
    return label

def del_aka(label: str) -> Tuple[str]:
    """
    Delete stopwords like 'aka' from the label.
    Add parenthesis around the other name (after the aka), so that
    we can extract it as an acronym (alternate name).
    Returns:
        Tuple(Label without akas, aka)

    Keyword arguments:
    label -- the label to delete akas and get the alternate name from.
    """
    stopwords = '|'.join(["aka",
                          "a.k.a.",
                          "also known as",
                          "formerly the",
                          "formerly"])

    exp = re.compile(f"\\b({stopwords})\\b")
    alt_label = "" # Alt label for the whole entity
    for match in re.finditer(exp, label):
        start_index = match.start()
        end_index = match.end()
        if label[start_index-1] != '(': # The aka is not between ()
            # Add () around the aka
            after_aka = label[end_index:]
            # The aka end after a ','
            comma = after_aka.find(',')
            if comma > 0: # not the last thing
                aka = after_aka[0:comma]
                after_aka = after_aka[comma:]
            else:
                aka = after_aka
                after_aka = "" # end
                alt_label = aka # if end of string, is alt label of entity
            label = f"{label[:start_index]} ({aka}) {after_aka}"
        else:
            # Find the aka for alt_label (if at the end of the string)
            if label[end_index:].count(')') == 1 and label.endswith(')'):
                alt_label = label[end_index:-1]
                label = label[:start_index-1]
    label = re.sub(exp, "", label)
    label = re.sub(r" +", " ", label)
    label = re.sub(r"\( ", "(", label)
    label = label.strip()

    # If the altLabel is in the location of the entity
    # it is not an altLabel of the entity.
    if alt_label and " at " not in label:
        return label, alt_label
    #if label[-1] == ')':
    #    return label.strip(), aka# last is a )
    return label, ""

def cut_acronyms(label: str) -> Tuple[str]:
    """
    Acronyms are alternate names that are between ().
    Returns:
        (name without acronyms, last acronym*)
        *if the last acronym is at the end of the label.

    Keyword arguments:
    label -- the label containing acronyms
    """
    label = label.strip()
    label, acronym_aka = del_aka(label)
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
    if not acronyms:
        if acronym_aka:
            acronym_str = acronym_aka
    if len(acronyms) > 1:
        # If there are more than one acronym, impossible to detect which
        # acronym is the right one.
        acronym_str = ""
    # Return full name without acronyms + the last acronym
    return full_name_without_acronyms.strip(), acronym_str

def cut_location(label: str,
                 delimiter: str,
                 alt_labels: Set[str]) -> Tuple[str]:
    """
    Get the location of an entity by splitting it on a
    certain delimiter and add new alternate labels.
    Add alternate labels in alt_labels for:
    - the entity without the location,
    - the entity without the location and acronyms,
    - the entity without the location's acronym.

    Keyword arguments:
    label -- the label of an entity
    delimiter -- the delimiter (" at ", ","...)
    alt_labels -- the set of alternate labels to add to
    """
    location = ""
    if label.count(delimiter) == 1:
        label, location = [a.strip() for a in label.split(delimiter)]
        label_without_acronyms, label_acronym = cut_acronyms(label)
        alt_labels.add(label)
        alt_labels.add(label_without_acronyms)
        alt_labels.add(label_acronym)
        if "" in alt_labels:
            alt_labels.remove("")
    return location

if __name__ == "__main__":
    pass
