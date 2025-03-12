from typing import List, Tuple
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
    """
    stopwords = '|'.join(["aka",
                          "a.k.a.",
                          "also known as",
                          "formerly the",
                          "formerly"])

    exp = re.compile(f"\\b({stopwords})\\b")
    for match in re.finditer(exp, label):
        start_index = match.start()
        end_index = match.end()
        if label[start_index-1] != '(': # The aka is not between ()
            # Add () around the aka
            after_aka = label[start_index:]
            # The aka end after a ','
            comma = after_aka.find(',')
            if comma:
                aka = after_aka[0:comma]
                after_aka = after_aka[comma+1:]
            else:
                aka = after_aka
                after_aka = ""
            label = f"{label[:start_index]} ({aka}) {after_aka}"
    label = re.sub(exp, "", label)
    print("ici:", label)
    label = re.sub(r" +", " ", label)
    label = re.sub(r"\( ", "(", label)
    return label.strip()

def cut_acronyms(label: str) -> Tuple[str]:
    """
    Acronyms are alternate names that are between ().
    Return a tuple (name1, acronym)

    Keyword arguments:
    label -- the label containing acronyms
    """
    label = del_aka(label)
    label = label.strip()
    acronyms = list(re.finditer(r"\([^(]+?\)", label))
    if not acronyms:
        return label, ""
    full_name_without_acronyms = ""
    acronym_str = ""
    result = []
    prev_acronym_idx = 0
    for acronym in acronyms:
        name = label[prev_acronym_idx:acronym.start()-1].strip()
        prev_acronym_idx = acronym.end()+1
        acronym_str = acronym.group()[1:-1].strip() # remove ()
        full_name_without_acronyms += name + " "

    full_name_without_acronyms += label[prev_acronym_idx:]
    if label[-1] != ')':
        acronym_str = "" # Acronym for the whole string (last word)
    # Return full name without acronyms + the last acronym
    if len(acronyms) > 1:
        # If there are more than one acronym, impossible to detect which
        # acronym is the right one.
        acronym_str = ""
    return full_name_without_acronyms.strip(), acronym_str

def get_alternate_name(label: str) -> list:
    """
    When there is 'and' or 'or' in a label, it may be an alternate name.
    """
    # TODO

if __name__ == "__main__":
    pass