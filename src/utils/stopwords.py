import pathlib
from pathlib import Path

stopwords_folder = pathlib.Path(__file__).parent.parent.parent / "data" / "stopwords"

all_stopwords = dict()

for file in stopwords_folder.glob("*"):
    filename = file.name
    if not "README" in str(filename):
        with open(file, "r") as f:
            all_stopwords[filename] = f.read().split('\n')

def words(lang):
    return all_stopwords[lang]

def get_languages():
    """list all available languages"""
    return list(all_stopwords.keys())
