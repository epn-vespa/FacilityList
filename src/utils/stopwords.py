import pathlib
from pathlib import Path

stopwords_folder = pathlib.Path(__file__).parent.parent.parent / "data" / "stopwords"
print(stopwords_folder)

all_stopwords = dict()

for file in stopwords_folder.glob("*"):
    filename = file.name
    if not "README" in str(filename):
        with open(file, "r") as f:
            all_stopwords[filename] = f.read().split('\n')
print(all_stopwords)

def words(lang):
    return all_stopwords[lang]

def get_languages():
    """list all available languages"""
    return list(all_stopwords.keys())
