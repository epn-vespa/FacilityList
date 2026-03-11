
import re
from utils import stopwords


# Load stopwords
LANGUAGES = ["english", "french", "spanish", "italian", "russian", "arabic"]
stop_words = set()
for lang in LANGUAGES:
    stop_words = stop_words.union(stopwords.words(lang))

def tokenize(string: str) -> list[str]:
    # return word_tokenize(string)
    return re.findall(r"[^\b ]+", string)

def del_stopwords(string: str) -> str:
    tokens = tokenize(string)
    for start, end in list(tokens)[::-1]:
        if string[start:end] in stop_words:
            string = string[:start] + string[end:]
    string = re.sub(r" +", " ", string)
    return string.strip()
