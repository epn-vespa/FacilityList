
import re
from nltk.corpus import stopwords
import nltk
from nltk import WhitespaceTokenizer
from nltk.tokenize import word_tokenize
tokenizer = WhitespaceTokenizer()

try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    print("Downloading nltk stopwords...")
    nltk.download('stopwords')

# Load stopwords
LANGUAGES = ["english", "french", "spanish", "italian", "russian", "arabic"]
stop_words = set()
for lang in LANGUAGES:
    stop_words = stop_words.union(stopwords.words(lang))

def tokenize(string: str) -> list[str]:
    return word_tokenize(string)

def del_stopwords(string: str) -> str:
    tokens = tokenizer.span_tokenize(string)
    for start, end in list(tokens)[::-1]:
        if string[start:end] in stop_words:
            string = string[:start] + string[end:]
    string = re.sub(r" +", " ", string)
    return string.strip()