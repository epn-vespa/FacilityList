import re
import nltk
import math
from nltk.corpus import stopwords


try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    print("Downloading nltk stopwords...")
    nltk.download('stopwords')


def _compute_for(acronym: list[str],
                 first_letters: list[str],
                 second_letters: list[str],
                 stopwords_letters: list[str],
                 uppercases_letters: list[str],
                 removed_letter: int = -1):
    """
    Recursive method. Do not call. Returns a score between 0 and 1
    for the probability of acronym to be the acronym of the data.
    The parameters are lists of the same size that have either a letter or
    a blank (' '). Example for NASA's label:
    ['n', ' ', 'a', ' ', ' ', ' ', ' ', 's', ' ', 'a', ' ']
    [' ', 'a', ' ', 'e', ' ', ' ', ' ', ' ', 'p', ' ', 'd']
    [' ', ' ', ' ', ' ', 'a', 'n', 'd', ' ', ' ', ' ', ' ']
    [' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ']

    Keyword arguments:
    first_letters --  first letters of each word (except in stopwords)
    second_letters -- second letters of each word (except in stopwords)
    stopwords_letters -- stopwords letters
    uppercases_letters -- uppercases that are not in 1st or 2nd and not in a stopword
    removed_letter -- the index of the previously removed letter
    """
    if len(acronym) == 0:
        # return the proportion of first letters used.
        return 1 - len([x for x in first_letters if x not in " _"]) / len(first_letters)

    # Debug


    print(f"----state {acronym}----")
    print(first_letters)
    print(second_letters)
    print(stopwords_letters)
    print(uppercases_letters)


    letter = acronym[0]
    if letter == 'x':
        # x are often the second letter.
        best_score = 0
        if letter in second_letters:
            for index, matched in enumerate(second_letters):
                if (second_letters[index] == letter and
                    first_letters[index - 1] != ' '):
                    # the first letter is still here. We remove both.
                    first_letters_copy = first_letters.copy()
                    first_letters_copy[index - 1] = '_' # Deleted
                    second_letters[index] = '_' # Deleted
                    score = _compute_for(acronym[1:],
                                         first_letters_copy,
                                         second_letters,
                                         stopwords_letters,
                                         uppercases_letters,
                                         index)
                    if score == 1:
                        return score
                    if score > best_score:
                        best_score = score

        # ignore the Xs as they often have a meaning.
        # FIXME But they can also replace a word (like cross,  multi...),
        # so they should remove a word in the end.
        score =  _compute_for(acronym[1:],
                              first_letters,
                              second_letters,
                              stopwords_letters,
                              uppercases_letters)
        if score > best_score:
            best_score = score
        return best_score
    best_score = 0 # find the best option
    if letter in first_letters:
        # find all indexes of letter in first_letters.
        for index, matched in enumerate(first_letters):
        # TODO add a word order malus for first letters.
            if matched == letter:
                before_letters = first_letters[:index]
                if before_letters.count(' ') + before_letters.count('_') == len(before_letters):
                    # if index == 0 or ''.join(first_letters[:index]):
                    malus = 0
                    # First letter, do not need to give a score malus
                else:
                    malus = 1 / (len(acronym) ** 2)
                first_letters_copy = first_letters.copy()
                first_letters_copy[index] = '_' # Deleted
                score = _compute_for(acronym[1:],
                                     first_letters_copy,
                                     second_letters,
                                     stopwords_letters,
                                     uppercases_letters,
                                     removed_letter = index)
                if score == 1:
                    return score - malus # found the best path
                if score - malus > best_score:
                    best_score = score - malus
    if letter in second_letters:
        # index of prev letter is remove_letter.
        if (len(second_letters) > removed_letter + 1
            and second_letters[removed_letter + 1] == letter):
            second_letters_copy = second_letters.copy()
            second_letters_copy[removed_letter + 1] = '_' # deleted
            score = _compute_for(acronym[1:],
                                 first_letters,
                                 second_letters_copy,
                                 stopwords_letters,
                                 uppercases_letters,
                                 removed_letter = removed_letter + 1)
            if score == 1:
                return score # found the best path
            if score > best_score:
                best_score = score
    if letter in stopwords_letters:
        for index, matched in enumerate(stopwords_letters):
            if matched == letter:
                if (stopwords_letters[index - 1] == '_' and removed_letter != index - 1
                    or stopwords_letters[index - 1] != ' '):
                    # The acronym can not have a letter from the middle of a stopword alone.
                    print("The letter is in the middle of a stopword!", letter)
                    return 0
                stopwords_letters_copy = stopwords_letters.copy()
                stopwords_letters_copy[index] = '_'
                score = _compute_for(acronym[1:],
                                     first_letters,
                                     second_letters,
                                     stopwords_letters_copy,
                                     uppercases_letters,
                                     removed_letter = index)
                if score == 1:
                    return score # found the best path
                if score > best_score:
                    best_score  = score

    if letter in uppercases_letters:
        #if (len(uppercases_letters) > removed_letter + 1
        #    and uppercases_letters[removed_letter + 1] == letter): # following another letter
        for index, matched in enumerate(uppercases_letters):
                if matched == letter:
                    uppercases_letters_copy = uppercases_letters.copy()
                    uppercases_letters_copy[index] = '_'
                    score = _compute_for(acronym[1:],
                                        first_letters,
                                        second_letters,
                                        stopwords_letters,
                                        uppercases_letters_copy,
                                        removed_letter = index)
                    if score == 1:
                        return score # found the best path
                    if score > best_score:
                        best_score = score
    return best_score


def _get_matrixes(label,
                  languages):
    """
    Generate and return matrixes for the label:
    - a matrix containing the first letters of the words in the label,
    - a matrix containing the second letters of the words in the label,
    - a matrix containing the stopwords' letters,
    - a matrix containing other letters in the label that are uppercase.

    Keyword arguments:
    label -- the label without the acronym
    languages -- used to delete stopwords from the label in those languages
    """
    stop_words = set()
    for lang in languages:
        stop_words = stop_words.union(set(stopwords.words(lang)))

    # Create matrixes of letters that are likely to be in the acronym
    first_letters = []
    second_letters = []
    stopwords_letters = []
    uppercases_letters = []
    words = re.findall(r"[\w\d]+", label)
    for word in words:
        state = 'out'
        for letter in word:
            if word.lower() in stop_words:
                first_letters.append(' ')
                second_letters.append(' ')
                stopwords_letters.append(letter.lower()) # keep only first letter of stopwords
                uppercases_letters.append(' ')
            elif state == 'out':
                first_letters.append(letter.lower()) # keep first letter
                second_letters.append(' ')
                stopwords_letters.append(' ')
                uppercases_letters.append(' ')
                state = 'second'
            elif state == 'second':
                first_letters.append(' ') # keep second letter
                second_letters.append(letter.lower())
                stopwords_letters.append(' ')
                uppercases_letters.append(' ')
                state = 'in'
            elif state == 'in':
                if letter.isupper():
                    first_letters.append(' ')
                    second_letters.append(' ')
                    stopwords_letters.append(' ')
                    uppercases_letters.append(letter.lower()) # keep uppercases

            else:
                state = 'out'
    return first_letters, second_letters, stopwords_letters, uppercases_letters


def _clean_acronym(acronym: str) -> str:
    """
    Remove non-alphanumeric characters from the acronym.
    """
    acronym = re.sub(r"[^\w\d]", "", acronym)
    return acronym


def proba_acronym_of(acronym: str,
                     label: str,
                     languages: list[str] = ['english',
                                             'french',
                                             'spanish']) -> bool:
    """
    Returns the probability of an acronym to be the acronym of a label.
    Authorized characters in acronym are alphanumeric characters.
    The case is ignored except if there are uppercases in the label,
    they are taken into account when matching with the acronym.
    To penalize probabilities that are lower than 1, we return proba ^ 4.

    Keyword arguments:
    acronym -- acronym to compute probability from
    label -- the label without the acronym
    languages -- used to delete stopwords from the label in those languages
    """
    acronym = _clean_acronym(acronym)
    acronym = acronym.strip()
    if len(acronym) > len(label):
        return 0
    if ' ' in acronym:
        return 0
    # Acronym is at most 3 times shorter
    if len(acronym) > len(label) / 3:
        return 0
    # Acronym has only uppercase
    if acronym.upper() != acronym:
        return 0

    first_letters, second_letters, stopwords_letters, uppercases_letters = _get_matrixes(label,
                                                                                         languages)
    score = _compute_for(acronym.lower(),
                         first_letters,
                         second_letters,
                         stopwords_letters,
                         uppercases_letters)
    if score == 0:
        new_acronym = _del_numbers(acronym)
        if new_acronym != acronym:
            score = _compute_for(new_acronym.lower(),
                                 first_letters,
                                 second_letters,
                                 stopwords_letters,
                                 uppercases_letters)
    return math.pow(score, 4)


def _del_numbers(acronym: str) -> str:
    """
    Get a version of the acronym with multiplied letters (ex: DA2I => DAII)
    """
    number = 0
    res = ""
    for letter in acronym:
        if letter.isnumeric():
            number = number * 10 + int(letter)
        elif letter.isalpha():
            if number > 0 and number < 10:
                res += letter * number
            elif number >= 10:
                res += str(number) + letter
            else:
                res += letter
            number = 0
        else:
            number = 0
            res += letter
    if number > 0:
        res += str(number)
    return res

def __test__(label, acronym):
    print(f"-- test for {label} // {acronym} --")
    score = proba_acronym_of(acronym, label)
    print("score:", score)


def main():
    test = [("COVID-19 Vaccines Global Access", "COVAX"), # 1
            ("National Aeronautics and Space Administration", "NASA"), # 1
            ("National Aeronautics of Space Administration", "SAONAE"), # 0
            ("Taxe sur la Valeur Ajoutée", "TVA"), # 1
            ("United Nations Educational, Scientific and Cultural Organization", "UNESCO"), # 1
            #("Société Nationale des Chemins de fer Français ", "SNCF"), # not 1 because of "fer"
            #("Société Nationale des Chemins de fer Français ", "SNCFF"), # 1
            #("System for Audio-Visual Event Modeling", "SAVEM"), # 1
            #("System for Audio-Visual Event Modeling", "SyfAuViEvMo"), # 1
            #("SUMmarization in Open Context", "SUMINO" ), # 1
            #("Développement et Administration Internet et Intranet", "DA2I"), # 1 after 2*i
            #("extensible Markup Language", "XML"), # 1
            ("Southern Photometric Local Universe Survey", "S-PLUS"),
            #("Telescopea Action Rapide pour les Objets Transitoires, Rapid Action Telescope for transient objects at Calern Observatory", "Explorer 32"),
            #("international vlbi service for geodesy and astrometry", "SONMIANI"),
            ("international vlbi service for geodesy and astrometry", "IAGA:SON"),
    ]
    for label, acronym in test:
        __test__(label, acronym)

if __name__ == "__main__":
    # Test
    main()