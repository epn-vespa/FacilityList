"""
Utility function for different automatic validation strategies.
1. With z-score:
    exactMatch, broaderMatch, narrowerMatch
    only propose the high entities
2. With 1st match only
3. With top-k (all are proposed to the AI at the same time and AI makes a choice)
4. With top-1 pairwise (same|distinct + justification string, like in v1.0)

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import numpy as np

def softmax(x: np.array) -> np.array:
    """
    Sum of distribution becomes 1
    """
    e_x = np.exp(x - np.max(x, axis=-1, keepdims=True))
    return e_x / np.sum(e_x, axis=-1, keepdims=True)

def sigmoid(x: np.array) -> np.array:
    """
    The higher values are even higher
    """
    return 1 / (1 + np.exp(-x))

def get_high_values_indexes(x, k = 0.3, eps = 0.01, min_val = 0.6):
    """
    eps: epsilon, minimal standard deviation.
         Will return indexes > min_val if std < eps.
    """
    x = sigmoid(x)
    mean = np.mean(x)
    std = np.std(x)
    if std < eps:
        return np.where(x >= min_val)[0]
    threshold = mean + k * std
    res = np.where(x >= threshold)[0]
    return res

def strat1(scores: list[float],
           top_k: int = 10):
    """
    Scores that are higher than average + standard variation
    or higher than 0.4 if all the scores are equivalent (std < 0.01).

    Arguments:
        scores: similarity scores of each entity of the compared list
        top_k: if top_k or more entities are selected, return none instead
    """
    x = np.array(scores)
    indexes = get_high_values_indexes(x)
    indexes = list(indexes)
    if len(indexes) >= top_k:
        return [] # Do not return top_k indexes
    return indexes
