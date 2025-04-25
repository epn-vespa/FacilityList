"""
Tests for LLM entity classification.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import setup_path

import matplotlib.pyplot as plt
from sklearn.metrics import confusion_matrix
import seaborn as sns

from data_updater.entity_types import *

if __name__ == "__main__":

    dataset = [["Cassini-Huygens", "NASA space mission sent to the Saturn system", "space mission"],
              ["Cassini", "space probe that went to Saturn, part of Cassini−Huygens mission", "spacecraft"],
              ["Tarleton State University Obs., Stephenville", "", "ground observatory"],
              ["University of Minnesota, Minneapolis", "", "ground observatory"],
              ["Vanguard 2", "Earth orbiting weather satellite", "unknown"],
              ["2010 KQ", "space debris with spectral characteristics similar to a rocket body", "unknown"],
              ["2MS", "", "unknown"],
              ["0.48m Swedish Vacuum Solar Telescope (SVST) at Roque de los Muchachos Observatory", "", "telescope"],
              ["Swinburne University of Technology", "", "ground observatory"],
              ["Davis Station", "", "ground observatory"],
              ["Search for habitable Planets EClipsing ULtra-cOOl Stars (SPECULOOS ) Northern Observatory three 1 meter telescopes.", '', "observatory network"],
              ["International Space Station", "", "spacecraft"],
              ["La Palma (GOTO-North) and Siding Spring Observatory (GOTO-South)", "", "observatory network"],
              ["The Instituto de Astrofísica de Canarias (IAC) Two-meter Twin Telescope facility. Wavelength: Optical.", "", "telescope"],# or observatory network ?
              ["The Kodaikanal Solar Observatory's Solar tunnel telescope. Location: Asia. Observed object: solar facility. Waveband: Optical. ", "", "telescope"],
              ["Kyoto University Astronomical Observatory and Department of Astronomy 3.8-meter Seimei Telescope. Location: Asia. Waveband: Optical, Infrared.", "", "telescope"],
              ["Asteroid Terrestrial-impact Last Alert System", "Optical (3000 - 10,000 Angstroms or 0.3 - 1 micron):	Optical", "mission"]]
    ok = 0
    all = len(dataset)
    errors = []
    llm_choices = set()

    possible_categories = sorted(set(categories_by_descriptions.values()))

    y_true = []
    y_pred = []

    for label, description, expected in dataset:
        predicted = classify(label + " " + description, from_cache=False)
        if predicted == expected:
            ok += 1
        else:
            errors.append((label, predicted, expected))  
        y_true.append(expected)
        y_pred.append(predicted)
    score = ok / all

    print(f"---{len(errors)} errors---")
    for error in errors:
        print(error, "\n")
    print(f"score of the model {OLLAMA_MODEL}:", score)

    cm = confusion_matrix(y_true, y_pred, labels=possible_categories)

    plt.figure(figsize=(10, 8))
    sns.heatmap(cm, annot=True, fmt="d", cmap="Blues", cbar=True,
                xticklabels=possible_categories,
                yticklabels=possible_categories)

    plt.xlabel("Predicted")
    plt.ylabel("Expected")
    plt.title(f"Confusion matrix of {OLLAMA_MODEL}")
    plt.xticks(rotation=45, ha='right')
    plt.yticks(rotation=0)
    plt.tight_layout()
    plt.show()

