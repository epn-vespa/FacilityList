"""
Tests for LLM entity classification.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
from tqdm import tqdm
import setup_path

import matplotlib.pyplot as plt
from sklearn.metrics import confusion_matrix
import seaborn as sns
from config import OLLAMA_MODEL
from graph import Graph
from rdflib import URIRef
from pathlib import Path


from data_updater.entity_types import to_string, categories_by_descriptions
from utils import llm_connection


def load_dataset():
    """
    Loads the dataset as a 2d array [[text, label]]
    """
    dataset = []
    annotations = dict()
    with open(Path(__file__).parent.parent / "data" / "test"/ "types_annotated-updated-2025.05.20.tsv") as file:
        for line in file.readlines():
            uri, cat, _ = line.split('\t')
            annotations[uri] = cat
    g = Graph()
    g.parse(str(Path(__file__).parent.parent / "data" / "ontologies" / "updated-2025.05.20.ttl"))
    for uri, cat in annotations.items():
        # Get textual representation for the uri
        data = dict()
        for s, p, o in g.triples((URIRef(uri), None, None)):
            attr_name = g.OM.get_attr_name(p)
            value = str(o)
            data[attr_name] = value
        if not data:
            continue
        repr = to_string(data,
                         exclude = ["code",
                                    "uri",
                                    "url",
                                    "ext_ref",
                                    "alt_label",
                                    "launch_date",
                                    "start_date",
                                    #"launch_place",
                                    "COSPAR_ID",
                                    "NSSDCA_ID",
                                    "NAIF_ID",
                                    "equivalent_class",
                                    "source",
                                    "type",
                                    "MPC_ID"])

        dataset.append([repr, cat])
    return dataset


if __name__ == "__main__":

    dataset = [["Cassini-Huygens NASA space mission sent to the Saturn system", "space mission"],
              ["Cassini space probe that went to Saturn, part of Cassini−Huygens mission", "spacecraft"],
              ["Tarleton State University Obs., Stephenville", "ground observatory"],
              ["University of Minnesota, Minneapolis", "ground observatory"],
              ["Vanguard 2 Earth orbiting weather satellite", "unknown"],
              ["2010 KQ space debris with spectral characteristics similar to a rocket body", "unknown"],
              ["2MS", "unknown"],
              ["0.48m Swedish Vacuum Solar Telescope (SVST) at Roque de los Muchachos Observatory", "telescope"],
              ["Swinburne University of Technology", "ground observatory"],
              ["Davis Station", "ground observatory"],
              ["Search for habitable Planets EClipsing ULtra-cOOl Stars (SPECULOOS ) Northern Observatory three 1 meter telescopes.", "observatory network"],
              ["International Space Station", "spacecraft"],
              ["La Palma (GOTO-North) and Siding Spring Observatory (GOTO-South)", "observatory network"],
              ["The Instituto de Astrofísica de Canarias (IAC) Two-meter Twin Telescope facility. Wavelength: Optical.", "telescope"],# or observatory network ?
              ["The Kodaikanal Solar Observatory's Solar tunnel telescope. Location: Asia. Observed object: solar facility. Waveband: Optical. ", "telescope"],
              ["Kyoto University Astronomical Observatory and Department of Astronomy 3.8-meter Seimei Telescope. Location: Asia. Waveband: Optical, Infrared.", "telescope"],
              ["Asteroid Terrestrial-impact Last Alert System Optical (3000 - 10,000 Angstroms or 0.3 - 1 micron):	Optical", "mission"],
              ["ITOS-J. Description: TOS-J was the 4th in a series of third-generation spacecraft in the National Operational Meteorlogical Satellite System (NOMSS).. Type: Improved TIROS Operational System. ", "unknown"],
              ["CSO-3. Description: French Earth observation satellite. Type: Earth observation satellite. Is part of: Composante Spatiale Optique.", ""]
    ]

    dataset = load_dataset() # Load from the annotated corpus & a saved ontology
    print("Dataset loaded successfully. Dataset size:", len(dataset))
    ok = 0
    all = len(dataset)
    errors = []
    llm_choices = set()

    possible_categories = sorted(set(categories_by_descriptions.values()))

    y_true = []
    y_pred = []

    for description, expected in tqdm(dataset, desc = "Predicting types"):
        predicted = llm_connection.LLM().classify(description, from_cache = False)
        if predicted == expected:
            ok += 1
        else:
            errors.append((predicted, expected))
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
    plt.title(f"Confusion matrix of {OLLAMA_MODEL}. Score = {score}")
    plt.xticks(rotation=45, ha='right')
    plt.yticks(rotation=0)
    plt.tight_layout()
    plt.savefig(f"confusion_matrix_types_{OLLAMA_MODEL}_score_{score}")

