import requests
from bs4 import BeautifulSoup
import json
from pathlib import Path

data_dir = Path(__file__).parents[1] / "data"

# Faire une requête HTTP pour récupérer le code HTML de la page
url = "https://www.minorplanetcenter.net/iau/lists/ObsCodes.html"
response = requests.get(url)

# Vérifier si la requête a réussi (statut HTTP 200)
if response.status_code == 200:
    # Utiliser BeautifulSoup pour traiter le code HTML
    soup = BeautifulSoup(response.content, 'html.parser')

    # Extraire le texte entre les balises <pre>
    pre_text = soup.find('pre').get_text()

    # Traitement du texte pour extraire la première colonne comme ID et la dernière colonne comme Name
    data = []
    lines = pre_text.split('\n')
    for line in lines[2:]:  # Ignorer les deux premières lignes (noms de colonnes et ligne vide)
        #C15 132.1656 0.72418 +0.68737 ISON-Ussuriysk Observatory
        obs_id=line[0:3]
        obs_name=line[30:]
        data.append({"ID": obs_id, "Name": obs_name.strip()})

    # Enregistrer les données dans un fichier JSON
    output_file= data_dir / 'iau-mpc.json'
    with open(output_file, 'w') as json_file:
        json.dump(data, json_file, indent=2)
    print("Les données ont été enregistrées dans " + output_file)
else:
    print(f"La requête a échoué avec le statut {response.status_code}")
