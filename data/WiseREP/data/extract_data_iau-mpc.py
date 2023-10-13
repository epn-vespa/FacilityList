import requests
from bs4 import BeautifulSoup
import json
import re

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
        columns = line.split(None, 2)
        if len(columns) == 3:
            # La première colonne comme ID
            obs_id = columns[0]
            # La dernière colonne comme Name
            obs_name = columns[2]

            # Utiliser une expression régulière pour exclure les coordonnées et le texte entre parenthèses
            obs_name = re.sub(r'[-+]?[0-9]*\.?[0-9]+', '', obs_name)
            obs_name = re.sub(r'\([^)]*\)', '', obs_name).strip()

            # Ajouter les données à la liste
            data.append({"ID": obs_id, "Name": obs_name.strip()})

    # Enregistrer les données dans un fichier JSON
    with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/IAU-MPC/data/iau-mpc.json', 'w') as json_file:
        json.dump(data, json_file, indent=2)

    print("Les données ont été enregistrées dans observations.json.")
else:
    print(f"La requête a échoué avec le statut {response.status_code}")
