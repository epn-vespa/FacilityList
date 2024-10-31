import requests
from bs4 import BeautifulSoup
import json

url = "http://tdc-www.harvard.edu/iraf/rvsao/bcvcorr/obsdb.html"

# Effectuer la requête HTTP
response = requests.get(url)

# Vérifier si la requête a réussi
if response.status_code == 200:
    # Parser le contenu HTML avec BeautifulSoup
    soup = BeautifulSoup(response.content, "html.parser")

    # Extraire les informations
    observatories = []
    for line in soup.find_all("pre")[0].text.split('\n'):
        if "observatory" in line:
            obs_id = line.split(' ')[-1][1:-1]  # Extraire l'ID de l'observatoire
        elif "name" in line:
            obs_name = line.split('"')[1]  # Extraire le nom de l'observatoire
            observatories.append({"ID": obs_id, "Name": obs_name})
            # Enregistrer les données dans un fichier JSON
            with open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/IRAF/data/iraf.json', 'w') as json_file:
                json.dump(observatories, json_file, indent=2)
    # Afficher les résultats
    print(observatories)
else:
    print(f"Échec de la requête HTTP. Statut : {response.status_code}")
