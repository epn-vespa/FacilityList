import requests
from bs4 import BeautifulSoup
import json
from pathlib import Path

url = "https://journals.aas.org/author-resources/aastex-package-for-manuscript-preparation/facility-keywords/"
data_dir = Path(__file__).parents[1] / "data"

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    soup = BeautifulSoup(response.text, 'html.parser')

    # Continuez avec le reste de votre code pour extraire les données du tableau.
    table = soup.find('table')

    rows = table.find_all('tr')

    # Récupérer les en-têtes de colonne (en minuscules pour assurer la casse insensible)
    headers = [header.text.strip().lower() for header in rows[0].find_all('th')]

    # Créer une liste pour stocker les résultats
    results = []

    for row in rows[1:]:  # Commencer à partir de la deuxième ligne pour éviter les en-têtes
        cols = row.find_all('td')
        cols = [col.text.strip() for col in cols]

        # Créer un dictionnaire pour stocker les données de chaque ligne
        row_data = dict(zip(headers, cols))

        # Filtrer et renommer les colonnes selon vos besoins
        filtered_data = {"Name": row_data.get("full facility name", ""), "ID": row_data.get("keyword", "")}

        # if ID="", skip the record
        if filtered_data['ID'] != "":
            results.append(filtered_data)

    # Enregistrer la liste des résultats dans un fichier JSON
    with open(data_dir / "aas.json", 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=4, ensure_ascii=True)

else:
    print(f"Erreur {response.status_code} lors de la requête.")