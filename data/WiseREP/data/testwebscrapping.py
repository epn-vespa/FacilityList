
import json
import requests
from bs4 import BeautifulSoup

url = "https://naif.jpl.nasa.gov/pub/naif/toolkit_docs/FORTRAN/req/naif_ids.html"
response = requests.get(url)

if response.status_code == 200:
    html = response.text
    soup = BeautifulSoup(html, 'html.parser')

    # Trouver la section "Spacecraft ID Codes"
    spacecraft_id_section = soup.find('a', {'name': 'Spacecraft'}).find_next('pre')

    # Extraire les ID et les noms des vaisseaux spatiaux
    spacecraft_data = []
    for line in spacecraft_id_section.stripped_strings:
        parts = line.split(None, 1)
        if parts and parts[0].isdigit():
            spacecraft_data.append({
                "ID": int(parts[0]),
                "Name": parts[1].strip()
            })

    # Afficher les données
    for spacecraft in spacecraft_data:
        print(spacecraft)
    with open('spacecraft.json', 'w') as json_file:
        json.dump(spacecraft_data, json_file, indent=2)
else:
    print(f"Erreur {response.status_code} lors de la récupération du contenu.")








