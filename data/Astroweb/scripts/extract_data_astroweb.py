import requests
from bs4 import BeautifulSoup
import json
from pathlib import Path

data_dir = Path(__file__).parents[1] / "data"

url = "https://cds.unistra.fr//astroWeb/astroweb/telescope.html"

response = requests.get(url)

if response.status_code == 200:
    soup = BeautifulSoup(response.text, 'html.parser')

    # Identifier la balise ou la classe qui contient les informations des télescopes
    telescope_list = soup.find('dl')

    # Initialiser une liste pour stocker les données des télescopes
    telescopes_data = []

    # Boucler à travers les éléments de la liste (balises DT et DD)
    for dt, dd in zip(telescope_list.find_all('dt'), telescope_list.find_all('dd')):
        # Extraire les données spécifiques
        name = dt.find('a').text.strip()

        # Extraire l'ID du texte de la balise DT et nettoyer
        telescope_id = dt.text.strip().replace(name, '').replace('\n', '').replace('(', '').replace(')', '')

        # Extraire la description en combinant le texte de toutes les balises DD
        description = ' '.join([text.strip() for text in dd.find_all(text=True, recursive=False)])

        # Nettoyer la description en supprimant les caractères "\n1" et "\n"
        description = description.replace("\n1", "").replace("\n", "")

        # Vérifier si la balise 'a' existe avant d'accéder à l'attribut 'href'
        link_tag = dt.find_next('a')  # Extraire le lien de la balise DT
        link = link_tag['href'] if link_tag else None

        # Extraire les catégories à l'aide d'une expression régulière
       #categories_match = re.search(r'Categories: (.*?)-*<FONT', str(dd), re.IGNORECASE)
        #categories = categories_match.group(1).strip() if categories_match else ""

        # Créer le dictionnaire de données pour chaque télescope
        telescope_data = {
            "Name": name,
            "ID": telescope_id,
            "description": description,
            "link": link
            #"categories": categories
        }

        # Ajouter les données du télescope à la liste
        telescopes_data.append(telescope_data)

    # Enregistrer les données dans un fichier JSON avec l'encodage UTF-8
    with open(data_dir / 'astroweb.json', 'w', encoding='utf-8') as f:
        json.dump(telescopes_data, f, ensure_ascii=False, indent=2)

    print("Données des télescopes enregistrées dans le fichier 'telescopes_data.json'.")

else:
    print(f"Erreur {response.status_code} lors de la requête.")
