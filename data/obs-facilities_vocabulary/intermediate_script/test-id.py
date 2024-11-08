import json

# Chemin vers le fichier JSON d'entrée
input_file = '/Users/ldebisschop/PycharmProjects/FacilityList/data/obs-facilities_vocabulary/obsfacilities_vocabulary-clean-accent.json'
# Chemin vers le fichier texte de sortie
output_file = '/Users/ldebisschop/PycharmProjects/FacilityList/data/obs-facilities_vocabulary/ids_list.txt'

try:
    # Chargement du fichier JSON
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Vérifiez que 'data' est bien une liste d'objets
    if isinstance(data, list):
        # Ouvrir le fichier de sortie en mode écriture (tout le contenu est réinitialisé à chaque exécution)
        with open(output_file, 'w', encoding='utf-8') as f:
            # Extraction et écriture des valeurs de la clé "@id"
            for item in data:
                # Vérifier que chaque élément est un dictionnaire contenant "@id"
                if isinstance(item, dict) and "@id" in item:
                    id_value = item["@id"]
                    print(f"Trouvé @id: {id_value}")  # Affiche chaque @id trouvé
                    f.write(id_value + '\n')  # Ajoute chaque @id dans le fichier, sans écrasement

        print(f"Les valeurs de '@id' ont été enregistrées dans : {output_file}")
    else:
        print("Erreur : Les données JSON doivent être une liste d'objets.")

except FileNotFoundError:
    print(f"Erreur : Le fichier '{input_file}' est introuvable.")
except json.JSONDecodeError:
    print("Erreur : Le fichier d'entrée n'est pas un JSON valide.")
except Exception as e:
    print(f"Une erreur s'est produite : {e}")
