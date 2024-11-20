import json
from pathlib import Path

data_dir = Path(__file__).parents[1] / "data"

input_file= data_dir / "aas.json"
output_file= data_dir / "aas.json"

with open(input_file) as f:
    data=json.load(f)

cles_a_supprimer = [
    "Facility Location",
    "Facility has X-Ray observing capability (0.1 - 100 Angstroms or 0.12 - 120 keV)",
    "Facility has Optical observing capability (3000 - 10,000 Angstroms or 0.3 - 1 micron)",
    "Facility has solar observing capability",
    "Facility has Radio observing capability (below 30 GHz)",
    "Facility has Neutrinos, particles, and gravitational waves observing capability",
    "Archive/Database",
    "Facility has Gamma-Ray observing capability (above 120 keV)",
    "Facility has Infrared observing capability (1 - 100 microns)",
    "Computational Center",
    "Facility has Millimeter observing capability (0.1 - 10 millimeters or 3000 - 30 GHz)",
    "Facility has Ultraviolet observing capability (100 - 3000 Angstroms or 1.2 - 120 eV)"
]

# Parcourir chaque dictionnaire dans la liste et supprimer les clés spécifiées
for dictionnaire in data:
    for cle in cles_a_supprimer:
        if cle in dictionnaire:
            del dictionnaire[cle]


# Parcourir chaque dictionnaire dans la liste et renommer la clé
for dictionnaire in data:
    if "Full Facility Name" in dictionnaire:
        dictionnaire["Name"] = dictionnaire.pop("Full Facility Name")
    if "name" in dictionnaire:
        dictionnaire["Name"] = dictionnaire.pop("name")
    if "Abbreviated ID" in dictionnaire:
        dictionnaire["ID"] = dictionnaire.pop("Abbreviated ID")        
# Afficher le résultat
print(data)
with open(output_file,"w",encoding='utf-8') as out_f:
    out_f.write(json.dumps(data, ensure_ascii=False, indent=4))
