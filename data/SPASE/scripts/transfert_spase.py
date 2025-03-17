import json

# Chargement des fichiers JSON
with open('/Users/ldebisschop/PycharmProjects/FacilityList/data/obs-facilities_vocabulary/added-spase-id.json', 'r') as f1, open('/Users/ldebisschop/PycharmProjects/FacilityList/data/SPASE/scripts/added-spase-id_manually.json', 'r') as f2:
    data1 = json.load(f1)
    data2 = json.load(f2)

# Extraction des identifiants spase du fichier 1
spase_ids = {}
for entry in data1:
    if 'skos:exactMatch' in entry:
        for match in entry['skos:exactMatch']:
            if 'spase://' in match:
                if entry['@id'] not in spase_ids:
                    spase_ids[entry['@id']] = []
                spase_ids[entry['@id']].append(match)

# Ajout des identifiants spase au fichier 2
for entry in data2:
    if entry['@id'] in spase_ids:
        if 'skos:exactMatch' not in entry:
            entry['skos:exactMatch'] = []
        for spase_id in spase_ids[entry['@id']]:
            if spase_id not in entry['skos:exactMatch']:
                entry['skos:exactMatch'].append(spase_id)

# Sauvegarde du fichier modifié
with open('added-spase-id_manually1.json', 'w') as f2_mod:
    json.dump(data2, f2_mod, indent=4)
