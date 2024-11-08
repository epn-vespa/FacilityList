import json
with open('xephem_sites.json', 'r') as f:
    print(len(json.load(f)))

