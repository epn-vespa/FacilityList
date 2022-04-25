import json
with open('list_observatories_spacecrafts.json', 'r') as f:
    print(len(json.load(f)))

