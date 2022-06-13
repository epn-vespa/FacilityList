import json
with open('list_observatories_spacecrafts1.json', 'r') as f:
    print(len(json.load(f)))

