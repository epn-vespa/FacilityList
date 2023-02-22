import json
with open('pds-list.json', 'r') as f:
    print(len(json.load(f)))

