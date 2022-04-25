import json
with open('spacecraft.json', 'r') as f:
    print(len(json.load(f)))

