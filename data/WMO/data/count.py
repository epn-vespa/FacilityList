import json
with open('WMO_list.json', 'r') as f:
    print(len(json.load(f)))

