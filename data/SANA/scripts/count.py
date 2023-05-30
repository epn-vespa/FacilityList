import json
with open('all_datas_spacecraft.json', 'r') as f:
    print(len(json.load(f)))

