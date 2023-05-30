import json
with open('NSSDC_list1.json', 'r') as f:
    print(len(json.load(f)))

