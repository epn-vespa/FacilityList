import json
with open('pds_instrument_host.json', 'r') as f:
    print(len(json.load(f)))

