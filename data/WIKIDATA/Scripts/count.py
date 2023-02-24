### couunt the number of items in extract_wikidata
import json
with open('extract_wikidata.json', 'r') as f:
    print(len(json.load(f)))

