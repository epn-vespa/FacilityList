import json
with open('NAIF.json', 'r') as f:
    naif = len(json.load(f))
with open('NAIF_DSN/DSN.json', 'r') as f:
    dsn = len(json.load(f))

print(naif+dsn)


