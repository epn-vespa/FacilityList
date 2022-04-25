from pathlib import Path
import json

files = Path('./').glob('NSSDC_*.json')

n = 0
for file in files:
    with open(file, 'r') as f:
        n += len(json.load(f))

print(n)

