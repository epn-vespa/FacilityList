from pathlib import Path
import json

files = Path('./').glob('memes_noms*.json')

n = 0
for file in files:
    with open(file, 'r') as f:
        n += len(json.load(f))

print(n)

