import json
from bs4 import BeautifulSoup
import codecs

input_file = "/Users/ldebisschop/Documents/GitHub/FacilityList/data/VESPA/data/facilities_epncore_v02.json"
output_file = "/Users/ldebisschop/Documents/GitHub/FacilityList/data/VESPA/data/facilities_epncore-v02.json"

with codecs.open(input_file, "r", encoding='utf-8') as in_f:
    data = json.load(in_f)

for dictionnaire in data:
    instrument_host_name = dictionnaire.get("facility", "")
    if isinstance(instrument_host_name, str):
        decoded_name = BeautifulSoup(instrument_host_name, 'html.parser').text
        nouvelle_valeur = decoded_name.lower().replace(" ", "-")
        dictionnaire["facility"] = nouvelle_valeur

print(data)

with codecs.open(output_file, "w", encoding='utf-8') as out_f:
    json.dump(data, out_f, ensure_ascii=False, indent=4)
