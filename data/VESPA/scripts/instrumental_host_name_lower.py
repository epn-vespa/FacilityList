import json
input_file="/Users/ldebisschop/Documents/GitHub/FacilityList/data/VESPA/data/instrument_host_name.json"
output_file="/Users/ldebisschop/Documents/GitHub/FacilityList/data/VESPA/data/instrument_host_name.json"
data=json.load(open("/Users/ldebisschop/Documents/GitHub/FacilityList/data/VESPA/data/instrument_host_name.json"))

with open(input_file, "r", encoding='utf-8') as in_f:
    contenu = in_f.read()

for dictionnaire in data:
    instrument_host_name = dictionnaire["instrument_host_name"]
    if isinstance(instrument_host_name, str):  # Vérifier si la valeur est une chaîne de caractères
        nouvelle_valeur = instrument_host_name.lower().replace(" ", "-")
        dictionnaire["instrument_host_name_lower"] = nouvelle_valeur
    

print(data)

with open(output_file,"w",encoding='utf-8') as out_f:
    out_f.write(json.dumps(data, ensure_ascii=False, indent=4))