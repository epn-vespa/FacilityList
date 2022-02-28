import json
output_file='/Users/ldebisschop/Documents/GitHub/FacilityList/data/NSSDC/new_NSSDC_Planetery-Science.json'
datadict=json.load(open("NSSDC_Planetery-Science.json"))

datadict2 = []
for e in datadict :
    datadict2.append({
            "name" : e["name"],
            "NSSDC id" : e["NSSDC id"]
        }
    )
with open(output_file,"w") as out_f:
    out_f.write(json.dumps(datadict2, indent=4))