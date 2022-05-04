import json
output_file='/Users/ldebisschop/Documents/GitHub/FacilityList/data/SANA/spacecraft-202202031333.json'
datadict=json.load(open("all_datas_spacecraft-202202031333.json"))

#for e in datadict[:3] : print(e)


# maniere 1
datadict2 = []
for e in datadict :
    # si la valeur associee a aliases est equivalent a une liste vide, (comme par exemple si c'est une liste vide :D), on laisse tomber pour cette iteration, et on passe a la suivante
    # le continue est un mot cle qui n'a de sens que dans une boucle for ou while, et qui signifie d'arreter la l'execution de l'iteration en cours de la boucle, et de passer a l'iteration suivante
    # -> si pas d'alias, on ignore la ligne
    #if e["Aliases"] == [] : continue
    datadict2.append({
            "OID" : e["OID"],
            "Name" : e["Name"],
            "Abbreviation" : e["Abbreviation"],
            "Aliases" : e["Aliases"]
        }
    )
#for e in datadict2[:3] : print(e)
    
# 2
#datadict2 = []
#for e in datadict :
 #   if e['Aliases'] == [] : continue
  #  datadict2.append(
 #       {  x : e[x] for x in ["OID", "Name", "Abbreviation", "Aliases"]  }
#    )
#for e in datadict2[:3] : print(e)

#3
#names = ["OID", "Name", "Abbreviation", "Aliases"]
#datadict2= [ { x : e[x] for x in names } for e in datadict if len(e["Aliases"])>0 ]
    
#for e in datadict2[:3] : print(e)

with open(output_file,"w") as out_f:
    out_f.write(json.dumps(datadict2, indent=4))
