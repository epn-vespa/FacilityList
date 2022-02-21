import json

file = '/Users/ldebisschop/Documents/GitHub/FacilityList/data/IAU-MPC/IAU-MPC.txt'
output_file = '/Users/ldebisschop/Documents/GitHub/FacilityList/data/IAU-MPC/IAU-MPC.json'

my_list=[]
with open(file) as f:
    for line in f:
        description = list(line.strip().split())
        dic_line ={ 
            "code" : description[0],
            "Longitude": description[1],
            "cos" : description[2],
            "sin" : description[3],
            "Name": description[4]
            }
            
        my_list.append(dic_line)
        print(json.dumps(dic_line, indent=4))


with open(output_file,"w") as out_f:
    out_f.write(json.dumps(my_list, indent=4))

        

