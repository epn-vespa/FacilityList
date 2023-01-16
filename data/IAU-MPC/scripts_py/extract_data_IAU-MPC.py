import json

file = '/Users/ldebisschop/Documents/GitHub/FacilityList/data/observatories/observatories.txt'
output_file = '/Users/ldebisschop/Documents/GitHub/FacilityList/data/IAU-MPC/observatories.json'

my_list=[]
#first line flag
first_line_flag= True
with open(file) as f:
    for line in f:
        if first_line_flag :
            first_line_flag = False
            continue
        description = list(line.strip().split(maxsplit=4)
)
        dic_line ={ 
            "name" : description[0],
            #"ObservatoryLatitude": description[1],
            #"ObservatoryLongitude" : description[2],
            #"ObservatoryAltitude" : description[3],

            }
            
        my_list.append(dic_line)
        #print(json.dumps(dic_line, indent=4))


with open(output_file,"w") as out_f:
    out_f.write(json.dumps(my_list, indent=4))

        

