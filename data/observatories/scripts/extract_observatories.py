import json

file = '/Users/ldebisschop/Documents/GitHub/FacilityList/data/observatories/observatories.txt'
output_file = '/Users/ldebisschop/Documents/GitHub/FacilityList/data/observatories/observatories.json'

my_list = []
# first line flag
first_line_flag = True
with open(file) as f:
    for line in f:
        description = line.strip().split(sep='\t')
        print(description)
        dic_line = {
            "name": description[0],
            "Observatorycoordonnées": description[1],
            #"ObservatoryAltitude": description[2],

        }

        my_list.append(dic_line)
        # print(json.dumps(dic_line, indent=4))

with open(output_file, "w") as out_f:
    out_f.write(json.dumps(my_list, indent=4))