import json

file = '/Users/ldebisschop/Documents/GitHub/FacilityList/data/NAIF/naif.txt'
output_file = '/Users/ldebisschop/Documents/GitHub/FacilityList/data/NAIF/naif.json'

my_list = []
# first line flag
first_line_flag = True
with open(file) as f:
    for line in f:
        if first_line_flag:
            first_line_flag = False
            continue
        description =" ".join(line.strip().split()).split("'")
        dic_line = {
            "code": description[0].strip(),
            "Name": description[1]
        }

        my_list.append(dic_line)
        # print(json.dumps(dic_line, indent=4))

with open(output_file, "w") as out_f:
    out_f.write(json.dumps(my_list, indent=4))



