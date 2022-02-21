import json

file= '/Users/ldebisschop/Documents/GitHub/FacilityList/data/IAU-MPC/IAU-MPC.txt'

print(file)
dict = {}

fields= ['Code', 'Longitude', 'cos', 'sin', 'Name',]
l = 1
with open(file) as f:
    for line in f:
       description = list(line.strip().split())
       #print(description)
       print("code :", description[0])
       print("Longitude : ", description[1])
       print("cos : ", description[2])
       print("sin : ", description[3])
       print("Name : ", description[4])
       
#       i = 0
#      dict2 = {}
#       while i<len(fields):
#            dict2[fields[i]]= description[i]
#            i = i + 1
#            dict.update(dict2)
#            l = l + 1
        
out_file = open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/IAU-MPC/IAU-MPC.json', "w")
json.dump(dict, out_file, indent = 4)
out_file.close()