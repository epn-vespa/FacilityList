
# Created By  : Laura Debisschop
# Created Date: 02-13-2022
# version ='1.0'


import json

file = '/Users/ldebisschop/Documents/GitHub/FacilityList/data/ADS/ADS_facilities.txt'
output_file = '/Users/ldebisschop/Documents/GitHub/FacilityList/data/ADS/ADS_facilities.json'

my_list=[]

with open(file) as f:
    for line in f:
        # si la ligne commence par #
        #if line.startswith("#"):
            #on passe a la suivante
            #continue
        
        v = line.split()# list des chaines de caractere present dans line et separe par ;
        description = [ x.strip() for x in v ] # liste des elements x de v auquels on apllique la fonction strip
        dic_line ={ 
            "ID" : description[0],
            "Name": description[1]
            }
            
        my_list.append(dic_line)
        #
        print(json.dumps(dic_line, indent=4))


with open(output_file,"w") as out_f:
    out_f.write(json.dumps(my_list, indent=4))

        




