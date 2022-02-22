import lxml
from lxml import etree
from bs4 import BeautifulSoup
import json
import re


input_file= open('/Users/ldebisschop/Documents/GitHub/FacilityList/data/Astroweb/Astroweb.html', "r")
output_file ='/Users/ldebisschop/Documents/GitHub/FacilityList/data/Astroweb/Astroweb.json'
file = input_file.read()
parser = 'html.parser' 
result=[]
S = BeautifulSoup(file, 'lxml')


 

print(S.body.form)
DL_node = S.body.dl
definitions=[] # la liste des definitions et descriptions reconnues
for i, DL_child in enumerate(DL_node.children) :
    if DL_child.name == "dt" :
        # encountered a new definition such as :
#   <DT>
#        <A HREF="http://heasarc.gsfc.nasa.goV/docs/heao1/heao1.html">
#            1st High Energy Astrophysics Observatory
#        </A>
#        <!-- OWNER made anonymous -->
#        (HEAO 1. GSFC. NASA)
#    </DT>
        print()
        print("  # # # # # # # # # # # #")
        print("  # found new DT node #")
        print("  # # # # # # # # # # # #")
        print(DL_child)
        print()
        definition={}
        definition["href"] = DL_child.a["href"]
        definition["name"] = DL_child.a.text.strip()
        # on extrait individuellement chaque ligne de texte, sans les espaces et retours a la ligne
        strings_list=list(DL_child.stripped_strings)
        print("strings_list = ", strings_list)
        # dqms certains cas, pas de short name. on verifie donc le nomdre de lignes recuperees
        if len(strings_list) >= 2 : definition["short_name"]=strings_list[1]
        
        print("new definition :", definition)
        definitions.append(definition)
        
    elif DL_child.name == "dd" :
        # encountered a description for current definition
        print()
        print("  - - - - - - - - - - - -")
        print("  - found new DL node -")
        print("  - - - - - - - - - - - -")
        print(DL_child)
        print()        
        description = {}
        
        # remplir la description
        
        # completer la derniere definition appercue
        definitions[-1]["description"] = description 
    
    if i > 50 : break # remove this line do do the whole file



with open(output_file, "w") as f :
    f.write(json.dumps(definitions, indent=4 ))
      
