from bs4 import BeautifulSoup, SoupStrainer
import json
import glob
#import os
#print("os.getcwd()=" + os.getcwd())
spase_url="https://heliophysicsdata.gsfc.nasa.gov/websearch/dispatcher"

def read_xml(xml_file_path):
    with open(xml_file_path) as xml_file :
        soup = BeautifulSoup(xml_file, features="lxml")
        print(soup)
        r = {}
        r['ResourceName'] = soup.resourcename.string
        r['ResourceID'] = soup.resourceid.string
        r['AlternateName'] = [ x.string for x in soup.find_all("alternatename") ]
        r['Description'] = soup.description.string
        return r

result=[]
for f in glob.glob('../data/SMWG/Observatory/**/*.xml', recursive=True) :
    result.append(read_xml(f))

with open("spase_data_V_1.0.json", "w") as f:
    f.write(json.dumps(result, indent=4))


