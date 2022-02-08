# Python3 code implementing web scraping using lxml & Beautifulsoup
import httplib2
import requests
#import urllib3
from bs4 import BeautifulSoup, SoupStrainer
import urllib 
from lxml import html

 
 #url to scrap data from
#url = 'https://pds.nasa.gov/data/pds4/context-pds4/instrument_host/'
 
 #path to particular element
#path = '/html/body/div/div[2]/table'
 
 #get response object
#response = requests.get(url)
 
 #get byte string
#byte_data = response.content
 
 #get filtered source code
#source_code = html.fromstring(byte_data)
 
 #jump to preferred html element
#tree = source_code.xpath(path)
 
# print texts in first element in list
#print(tree[0].text_content())

#f = open("pds.txt", "w")
#f.write(tree[0].text_content())
#f.close()


# l'url de la page PDS a scrapper
pds_url = 'https://pds.nasa.gov/data/pds4/context-pds4/instrument_host/'

# la fonction get_links_pds prend en argument l'url de la page à scrapper,
# et renvoie la liste des noeuds <a> dont l'attribut href est non-nul.
def get_links_pds(pds_url_f) :

    http = httplib2.Http()
    status, response = http.request(pds_url_f)
    resp = urllib.request.urlopen(pds_url_f)

    parser = 'html.parser' 
    soup = BeautifulSoup(resp, parser, from_encoding=resp.info().get_param('charset'))

    # recupere le premier div dont l'id est 'files' (le panneau contenant le tableau contenant les liens)
    files_div = soup.find('div', id='files')

    # dans la premiere table de files_div, récupere la liste des noeuds <a> dont l'attribut href est non-nul
    files_table = files_div.table
    links = files_table.find_all('a', href=True)
    #print("len(links)=" + str(len(links))) # affiche le nombre de noeuds

    # on construit la liste des liens comme la liste des valeurs de l'attribut href de chaque noeud de la liste links
    resultat = [ link["href"] for link in links ]
    # cette notation est équivalente à la suivante : on peut construire la même chose dans une boucle for explicite
    #resultat = []
    #for link in links :
    #    resultat.append(link["href"])
    
    # avant de retourner le resultat, on transforme la liste
    # des résultat en set, puis de nouveau en liste,
    # ce qui a pour effet de supprimer les doublons
    return list(set(resultat)) 


# links est la liste des fichiers
links = get_links_pds(pds_url)
# on reconstruit une liste dont les éléments sont ceux de links qui se terminent par ".xml"
xml_links = [ link for link in links if link.endswith(".xml") ]
print(xml_links)
for link in xml_links :
    
    link_url=f'{pds_url}{link}'
    # f'texte{variable}' renvoie une chaine de caracteres
    # contenant 'texte' et (le résultat de la conversion en chaine de caractes de) ma_variable

    http = httplib2.Http()
    status, response = http.request(link_url)
    
    resp = urllib.request.urlopen(link_url)

    parser = 'html.parser'
    soup = BeautifulSoup(resp, parser, from_encoding=resp.info().get_param('charset'))
    with open(link,"w") as output_file :
        output_file.write(soup.prettify())
    # lecture de la premiere balise <type>
    try :
        type_text=soup.type.string
        print( link_url + " :  <type> est " + type_text)
    except AttributeError :
        # S'il n'existe pas de balise type
        # tant pis, on met un petit message mais pas grave pour le moment, on pourra gérer cette partie plus tard
        print( link_url + " :  pas de balise <type>")
        pass

    
        