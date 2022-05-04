import httplib2
import requests
from bs4 import BeautifulSoup, SoupStrainer
import urllib
import json
from lxml import html

spase_url = 'https://heliophysicsdata.gsfc.nasa.gov/websearch/dispatcher'


def get_links_spase(spase_url_f):
    http = httplib2.Http()
    status, response = http.request(spase_url_f)
    resp = urllib.request.urlopen(spase_url_f)

    parser = 'html.parser'
    soup = BeautifulSoup(resp, parser, from_encoding=resp.info().get_param('charset'))
    files_div = soup.find('div', id='files')
    files_table = files_div.table
    links = files_table.find_all('a', href=True)
    resultat = [link["href"] for link in links]
    return list(set(resultat))


links = get_links_spase(spase_url)
xml_links = [link for link in links if link.endswith(".xml")]

result = []
for link in xml_links:
    link_url = f'{spase_url}{link}'
    print(link_url)
http = httplib2.Http()
status, response = http.request(link_url)

resp = urllib.request.urlopen(link_url)

parser = 'html.parser'
soup = BeautifulSoup(resp, parser, from_encoding=resp.info().get_param('charset'))
with open(link, "w") as output_file:
    output_file.write(soup.prettify())

    try:
        title_text = soup.title.string
        logical_identifier = soup.logical_identifier.string
        Alias_List = soup.Alias_List.string
        alternate_id = soup.alternate_id.string
        alternate_title = soup.alternate_title.string

    except AttributeError:
        pass

result_elt = {"title": title_text, " logical_identifier": logical_identifier}
print("result_elt :", result_elt)
result.append(result_elt)

with open("pds-test-list.json", "w") as f:
    f.write(json.dumps(result, indent=4))


