import json
import requests
from datetime import datetime, timedelta
from SPARQLWrapper import SPARQLWrapper, JSON

# Configurez la requête SPARQL
sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
end_date = datetime.now()
start_date = end_date - timedelta(days=5)

sparql.setQuery(f"""
SELECT ?spacecraft WHERE {{
  ?spacecraft wdt:P31 wd:Q1254933; 
              schema:dateModified ?modDate .
  FILTER (?modDate >= "{start_date.strftime('%Y-%m-%dT%H:%M:%SZ')}"^^xsd:dateTime)
}}
""")

sparql.setReturnFormat(JSON)
results = sparql.query().convert()

# Extrait les IDs
spacecraft_qids = [result['spacecraft']['value'].split('/')[-1] for result in results["results"]["bindings"]]
#print(spacecraft_qids)

def check_recent_changes(qid):
    api_url = 'https://www.wikidata.org/w/api.php'
    params = {
        'action': 'query',
        'format': 'json',
        'prop': 'revisions',
        'titles': f'{qid}',
        'rvslots': '*',  # Ceci permet de récupérer le contenu complet de la révision
        'rvprop': 'ids|timestamp|user|comment|content',
        'rvlimit': '1',
        'rvdir': 'older',
        'rvstart': end_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'formatversion': '2'
    }
    response = requests.get(api_url, params=params)
    data = response.json()
    pages = data.get('query', {}).get('pages', [])

    for page in pages:
        for rev in page.get('revisions', []):
            timestamp = rev['timestamp']
            content = rev['slots']['main']['content']
            revision_data = json.loads(content)


            if 'labels' in revision_data or 'aliases' in revision_data:
                for lang, label_info in revision_data['labels'].items():
                    print(f"Label en {lang}: {label_info['value']}")

                    # Vérifier les modifications des alias
            if 'aliases' in revision_data and revision_data['aliases'] is dict:
                for lang, aliases in revision_data['aliases'].items():
                    for alias in aliases:
                        print(f"Alias en {lang}: {alias['value']}")

            if 'claims' in revision_data and 'P31' in revision_data['claims']:
                for claim in revision_data['claims']['P31']:
                    p31_value = claim['mainsnak']['datavalue']['value']['id']
                    print(f"Instance de (P31): {p31_value}")

                return timestamp, revision_data  # ou toute autre information pertinente
    return None



# Appliquez cette fonction à chaque QID récupéré
for qid in spacecraft_qids:
    changes = check_recent_changes(qid)
    if changes:
        revision_data = changes[1]
        print(f"QID {qid}: has Changes :")
        if 'labels' in revision_data: print(f"    labels : {revision_data['labels']}")
        if 'aliases' in revision_data: print(f"    aliases : {revision_data['aliases']}")
        if 'claims' in revision_data and 'P31' in revision_data['claims']:
            print(f"    claims['P31'] :")
            for e in revision_data['claims']['P31']:
                print(f"        {e}")
    else:
        print(f"QID {qid}: No recent relevant changes found")