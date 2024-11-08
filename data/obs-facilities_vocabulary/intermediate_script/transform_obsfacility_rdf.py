import json
from rdflib import Graph, URIRef, Literal, Namespace, RDF
import os
from langdetect import detect
import re
from tqdm import tqdm  # Import tqdm for the progress bar

# Input JSON file and output RDF file
input_file = '/Users/ldebisschop/PycharmProjects/FacilityList/data/obs-facilities_vocabulary/obsfacilities_vocabulary.json'
output_file = 'obsfacilities_vocabulary.ttl'

# Define namespaces with compact prefix handling
OBS = Namespace("http://example.org/resource/obs#")
OWL = Namespace("http://www.w3.org/2002/07/owl#")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
SKOS = Namespace("http://www.w3.org/2004/02/skos/core#")

# Function to detect language with fallback to default
def detect_language(text, default="en"):
    try:
        return detect(text)
    except:
        return default

# Function to clean URIs
def clean_uri(uri):
    # Replace | with -, " with nothing, and ,, with -
    uri = re.sub(r"\|", "-", uri)
    uri = re.sub(r'"', "", uri)
    uri = re.sub(r",,", "-", uri)
    uri = re.sub(r"\$", "dollar-", uri)  # Handle special characters like $
    return uri

# Load JSON data
with open(input_file, 'r', encoding='utf-8') as f:
    data = json.load(f)

# Initialize RDF graph
g = Graph()
g.bind("obs", OBS)  # Bind namespace prefix for obs
g.bind("owl", OWL)
g.bind("rdfs", RDFS)
g.bind("skos", SKOS)

# Iterate over items in JSON and populate the RDF graph with a progress bar
for item in tqdm(data, desc="Processing items", unit="item"):
    # Clean and use `@id` as the subject for each item
    subject_id = clean_uri(item['@id'])
    subject = OBS[subject_id]  # This will use obs:subject_id format in the output

    # Add owl:Class type
    g.add((subject, RDF.type, OWL.Class))

    # Add rdfs:label with detected language tag
    if "rdfs:label" in item:
        lang = detect_language(item["rdfs:label"])
        g.add((subject, RDFS.label, Literal(item["rdfs:label"], lang=lang)))

    # Add rdfs:comment with detected language tag
    if "rdfs:comment" in item:
        lang = detect_language(item["rdfs:comment"])
        g.add((subject, RDFS.comment, Literal(item["rdfs:comment"], lang=lang)))

    # Add skos:altLabel with detected language tag for each label
    for alt_label in item.get("skos:altLabel", []):
        lang = detect_language(alt_label)
        g.add((subject, SKOS.altLabel, Literal(alt_label, lang=lang)))

    # Add skos:exactMatch with cleaned URIRefs
    for exact_match in item.get("skos:exactMatch", []):
        cleaned_uri = clean_uri(exact_match)
        g.add((subject, SKOS.exactMatch, URIRef(cleaned_uri)))

# Serialize the graph to a Turtle file
g.serialize(destination=output_file, format="turtle")

print(f"The RDF file with cleaned URIs has been saved as: {os.path.abspath(output_file)}")
