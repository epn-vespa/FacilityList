"""
Script that takes an SSSOM ontology and some annotation TSV file(s)
and computes accuracy scores.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
from argparse import ArgumentParser
from rdflib import Graph, URIRef
from urllib.parse import quote
import re


def parse_annotation_file(tsv):
    with open(tsv, 'r') as file:
        lines = file.readlines()
        result = []
        # Find lines with annotations
        header = None
        for line in lines:
            # Find header
            if not header:
                if "\tEntity1\t" in line and "\tEntity2\t" in line:
                    header = line
                    comments = line.find("Comments")
                    n_annotation_cols = line[:comments].count("\t")
                    print("cols before comments:", n_annotation_cols)
                continue
            line = line.split('\t')
            annotations = line[:n_annotation_cols]
            print("annotations=", annotations)
            if annotations:
                x = annotations.count('x')
                o = annotations.count('o')
                if x == o:
                    print(f"Same amount of Xs {x} and Os {o}. Ignoring.")
                    continue
                # comment = line[n_annotation_cols]
                entity1 = line[n_annotation_cols + 1]
                entity2 = line[n_annotation_cols + 2]
                # Filter out http & https
                entity1 = re.sub(r"https?://[^\b]+", "", entity1).strip()
                entity2 = re.sub(r"https?://[^\b]+", "", entity2).strip()
                result.append((entity1, entity2, o > x))
        return result


def standardize_uri(label: str) -> str:
    """
    Creates a valid uri string from a label using lowercase and hyphens
    between words.

    Keyword arguments:
    label -- the label of the entity.
    """
    label = label.lower()
    label = re.sub(r"[^\w\s\.]", ' ', label)
    label = re.sub(r"\s+", ' ', label) # Remove multiple spaces
    label = label.split(' ')
    label = '-'.join([l for l in label if l])
    label = quote(label)
    return label

def main(tsv: list[str], sssom: str):
    g = Graph()
    g.parse(sssom)
    g.bind("sssom", "https://w3id.org/sssom/", override = False)
    for t in tsv:
        annotations = parse_annotation_file(t)
        list1, list2 = t.split('-')
        list1 = list1.rsplit('/', 1)[-1].lower()
        list2 = list2.rsplit('.', 1)[0].lower()
        namespace1 = f"https://voparis-ns.obspm.fr/rdf/obsfacilities/{list1}#"
        namespace2 = f"https://voparis-ns.obspm.fr/rdf/obsfacilities/{list2}#"
        g.bind(list1, namespace1, override = False)
        g.bind(list2, namespace2, override = False)

        TP = 0
        FP = 0
        TN = 0
        FN = 0
        FN_pairs = []
        for entity1, entity2, same in annotations:
            uri1 = URIRef(namespace1 + standardize_uri(entity1))
            uri2 = URIRef(namespace2 + standardize_uri(entity2))
            query = f"""
            SELECT ?mapping WHERE {{
                ?mapping a sssom:Mapping .
                ?mapping sssom:predicate_id skos:exactMatch .
                {{
                    ?mapping sssom:subject_id <{uri1}> .
                    ?mapping sssom:object_id <{uri2}> .
                }} UNION
                {{
                    ?mapping sssom:subject_id <{uri2}> .
                    ?mapping sssom:object_id <{uri1}> .
                }}
            }}
            """
            res = g.query(query)
            has_exact_match = False
            for _ in res:
                has_exact_match = True
            if has_exact_match:
                if same:
                    TP += 1
                else:
                    FP += 1
            else:
                if same:
                    FN += 1
                    FN_pairs.append((entity1, entity2))
                else:
                    TN += 1
            print(has_exact_match, same)
        
        print("TP:", TP)
        print("TN:", TN)
        print("FP:", FP)
        print("FN:", FN)
        print(FN_pairs)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-t", "--tsv",
                        nargs = '+',
                        required = True,
                        type = str,
                        help = "TSV file(s) with gold annotations: entity1 URI, entity2 URI, annotation 'x' or 'o'. The namespaces of entity1 & entity2 are retrieved from the filename.")
    parser.add_argument("-s", "--sssom",
                        required = True,
                        type = str,
                        help = "SSSOM ontology to evaluate.")
    args = parser.parse_args()
    main(args.tsv, args.sssom)
