"""
NoirlabExtractor scraps the NOIRLab webpage and stores data into a dictionary.
The output dictionary is compatible with the ontology mapping (see graph.py).
NOIRLab menu bar provides a cascade of observation facilities (Observatory > Telescope > )

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

URL = "https://noirlab.edu/public/programs"
# TODO