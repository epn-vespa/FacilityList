#!/bin/python3

"""
This script scraps the AAS webpage and stores data into a dictionary.
This dictionary is compatible with the ontology merger (merge.py).

"""

import requests
from bs4 import BeautifulSoup
import json


class AasExtractor():

    URL = "https://journals.aas.org/author-resources/aastex-package-for-manuscript-preparation/facility-keywords/"

    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
        AppleWebKit/537.36 (KHTML, like Gecko) \
        Chrome/58.0.3029.110 \
        Safari/537.3'}

    def get_source(self) -> str:
        return AasExtractor.URL

    def get_community(self) -> str:
        return "" # TODO (heliophysics / astronomy / planetology)

    def extract(self) -> dict:
        try:
            response = requests.get(
                    AasExtractor.URL,
                    headers = AasExtractor.HEADERS)
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)

        if response.ok:
            soup = BeautifulSoup(response.text, 'html.parser')
            rows = soup.find('tbody').find_all('tr')
            headers = soup.find('thead').find('tr')
            headers = [header.text.strip().lower()
                    for header
                    in headers.find_all('th')]

            # Wavelengths and facility types column indexes
            WL = 3
            FT = 10
            wavelengths = headers[WL:FT]
            facility_types = headers[FT:]
            # If you need to get wavelengths as measure units:
            # wavelengths measurement units are between parenthesis of columns 3-10.
            # wavelengths = [wl[wl.find('(')+1:wl.find(')')]
            #       for wl in headers[3:10]]

            # Process page's data into a dictionary.
            # This dictionary can then be processed by the ontology merger.
            result = dict()
            for row in rows:
                cols = row.find_all('td')
                cols = [col.text.strip() for col in cols]
                row_data = dict(zip(headers, cols)) # {"h1": "col1", "h2": "col2"}

                data = dict() # Dictionary to save the row's data

                uri = row_data["keyword"]
                location = row_data["location"]
                label = row_data["full facility name"]

                # Add and filter out facility types
                for facility_type, col in zip(facility_types, cols[FT:]):
                    if col:
                        data["type"] = facility_type
                if "type" in data and data["type"] in ["computational center", "archive/database"]:
                    continue # Filter out computational center & archive/database.

                # Add label to row dict
                data["label"] = label

                # Add location to row dict
                data["location"] = location

                # Add wavelength to row dict
                for wavelength, col in zip(wavelengths, cols[WL:FT]):
                    if col:
                        if "wavelength" not in data:
                            data["wavelength"] = [wavelength]
                        else:
                            data["wavelength"].append(wavelength)

                # If there is no identifier (or keyword), find it between ().
                if not uri:
                    uri = label[label.find('(')+1:label.find(')')]

                # Save the row's dict into the result dict
                result[uri] = data
            #print(result)
            return result
        else:
            print(f"Request to {url} failed with status code {response.status_code}")
            return None

if __name__ == "__main__":
    extract()
