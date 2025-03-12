"""
IauMpcExtractor scraps the IauMPC webpage and stores data into a dictionary.
The output dictionary is compatible with the ontology mapping (see graph.py).

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
from bs4 import BeautifulSoup
from extractor.cache import CacheManager
import math


class IauMpcExtractor():
    URL = "https://www.minorplanetcenter.net/iau/lists/ObsCodes.html"
    # Explained data:
    # https://www.minorplanetcenter.net/iau/lists/ObsCodesF.html

    # URI to save this source as an entity
    URI = "IAU-MPC_list"

    # URI to save entities from this source
    NAMESPACE = "iaumpc"

    # Default type used for all unknown types in this resource
    DEFAULT_TYPE = "observation facility"

    def __init__(self):
        pass

    def extract(self) -> dict:
        """
        Extract the page content into a dictionary.
        """
        content = CacheManager.get_page(IauMpcExtractor.URL)

        if not content:
            return dict()

        soup = BeautifulSoup(content, "html.parser")
        text = soup.find('pre').get_text()

        result = dict()
        lines = text.split('\n')
        for line in lines[2:]:  # Ignorer les deux premières lignes (noms de colonnes et ligne vide)
            #C15 132.1656 0.72418 +0.68737 ISON-Ussuriysk Observatory
            data = dict()

            obs_id = line[0:3].strip()
            obs_name = line[30:].strip()

            if not obs_name:
                continue # ignore data with no name

            # location
            if "," in obs_name:
                comma = obs_name.find(',')
                location = obs_name[comma + 1:].strip()
                data["location"] = location # city, lake...
                data["alt_label"] = [obs_name[:comma].strip()]
            line = line[3:30]
            longitude = line[:10].strip()
            if longitude:
                longitude = float(longitude)
                if longitude > 180:
                    longitude -= 360
                data["longitude"] = str(longitude)
            cosinus = line[10:18].strip()
            sinus = line[18:].strip()
            if sinus and cosinus:
                # geocentric latitude
                # sin and cos are ρsinφ′ and ρcosφ′ρ
                # φ′= arctan(ρcosφ′ρsinφ′​)
                # see https://www.minorplanetcenter.net/iau/lists/ObsCodesF.html
                latitude_rad = math.atan2(float(sinus), float(cosinus))
                latitude_deg = math.degrees(latitude_rad)
                data["latitude"] = str(latitude_deg)
            data["label"] = obs_name
            if obs_id:
                data["code"] = obs_id # non-ontological identifier
            result[obs_name] = data
        return result

if __name__ == "__main__":
    pass
