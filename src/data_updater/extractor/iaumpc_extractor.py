"""
IauMpcExtractor scraps the IauMPC webpage and stores data into a dictionary.
The output dictionary is compatible with the ontology mapping (see graph.py).

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import astropy
import astropy.coordinates
from bs4 import BeautifulSoup
from data_updater import entity_types
from data_updater.extractor.cache import CacheManager
from data_updater.extractor.extractor import Extractor
from utils.utils import cut_location
import math


class IauMpcExtractor(Extractor):
    URL = "https://www.minorplanetcenter.net/iau/lists/ObsCodes.html"
    # Explained data:
    # https://www.minorplanetcenter.net/iau/lists/ObsCodesF.html

    # URI to save this source as an entity
    URI = "IAU-MPC_list"

    # URI to save entities from this source
    NAMESPACE = "iaumpc"

    # Folder name to save cache/ and data/
    CACHE = "IAU-MPC/"

    # Default type used for all unknown types in this resource
    DEFAULT_TYPE = entity_types.GROUND_OBSERVATORY

    # List's types.
    # For merging strategies. Prevent merging data from lists
    # that do not have types in common
    POSSIBLE_TYPES = {entity_types.GROUND_OBSERVATORY,
                      entity_types.SPACECRAFT}

    # No need to disambiguate the type with LLM.
    # Useful for merging strategy: when the type is ambiguous,
    # it is recommanded to not discriminate on types.
    # 1: always known.
    # 0.5: partially known (see individuals)
    # 0: never known.
    TYPE_KNOWN = 1

    # Used to split the label into entity / location
    LOCATION_DELIMITER = ","


    def __init__(self):
        pass


    def __str__(self):
        return self.NAMESPACE


    def extract(self) -> dict:
        """
        Extract the page content into a dictionary.
        """
        content = CacheManager.get_page(IauMpcExtractor.URL,
                                        list_name = self.CACHE)

        if not content:
            return dict()

        soup = BeautifulSoup(content, "html.parser")
        text = soup.find('pre').get_text()

        result = dict()
        lines = text.split('\n')
        for line in lines[2:]:  # Ignore two first lines (col names & empty line)
            #C15 132.1656 0.72418 +0.68737 ISON-Ussuriysk Observatory
            data = dict()

            obs_id = line[0:3].strip()
            obs_name = line[30:].strip()
            # obs_name = del_aka(obs_name)

            if not obs_name:
                continue # ignore data with no name

            alt_labels = set()

            # location
            _, location = cut_location(obs_name,
                                       delimiter = self.LOCATION_DELIMITER)
            if location:
                data["location"] = location # city, lake...

            # longitude
            line = line[3:30]
            longitude = line[:10].strip()
            if longitude:
                longitude = float(longitude)
                #if longitude > 180:
                #    longitude -= 360
                data["longitude"] = longitude # keep a float

            # latitude
            cosinus = line[10:18].strip() # geocentric
            sinus = line[18:].strip() # geocentric
            if sinus and cosinus:
                sinus = float(sinus)
                cosinus = float(cosinus)
                # geocentric latitude
                # sin and cos are ρsinφ′ and ρcosφ′ρ
                # φ′= arctan(ρcosφ′ρsinφ′​)
                # see https://www.minorplanetcenter.net/iau/lists/ObsCodesF.html

                # old method was wrong because it computed a geocentric latitude:
                # latitude_rad = math.atan2(float(sinus), float(cosinus))
                # latitude_deg = math.degrees(latitude_rad)
                earth_radius = 6378137 # meters

                # rho is the earth radius at cos&sin
                # rho = math.sqrt(math.pow(cosinus, 2) + math.pow(sinus, 2)) * earth_radius

                el = astropy.coordinates.EarthLocation(earth_radius * cosinus * math.cos(math.radians(longitude)),
                                                       earth_radius * cosinus * math.sin(math.radians(longitude)),
                                                       earth_radius * sinus,
                                                       unit = "m")
                data["latitude"] = float(el.to_geodetic().lat.deg) # lat.deg => numpy.float64
                data["type"] = entity_types.GROUND_OBSERVATORY
            else:
                data["type"] = entity_types.SPACECRAFT
            data["type_confidence"] = 1

            # alt labels
            data["alt_label"] = alt_labels

            # label
            data["label"] = obs_name

            # Internal references
            if obs_id:
                data["code"] = obs_id # non-ontological identifier

            result[obs_name] = data
        return result

if __name__ == "__main__":
    pass
