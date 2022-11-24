from geopy.geocoders import Nominatim
import json

OBS_ID_JSON ='/Users/ldebisschop/Documents/GitHub/FacilityList/data/IAU-MPC/scripts_py/non_trouves.json'
OBS_ADRESS_JSON ='/Users/ldebisschop/Documents/GitHub/FacilityList/data/IAU-MPC/scripts_py/obs_address.json'

geolocator = Nominatim(user_agent="Laura")

with open(OBS_ID_JSON, 'r') as read_file:
    data = json.load(read_file)


obs_address = {}
num = 0
for entry in data:
    num += 1
    if num > 100:
        break
    observatory_name = entry['Name']
    location = geolocator.geocode(observatory_name)
    if location is None:
        print(f'j ai rien trouve pour {observatory_name}')
        continue
    print(f'c bon pour {observatory_name} !')
    obs_address[observatory_name] = {
        'address': location.address,
        'lon': location.longitude,
        'lat': location.latitude
    }


with open(OBS_ADRESS_JSON, 'w') as write_file:
    json.dump(obs_address, write_file, ensure_ascii=False, indent=4)