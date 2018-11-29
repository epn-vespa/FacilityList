"""
Available parsers in this file:
- load_aas_list
- load_ads_list
- load_dsn_list
- load_iraf_list
- load_mpc_gavo_list
- load_mpc_list
- load_naif_list
- load_nssdc_list
- load_ppi_list
- load_telwiserep_list
- load_xephem_list
"""

from astropy.io import votable
from urllib2 import URLError, HTTPError
import json
import os.path
import urllib2
import pyvo
import consts

import warnings
from astropy.utils.exceptions import AstropyWarning
warnings.simplefilter('ignore', category=AstropyWarning)

# directory containing raw input lists
# is usually overwritten via setDataDir, but "data/" is default
data_dir = "data/"

# add all values that occur in "MeasurementType" here that ought to be replaced by their corresponding UCD
MeasurementType2UCD = {
    "radiowave": "em.radio",
    "radio": "em.radio",
    "optical": "em.opt",
    "gamma-ray": "em.gamma",
    "microwaves": "em.mm.200-400GHz",
    "microwave": "em.mm.200-400GHz",
    "infrared": "em.IR",
    "submillimeter": "em.mm",
    "ultraviolet": "em.UV",
    "radiowaves": "em.radio",
    "x-ray": "em.X-ray",
    "particles": "phys.particle",
    "millimeter": "em.mm"
}


def set_data_dir(directory):
    global data_dir
    data_dir = directory


# replace single measurementType value with respective UCD
def translate_ucd(measurement_type):
    if measurement_type.lower() in MeasurementType2UCD.keys():
        return MeasurementType2UCD[measurement_type.lower()]
    else:
        return measurement_type


# replace all measurementType values of a given object
def replace_ucd_in_json(input_obj):
    print "Replacing values in 'measurementType' with respective UCD..."
    for obj in input_obj:
        if consts.KEY_STR_MEASUREMENT_TYPE in input_obj[obj]:
            for mt_index, mt_value in enumerate(input_obj[obj][consts.KEY_STR_MEASUREMENT_TYPE]):
                input_obj[obj][consts.KEY_STR_MEASUREMENT_TYPE][mt_index] = translate_ucd(mt_value)
    return input_obj


def fetch_list_from_tap(tap_url, schema_name):
    service = pyvo.dal.TAPService(tap_url)
    query = "SELECT * from {}".format(schema_name)
    return service.search(query)


def load_existing_json(json_file):
    if json_file.startswith('http://') or json_file.startswith('https://'):
        # URL (web service) has been provided
        try:
            print("Retrieving data from web service: " + json_file)
            response = urllib2.urlopen(urllib2.Request(json_file))
            return replace_ucd_in_json(json.load(response))
        except (URLError, HTTPError) as e:
            print "ERROR reading data from web service:"
            print(e.reason)
            return {}
    else:
        # filename has been provided
        if os.path.isfile(data_dir + json_file):
            print("Loading existing JSON file: " + json_file)
            return replace_ucd_in_json(json.load(open(data_dir + json_file)))
        else:
            print("WARNING: JSON file '" + json_file + "' does not exist!")
            return {}


"""
The following functions are the load_****_list() 
"""


def load_aas_list():
    """
    This function loads the AAS list in VOTable format as available from=
    https://raw.githubusercontent.com/epn-vespa/FacilityList/master/data/AAS.xml
    :return data: (dict)
    """

    # Authority name for this list
    authority = 'aas'

    # Data file
    list_file = data_dir + 'AAS.xml'

    # Loading data as a VOTable
    input_data = votable.parse(list_file).get_first_table().to_table(use_names_over_ids=True)

    # List with observation ranges
    obs_range_types = ['Gamma-Ray', 'X-Ray', 'Ultraviolet', 'Optical', 'Infrared', 'Millimeter', 'Radio', 'Particles']

    # Initializing data dictionary
    data = dict()

    # Iterating on records of the input list
    for record in input_data:

        # Initializing temporary dictionaries for this record
        data_tmp = dict()
        data_tmp[consts.KEY_STR_ALTERNATE_NAME] = []
        altname_tmp = dict()

        # record title is ID column here
        title = record['ID'].strip()

        # Alternate_name element
        altname_tmp[consts.KEY_STR_NAME] = record['Name'].strip()
        altname_tmp[consts.KEY_STR_ID] = title
        altname_tmp[consts.KEY_STR_NAMING_AUTHORITY] = authority
        data_tmp[consts.KEY_STR_ALTERNATE_NAME].append(altname_tmp)

        # Location (space, airborne or ground), with continent on ground
        if record['Location'] == 'Space':
            data_tmp[consts.KEY_STR_FACILITY_TYPE] = 'spacecraft'
        elif record['Location'] == 'Airborne':
            data_tmp[consts.KEY_STR_FACILITY_TYPE] = 'airborne'
        else:
            data_tmp[consts.KEY_STR_FACILITY_TYPE] = 'observatory'
            data_tmp[consts.KEY_STR_LOCATION] = dict()
            data_tmp[consts.KEY_STR_LOCATION][consts.KEY_STR_CONTINENT] = record['Location'].\
                strip().replace('&', 'and')  # Quick fix for continent names containing '&'

        # Observation Range
        for obs_type in obs_range_types:
            if record[obs_type].strip() != "\xc2\xa0":
                if consts.KEY_STR_MEASUREMENT_TYPE in data_tmp.keys():
                    data_tmp[consts.KEY_STR_MEASUREMENT_TYPE].append(translate_ucd(obs_type))
                else:
                    data_tmp[consts.KEY_STR_MEASUREMENT_TYPE] = [translate_ucd(obs_type)]

        # Special case if the Solar keyword is
        if record['Solar'].strip() != "\xc2\xa0":
            data_tmp[consts.KEY_STR_TARGET_LIST] = ['Sun']

        # adding temporary record to main data dictionary
        data[authority+":"+title] = data_tmp

    return data


def load_ppi_list():
    """
    This function loads the PDS/PPI list in JSON format
    This list is curated, and thus multiple spacecraft appear several times: an initial pre-merge is done on the fly
    :return data: (dict)
    """

    # Authority name for this list
    authority = 'pds-ppi'

    # Data file
    list_file = data_dir + 'pds-ppi-spacecraft.json'

    # Loading data as a JSON file
    with open(list_file) as data_file:
        input_data = json.load(data_file)

    # Initializing data dictionary
    data = dict()

    # Iterating on records of the input list
    for record in input_data['response']['docs']:

        # Current record spacecraft name
        current_sc = authority+":"+record['SPACECRAFT_NAME'][0]

        # if the current spacecraft object is not in the existing data dictionary, initialize it
        if current_sc not in data.keys():
            data[current_sc] = dict()
            data[current_sc][consts.KEY_STR_ALTERNATE_NAME] = []
            data[current_sc][consts.KEY_STR_FACILITY_GROUP] = []
            data[current_sc][consts.KEY_STR_INSTRUMENT_LIST] = []
            data[current_sc][consts.KEY_STR_TARGET_LIST] = []
            data[current_sc][consts.KEY_STR_FACILITY_TYPE] = 'spacecraft'

        if 'SPACECRAFT_NAME' in record.keys():
            for altname_item in record['SPACECRAFT_NAME']:
                altname_tmp_list = []
                for item in data[current_sc][consts.KEY_STR_ALTERNATE_NAME]:
                    altname_tmp_list.append(item[consts.KEY_STR_NAME])
                if altname_item not in altname_tmp_list:
                    altname_tmp = dict()
                    altname_tmp[consts.KEY_STR_NAME] = altname_item
                    altname_tmp[consts.KEY_STR_NAMING_AUTHORITY] = authority
                    data[current_sc][consts.KEY_STR_ALTERNATE_NAME].append(altname_tmp)

        if 'MISSION_NAME' in record.keys():
            if record['MISSION_NAME'] not in data[current_sc][consts.KEY_STR_FACILITY_GROUP]:
                data[current_sc][consts.KEY_STR_FACILITY_GROUP].append(record['MISSION_NAME'])

        if 'INSTRUMENT_NAME' in record.keys():
            for instrum_item in record['INSTRUMENT_NAME']:
                instrum_tmp_list = []
                for item in data[current_sc][consts.KEY_STR_INSTRUMENT_LIST]:
                    instrum_tmp_list.append(item['name'])
                if instrum_item not in instrum_tmp_list:
                    ii = record['INSTRUMENT_NAME'].index(instrum_item)
                    instrum_tmp = dict()
                    instrum_tmp['name'] = instrum_item
                    instrum_tmp['id'] = record['INSTRUMENT_ID'][ii]
                    data[current_sc]['instrumentList'].append(instrum_tmp)

        if 'TARGET_NAME' in record.keys():
            for target_item in record['TARGET_NAME']:
                target_item = target_item.strip()
                if target_item not in data[current_sc][consts.KEY_STR_TARGET_LIST]:
                    data[current_sc][consts.KEY_STR_TARGET_LIST].append(target_item)

    return data


def load_ads_list():
    authority = 'ads'
    list_file = data_dir + 'ADS_facilities.txt'
    with open(list_file,'r') as data_file:
        input = data_file.readlines()

    data = dict()
    for record in input:
        data_tmp = dict()
        data_tmp[consts.KEY_STR_ALTERNATE_NAME] = []
        title = record[0:16].strip()
        altname_tmp = dict()
        altname_tmp[consts.KEY_STR_NAMING_AUTHORITY] = authority
        altname_tmp[consts.KEY_STR_ID] = title
        altname_tmp[consts.KEY_STR_NAME] = record[16:].strip()
        data_tmp[consts.KEY_STR_ALTERNATE_NAME].append(altname_tmp)
        if '/' in record[16:]:
            data_tmp[consts.KEY_STR_FACILITY_GROUP] = [record[16:].split('/')[0]]
        if title[0:3] == 'Sa.':
            data_tmp[consts.KEY_STR_FACILITY_TYPE] = 'spacecraft'
        else:
            data_tmp[consts.KEY_STR_FACILITY_TYPE] = 'observatory'
        data[authority+":"+title] = data_tmp
    return data


def load_nssdc_list():
    authority = 'nssdc'
    list_file = data_dir + 'NSSDC.xml'
    input_data = votable.parse(list_file).get_first_table().to_table(use_names_over_ids=True)

    data = dict()

    for record in input_data:
        data_tmp = dict()
        data_tmp[consts.KEY_STR_ALTERNATE_NAME] = []
        altname_tmp = dict()

        title = record['NSSDC id']
        altname_tmp[consts.KEY_STR_NAME] = record['name']
        altname_tmp[consts.KEY_STR_ID] = title
        altname_tmp[consts.KEY_STR_NAMING_AUTHORITY] = authority
        data_tmp[consts.KEY_STR_ALTERNATE_NAME].append(altname_tmp)

        data_tmp[consts.KEY_STR_REFERENCE_URL] = []
        refurl_tmp = dict()
        refurl_tmp['url'] = record['URL']
        refurl_tmp['title'] = 'NSSDC catalog entry'
        data_tmp[consts.KEY_STR_REFERENCE_URL].append(refurl_tmp)
        data_tmp[consts.KEY_STR_FACILITY_TYPE] = 'spacecraft'
        if record['Launch date'] != "":
            data_tmp[consts.KEY_STR_LAUNCH_DATE] = record['Launch date']

        data[authority+":"+title] = data_tmp

    return data


def load_xephem_list():

    authority = 'xephem'

    list_file = data_dir + 'xephem_sites.txt'
    with open(list_file, 'r') as data_file:
        input_list = data_file.readlines()

    data = dict()

    for record in input_list:
        if record[0] != '#':
            record = ' '.join(record.split())
            data_tmp = dict()
            data_tmp[consts.KEY_STR_ALTERNATE_NAME] = []

            items = record.split(';')
            rec_name = items[0]
# Removing detection of location.country
#            if ',' in items[0]:
#                rec_tmp = items[0].split(',')
#                rec_name = rec_tmp[0].strip()
#                rec_location = ', '.join(rec_tmp[1:]).strip()
#            else:
#                rec_name = items[0]
#                rec_location = ''

            rec_lat_txt = items[1].strip().split(' ')
            rec_lat = float(rec_lat_txt[0])+float(rec_lat_txt[1])/60.+float(rec_lat_txt[2])/3600.
            if rec_lat_txt[3] == 'S':
                rec_lat = - rec_lat
            rec_lon_txt = items[2].strip().split(' ')
            rec_lon = float(rec_lon_txt[0])+float(rec_lon_txt[1])/60.+float(rec_lon_txt[2])/3600.
            if rec_lon_txt[3] == 'E':
                rec_lon = - rec_lon
            rec_alt_txt = items[3].strip()
            rec_alt = float(rec_alt_txt)

            title = rec_name.strip()
            altname_tmp = dict()
            altname_tmp[consts.KEY_STR_NAME] = title
            altname_tmp[consts.KEY_STR_NAMING_AUTHORITY] = authority
            data_tmp[consts.KEY_STR_ALTERNATE_NAME].append(altname_tmp)
            data_tmp[consts.KEY_STR_FACILITY_TYPE] = 'observatory'
            data_tmp[consts.KEY_STR_LOCATION] = dict()
#            if rec_location != '':
#                data_tmp['location']['country'] = rec_location
            data_tmp[consts.KEY_STR_LOCATION][consts.KEY_STR_COORDINATES] = dict()
            data_tmp[consts.KEY_STR_LOCATION][consts.KEY_STR_COORDINATES][consts.KEY_STR_LAT] = rec_lat
            data_tmp[consts.KEY_STR_LOCATION][consts.KEY_STR_COORDINATES][consts.KEY_STR_LON] = rec_lon
            if rec_alt != -1.:
                data_tmp[consts.KEY_STR_LOCATION][consts.KEY_STR_COORDINATES][consts.KEY_STR_ALT] = rec_alt

            data[authority+":"+title] = data_tmp

    return data


def load_naif_list():
    id_count = 0
    authority = 'naif'
    list_file = data_dir + 'NAIF.xml'
    input_data = votable.parse(list_file).get_first_table().to_table(use_names_over_ids=True)

    data = dict()

    for record in input_data:

        data_tmp = dict()
        data_tmp['alternateName'] = []
        altname_tmp = dict()

        title = record['NAIF ID'].strip()
        altname_tmp['name'] = record['NAIF name'].strip()
        altname_tmp['id'] = title
        altname_tmp['namingAuthority'] = authority

        if title in data.keys():
            data[authority+":"+title]['alternateName'].append(altname_tmp)
        else:
            data_tmp['alternateName'].append(altname_tmp)
            data[authority+":"+str(id_count)+":"+title] = data_tmp
        id_count += 1

    return data


def load_mpc_gavo_list():
    authority = 'gavo-mpc'
    tap_url = "http://dc.zah.uni-heidelberg.de/tap"
    schema_name = "obscode.data"
    input = fetch_list_from_tap(tap_url, schema_name)

    data = dict()
    for record in input:
        data_tmp = dict()
        data_tmp['alternateName'] = []

        title = str(record['code'])
        obs_lon = float(record['long'])
        obs_cos = float(record['cosphip'])
        obs_sin = float(record['sinphip'])
        obs_gd_lat = float(record['gdlat'])
        obs_gc_lat = float(record['gclat'])
        obs_height = float(record['height'])
        obs_name = str(record['name'])

        data_tmp['facilityType'] = 'observatory'
        data_tmp['location'] = dict()
        data_tmp['location']['coordinates'] = dict()
        data_tmp['location']['coordinates']['lon'] = obs_lon
        data_tmp['location']['coordinates']['lat'] = obs_gc_lat  # We use Geocentric latitude here
        data_tmp['location']['coordinates']['alt'] = obs_height

        altname_tmp = dict()
        altname_tmp['namingAuthority'] = authority
        altname_tmp['id'] = title
        altname_tmp['name'] = obs_name
        data_tmp['alternateName'].append(altname_tmp)
        data[authority+":"+title] = data_tmp

    return data


def load_mpc_list():
    authority = 'iau-mpc'
    list_file = data_dir + 'IAU-MPC.txt'
    with open(list_file,'r') as data_file:
        input = data_file.readlines()

    data = dict()
    for record in input[1:]:
        data_tmp = dict()
        data_tmp['alternateName'] = []

        title = record[0:3].strip()
        obs_lon_txt = record[4:13].strip()
        obs_cos_txt = record[13:21].strip()
        obs_sin_txt = record[21:30].strip()
        if obs_lon_txt == '':
            data_tmp['facilityType'] = 'spacecraft'
        else:
            data_tmp['facilityType'] = 'observatory'
            obs_lon = float(record[4:13].strip())
            obs_cos = float(record[13:21].strip())
            obs_sin = float(record[21:30].strip())
            data_tmp['location'] = dict()
            data_tmp['location']['coordinates'] = dict()
            data_tmp['location']['coordinates']['lon'] = obs_lon
            data_tmp['location']['coordinates']['cos'] = obs_cos
            data_tmp['location']['coordinates']['sin'] = obs_sin
        name = record[30:].strip()
        altname_tmp = dict()
        altname_tmp['namingAuthority'] = authority
        altname_tmp['id'] = title
        altname_tmp['name'] = name
        data_tmp['alternateName'].append(altname_tmp)
        data[authority+":"+title] = data_tmp

    return data


def load_iraf_list():
    authority = 'iraf'
    list_file = data_dir + 'IRAF.txt'
    with open(list_file,'r') as data_file:
        input = data_file.readlines()

    nlines = len(input)
    data = dict()

    for irec in range(nlines):
        record = input[irec]
        if record[0:3] == 'obs':
            data_tmp = dict()
            data_tmp['alternateName'] = []
            title = record.split(' = ')[1].strip().strip('"')
            obs_name = ''
            obs_lon = '0'
            obs_lat = '0'
            obs_alt = '0'
            obs_tz = '0'

            for i in range(5):
                iirec = irec+i+1
                if input[iirec][1:4] == 'nam':
                    obs_name = input[iirec].split(' = ')[1].strip().strip('"')
                if input[iirec][1:4] == 'lon':
                    obs_lon = input[iirec].split(' = ')[1].strip()
                if input[iirec][1:4] == 'lat':
                    obs_lat = input[iirec].split(' = ')[1].strip()
                if input[iirec][1:4] == 'alt':
                    obs_alt = float(input[iirec].split(' = ')[1].strip())
                if input[iirec][1:4] == 'tim':
                    obs_tz = float(input[iirec].split(' = ')[1].strip())

            altname_tmp = dict()
            altname_tmp['namingAuthority'] = authority
            altname_tmp['id'] = title
            altname_tmp['name'] = obs_name
            data_tmp['alternateName'].append(altname_tmp)

            if ':' in obs_lon:
                obs_lon_tmp = obs_lon.split(':')
                obs_lon = float(obs_lon_tmp[0])+float(obs_lon_tmp[1])/60.
                if len(obs_lon_tmp) == 3:
                    obs_lon = obs_lon + float(obs_lon_tmp[2])/3600.
            else:
                obs_lon = float(obs_lon)

            if ':' in obs_lat:
                obs_lat_tmp = obs_lat.split(':')
                obs_lat = float(obs_lat_tmp[0])+float(obs_lat_tmp[1])/60.
                if len(obs_lat_tmp) == 3:
                    obs_lat = obs_lat + float(obs_lat_tmp[2])/3600.
            else:
                obs_lat = float(obs_lat)

            data_tmp['location'] = dict()
            data_tmp['location']['coordinates'] = dict()
            data_tmp['location']['coordinates']['lon'] = obs_lon
            data_tmp['location']['coordinates']['lat'] = obs_lat
            data_tmp['location']['coordinates']['alt'] = obs_alt
            data_tmp['location']['coordinates']['tz'] = obs_tz

            data_tmp['facilityType'] = 'observatory'

            data[authority+":"+title] = data_tmp

    return data


def load_dsn_list():
    authority = 'dsn'
    list_file = data_dir + 'DSN.txt'
    with open(list_file,'r') as data_file:
        input = data_file.readlines()

    data = dict()
    for record in input:
        data_tmp = dict()
        data_tmp['alternateName'] = []
        rec_items = record.strip().split()
        id = rec_items[0]
        name = ' '.join(rec_items[1:]).strip("'")

        title = id
        altname_tmp = dict()
        altname_tmp['namingAuthority'] = authority
        altname_tmp['id'] = title
        altname_tmp['name'] = name
        data_tmp['alternateName'].append(altname_tmp)

        data[authority+":"+title] = data_tmp

    return data


def load_telwiserep_list():
    authority = 'wiserep'
    list_file = data_dir + 'Tel_WISERep.dat'

    with open(list_file, 'r') as data_file:
        input = data_file.readlines()

    data = dict()
    for record in input:

        if record.startswith('#'):
            pass
        else:
            data_tmp = dict()
            items = [cur.strip() for cur in record.split('|')]
            id = items[0]
            title = items[1]

            altname_tmp = dict()
            altname_tmp['namingAuthority'] = authority
            altname_tmp['id'] = id
            altname_tmp['name'] = title
            data_tmp['alternateName'] = [altname_tmp]

            altname_tmp = dict()
            altname_tmp['namingAuthority'] = authority
            altname_tmp['id'] = id
            altname_tmp['name'] = items[2]
            data_tmp['alternateName'].append(altname_tmp)

            data_tmp['location'] = dict()
            data_tmp['location']['coordinates'] = dict()
            data_tmp['location']['coordinates']['lat'] = float(items[3])
            data_tmp['location']['coordinates']['lon'] = float(items[4])
            if items[5] != "":
                data_tmp['location']['coordinates']['alt'] = float(items[5])

            data_tmp['facilityType'] = 'observatory'
            if items[8] == "Satellite/Spacecraft":
                data_tmp['facilityType'] = 'spacecraft'
            else:
                if items[8] != "":
                    data_tmp['facilityGroup'] = [items[8]]

            data_tmp['referenceURL'] = []
            refurl_tmp = dict()
            refurl_tmp['url'] = items[7]
            refurl_tmp['title'] = 'Homepage'
            data_tmp['referenceURL'].append(refurl_tmp)

            data[authority+":"+id] = data_tmp

    return data

