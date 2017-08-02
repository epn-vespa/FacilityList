import json
from parsers import *

# filenames
log_file_name = 'logs/types.log'
result_file_name = 'data/matrix_json_export_20170724.json'

# constants for property names
altname_str = 'alternateName'
name_str = 'name'
id_str = 'id'
targetlist_str = 'targetList'
facilitytype_str = 'facilityType'
facilitygroup_str = 'facilityGroup'
location_str = 'location'
continent_str = 'continent'
country_str = 'country'
coordinates_str = 'coordinates'
measurementtype_str = 'measurementType'
referenceurl_str = 'referenceURL'
launchdate_str = 'launchDate'
lon_str = 'lon'
lat_str = 'lat'
alt_str = 'alt'
tz_str = 'tz'
cos_str = 'cos'
sin_str = 'sin'
 						
def analyze_list (list):
		
	# indices of names and ids and locations
	target_index = {}
	continent_index ={}
	country_index = {}
	type_index ={}

	log_file = open( log_file_name, 'w' )

	print ( "Analizing JSON file..." )
	for rec in list:		
		# index for names, ids
		if list[rec].has_key(targetlist_str):
			for target in list[rec][targetlist_str]:					
				if not(target_index.has_key(target)):
					target_index[target] = True
		
		if list[rec].has_key(location_str) and list[rec][location_str].has_key(continent_str):
			if not(continent_index.has_key(list[rec][location_str][continent_str])):
				continent_index[list[rec][location_str][continent_str]] = True
			
		if list[rec].has_key(location_str) and list[rec][location_str].has_key(country_str):
			if not(country_index.has_key(list[rec][location_str][country_str])):
				country_index[list[rec][location_str][country_str]] = True

		if list[rec].has_key(measurementtype_str):
			for type in list[rec][measurementtype_str]:					
				if not(type_index.has_key(type)):
					type_index[type] = True

	log_file.write ("\n************************* CONTINENTS *****************************\n")
	for key in continent_index.keys():
		log_file.write (key + "\n")
		
	log_file.write ("\n************************* MEASUREMENT TYPES *****************************\n")
	for key in type_index.keys():
		log_file.write (key + "\n")
	
	log_file.write ("\n************************* TARGETS *****************************\n")
	for key in target_index.keys():
		log_file.write (key + "\n")
	
	log_file.write ("\n************************* COUNTRIES " + str ( len (country_index.keys()) ) + " *****************************\n")
	for key in country_index.keys():
		log_file.write (key + "\n")


with open(result_file_name) as results_file:    
	#lists = load_aas_list() # use any parser function from parsers.py here if needed
	lists = json.load(results_file)

analyze_list (lists)


