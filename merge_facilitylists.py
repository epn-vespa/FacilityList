from datetime import datetime
from parsers import *
from pprint import pprint

import json, pickle, copy, re, sys, os.path

# ***************** SCRIPT CONFIGURATION - change with care... ******************

# filenames
result_file_name = 'merged_list.json'
log_file_name = 'name_merge.log'
fuzzy_hints_file_name = 'fuzzy_merge.log'
partial_hints_file_name = 'partial_merge.log'

# directories
conf_dir = "conf/"
logs_dir = "logs/"
output_dir = "output/"
data_dir = "data/"

# precision of longitude/latitude comparison
precision = 3

# *******************************************************************************

def load_all_lists(dir): # see parsers defined in file parsers.py
	setDataDir(dir)
	data = {}
	#data.update(load_aas_list())
	#data.update(load_ppi_list())
	#data.update(load_ads_list())
	data.update(load_nssdc_list())
	#data.update(load_xephem_list())
	data.update(load_naif_list())
	#data.update(load_mpc_list())
	#data.update(load_iraf_list())
	#data.update(load_dsn_list())
	#data.update(load_existing_json("https://matrix.oeaw.ac.at/getModifiedRecords.php?magic=ep2020_tap")) # load json from URL, example
	#data.update(load_existing_json("matrix_json_export_20170731.json")) # load json from file in data directory, example

	return data

# command line inputs
fuzzy = False
interactive = False
partial = False
for arg in sys.argv:
	if arg == "-f" or arg == "-fuzzy": fuzzy = True
	if arg == "-i" or arg == "-interactive": interactive = True
	if arg == "-p" or arg == "-partial": partial = True

if (fuzzy and partial):
	print "Options 'f' or 'fuzzy' and 'p' or 'partial' can not be used at the same time!"
	sys.exit()

if (interactive and not (fuzzy or partial)):
	print "Option 'i' or 'interactive' can only be used in conjunction with 'f' ('fuzzy') or 'p' ('partial') !"
	sys.exit()

# constants for property names
altname_str = 'alternateName'
name_str = 'name'
id_str = 'id'
auth_str = 'namingAuthority'
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

tmp_checked_file_name = '_checked_.json'
_checked_ = {}

def remove_entries (list, entries):
	for elem in entries:
		if elem in list:
			del list[elem]

def write (logfile, message):
	if logfile != None:
		if logfile != False:
			logfile.write("\n" + message)
	else:
		print ( message )

def merge_elements (source, target):
	merged = False
	for item in source:
		if merged: break
		if item in target:
			merged = True
			for item2 in source:
				if not(item2 in target):
					target.append(item2)
			break
	return merged

def trunc(num):
	sp = str(num).split('.')
	if (len(sp) == 1):
		return sp[0]
	else:
		return '.'.join([sp[0], sp[1][:precision]])

def save_checked_file(num_merged):
	_checked_['_timestamp_'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	_checked_['merged_counter'] = num_merged
	checked_file = open( output_dir + tmp_checked_file_name, 'w')
	checked_file.write( json.dumps ( _checked_ ) )
	checked_file.close()

def merge_entries(doubles_array, list, logfile):
	log_text_1 = "* WARNING: differing values for property '"
	log_text_2 = "' for object: '"

	merged_entry = {}
	entry_array = []

	double_strings = ""
	for double in doubles_array:
		elem_copy = copy.copy(list[double])
		elem_copy['original_id'] = double
		entry_array.append(elem_copy)
		double_strings = double_strings + double + ", "

	if logfile != None: write(logfile, "-> Merged entries: " + double_strings[:-2])

	merged_refurls = {}

	for entry in entry_array:

		# merge alternate names
		if entry.has_key(altname_str):

			if not(merged_entry.has_key(altname_str)):
				merged_entry[altname_str]= []

			for name in entry[altname_str]:
				merged_entry[altname_str].append ( name )

		# merge target list
		if entry.has_key(targetlist_str):

			if not(merged_entry.has_key(targetlist_str)):
				merged_entry[targetlist_str]= copy.copy(entry[targetlist_str])
			else:
				for target in entry[targetlist_str]:
					if not(target in merged_entry[targetlist_str]):
						merged_entry[targetlist_str].append(target)

		# merge facility type
		if entry.has_key(facilitytype_str):

			if not(merged_entry.has_key(facilitytype_str)):
				merged_entry[facilitytype_str] = entry[facilitytype_str]
			elif entry[facilitytype_str] != merged_entry[facilitytype_str]:
				write(logfile,  log_text_1 + facilitytype_str + log_text_2 + entry['original_id'] + "'")

		# merge facility group
		if entry.has_key(facilitygroup_str):

			if not(merged_entry.has_key(facilitygroup_str)):
				merged_entry[facilitygroup_str] = entry[facilitygroup_str]
			elif entry[facilitygroup_str] != merged_entry[facilitygroup_str]:
				write(logfile,  log_text_1 + facilitygroup_str + log_text_2 +entry['original_id'] + "'")

		# merge location
		if entry.has_key(location_str):

			if not(merged_entry.has_key(location_str)):
				merged_entry[location_str] = {}

			if entry[location_str].has_key(continent_str):
				if not(merged_entry[location_str].has_key(continent_str)):
					merged_entry[location_str][continent_str] = entry[location_str][continent_str]
				elif entry[location_str][continent_str] != merged_entry[location_str][continent_str]:
					write(logfile,  log_text_1 + continent_str + log_text_2 +entry['original_id'] + "'")

			if entry[location_str].has_key(country_str):
				if not(merged_entry[location_str].has_key(country_str)):
					merged_entry[location_str][country_str] = entry[location_str][country_str]
				elif entry[location_str][country_str] != merged_entry[location_str][country_str]:
					write(logfile, log_text_1 + country_str + log_text_2 + entry['original_id'] + "'")

			if entry[location_str].has_key(coordinates_str):
				if not(merged_entry[location_str].has_key(coordinates_str)):
					merged_entry[location_str][coordinates_str] = entry[location_str][coordinates_str]
				else:

					if entry[location_str][coordinates_str].has_key(lat_str):
						if not(merged_entry[location_str][coordinates_str].has_key(lat_str)):
							merged_entry[location_str][coordinates_str][lat_str] = entry[location_str][coordinates_str][lat_str]
						elif merged_entry[location_str][coordinates_str][lat_str] != entry[location_str][coordinates_str][lat_str]:
							write(logfile, log_text_1 + coordinates_str + "." + lat_str + log_text_2 + entry['original_id'] + "' (" + str(merged_entry[location_str][coordinates_str][lat_str]) + " vs. " + str(entry[location_str][coordinates_str][lat_str]) + ")")

					if entry[location_str][coordinates_str].has_key(lon_str):
						if not(merged_entry[location_str][coordinates_str].has_key(lon_str)):
							merged_entry[location_str][coordinates_str][lon_str] = entry[location_str][coordinates_str][lon_str]
						elif merged_entry[location_str][coordinates_str][lon_str] != entry[location_str][coordinates_str][lon_str]:
							write(logfile, log_text_1 + coordinates_str + "." + lon_str + log_text_2 + entry['original_id'] + "' (" + str(merged_entry[location_str][coordinates_str][lon_str]) + " vs. " + str(entry[location_str][coordinates_str][lon_str]) + ")")

					if entry[location_str][coordinates_str].has_key(alt_str):
						if not(merged_entry[location_str][coordinates_str].has_key(alt_str)):
							merged_entry[location_str][coordinates_str][alt_str] = entry[location_str][coordinates_str][alt_str]
						elif merged_entry[location_str][coordinates_str][alt_str] != entry[location_str][coordinates_str][alt_str]:
							write(logfile, log_text_1 + coordinates_str + "." + alt_str + log_text_2 + entry['original_id'] + "' (" + str(merged_entry[location_str][coordinates_str][alt_str]) + " vs. " + str(entry[location_str][coordinates_str][alt_str]) + ")")

					if entry[location_str][coordinates_str].has_key(tz_str):
						if not(merged_entry[location_str][coordinates_str].has_key(tz_str)):
							merged_entry[location_str][coordinates_str][tz_str] = entry[location_str][coordinates_str][tz_str]
						elif merged_entry[location_str][coordinates_str][tz_str] != entry[location_str][coordinates_str][tz_str]:
							write(logfile, log_text_1 + coordinates_str + "." + tz_str + log_text_2 + entry['original_id'] + "' (" + str(merged_entry[location_str][coordinates_str][tz_str]) + " vs. " + str(entry[location_str][coordinates_str][tz_str]) + ")")

					if entry[location_str][coordinates_str].has_key(sin_str):
						if not(merged_entry[location_str][coordinates_str].has_key(sin_str)):
							merged_entry[location_str][coordinates_str][sin_str] = entry[location_str][coordinates_str][sin_str]
						elif merged_entry[location_str][coordinates_str][sin_str] != entry[location_str][coordinates_str][sin_str]:
							write(logfile, log_text_1 + coordinates_str + "." + sin_str + log_text_2 + entry['original_id'] + "' (" + str(merged_entry[location_str][coordinates_str][sin_str]) + " vs. " + str(entry[location_str][coordinates_str][sin_str]) + ")")

					if entry[location_str][coordinates_str].has_key(cos_str):
						if not(merged_entry[location_str][coordinates_str].has_key(cos_str)):
							merged_entry[location_str][coordinates_str][cos_str] = entry[location_str][coordinates_str][cos_str]
						elif merged_entry[location_str][coordinates_str][cos_str] != entry[location_str][coordinates_str][cos_str]:
							write(logfile, log_text_1 + coordinates_str + "." + cos_str + log_text_2 + entry['original_id'] + "' (" + str(merged_entry[location_str][coordinates_str][cos_str]) + " vs. " + str(entry[location_str][coordinates_str][cos_str]) + ")")

		# merge measurement type
		if entry.has_key(measurementtype_str):

			if not(merged_entry.has_key(measurementtype_str)):
				merged_entry[measurementtype_str] = copy.copy(entry[measurementtype_str])
			else:
				for elem in entry[measurementtype_str]:
					if not(elem in merged_entry[measurementtype_str]):
						merged_entry[measurementtype_str].append(elem)

		# merge reference urls
		if entry.has_key(referenceurl_str):

			if not(merged_entry.has_key(referenceurl_str)):
				merged_entry[referenceurl_str] = []

			for elem in entry[referenceurl_str]:
				if ( not(merged_refurls.has_key(elem['url'])) ):
					merged_entry[referenceurl_str].append(elem)
					merged_refurls[elem['url']] = True;

		# merge launch date
		if entry.has_key(launchdate_str):

			if not(merged_entry.has_key(launchdate_str)):
				merged_entry[launchdate_str] = entry[launchdate_str]
			elif entry[launchdate_str] != merged_entry[launchdate_str]:
				write(logfile, log_text_1 + launchdate_str + "' for merged object: '" + entry['original_id'] + "'" )

	return merged_entry

def merge_doubles (list):

	# indices of names and ids and locations
	name_id_index = {}
	lon_lat_index ={}

	if _checked_.has_key('merged_counter'):
		merged_counter = int(_checked_['merged_counter'])
	else:
		merged_counter = 0

	if not(fuzzy) and not(partial):	# no flags provided, simpole merge of identical names/ids
		log_file = open( logs_dir + log_file_name, 'w' )

		print ( "Calculating doubles for alternate names/ids..." )
		for rec in list:
			# index for names, ids
			if list[rec].has_key(altname_str):
				for altname in list[rec][altname_str]:

					# add id to index
					if altname.has_key(id_str):
						if name_id_index.has_key(altname[id_str]):
							if not ( rec in name_id_index[altname[id_str]] ):
								name_id_index[altname[id_str]].append(rec)
						else:
							name_id_index[altname[id_str]] = [rec]
						cur_id = altname[id_str];
					else: cur_id = '___##NOT!SET##___'

					# add name to index
					if altname.has_key(name_str) and altname[name_str] != cur_id:
						if name_id_index.has_key(altname[name_str]):
							if not ( rec in name_id_index[altname[name_str]] ):
								name_id_index[altname[name_str]].append(rec)
						else:
							name_id_index[altname[name_str]] = [rec]

		name_id_doubles = [];
		#only remember those that actually store doubles
		for entry in name_id_index:
			if len ( name_id_index[entry] ) > 1:
				name_id_doubles.append(name_id_index[entry])

		#merge doubles that share at least one element
		united_doubles_list = []
		for entry in name_id_doubles:
			found = False
			for entry2 in united_doubles_list:
				if ( merge_elements(entry, entry2) ):
					found = True
					break
			if not(found):
				united_doubles_list.append(entry)

		print ( "Merging dublicate entries for names/ids..." )

		log_file.write("\n********************** NAME AND ID MERGES **********************\n\n")

		# remove doubles in names, id
		for doubles in united_doubles_list:
			merged_name = "merged" + str(merged_counter)

			merged_entry = merge_entries(doubles, list, log_file)
			list[merged_name] = merged_entry
			merged_counter = merged_counter + 1
			remove_entries(list, doubles)
			log_file.write("\n")

		log_file.write("\n\n********************** LOCATION MERGES (precision " + str(precision) + ") **********************\n")

		print ( "Calculating doubles for locations..." )
		for rec in list:
			# index for longitude, latitude
			if list[rec].has_key('location') and list[rec]['location'].has_key('coordinates') and list[rec]['location']['coordinates'].has_key('lon') and list[rec]['location']['coordinates'].has_key('lat'):
				lon_lat_str = trunc ( list[rec]['location']['coordinates']['lon'] ) + ", " + trunc ( list[rec]['location']['coordinates']['lat'] )
				if lon_lat_index.has_key(lon_lat_str):
					lon_lat_index[lon_lat_str].append (rec)
				else:
					lon_lat_index[lon_lat_str] = [rec]

		print ( "Merging dublicate entries for locations..." )

		# remove doubles in location
		for rec in lon_lat_index:
			if len( lon_lat_index[rec] ) > 1:
				merged_entry = merge_entries(lon_lat_index[rec], list, log_file)
				list["merged_" + str(merged_counter)] = merged_entry
				merged_counter = merged_counter + 1
				remove_entries(list, lon_lat_index[rec])
				log_file.write("\n")

		log_file.close()

		print ( str(merged_counter) + " merge operations in total." )


	elif fuzzy: # -f or -fuzzy flag provided

		if not (interactive):
			hints_file = open( logs_dir + fuzzy_hints_file_name, 'w' )

		while True:

			found_fuzzy = False

			print ( 'Creating fuzzy index for names...' )
			name_index = {}
			for rec in list:
				# index for names
				if list[rec].has_key(altname_str):
					for altname in list[rec][altname_str]:
						if altname.has_key(name_str):
							name_index[altname[name_str]] = { 'auth':altname[auth_str], 'obj':rec, 'cln': re.sub ('[/\-*+#,\s\.\'\"]', '', altname[name_str]).upper() } # match will be case INsensitive!

			print ( 'Searching for fuzzy matches...\n' )
			num_hints = 0
			num_ignored = 0

			# "fuzzy" search for similar keys in id/names...
			for checked_key in name_index.keys():

				# could have been already merged before...
				if not ( list.has_key(name_index[checked_key]['obj']) ): continue

				for key in name_index.keys():
					if not ( list.has_key(name_index[checked_key]['obj']) ): break
					if not ( list.has_key(name_index[key]['obj']) ): continue

					if _checked_.has_key ( name_index[checked_key]['obj'] + name_index[key]['obj'] ):
						num_ignored = num_ignored + 1
						continue

					if (name_index[checked_key]['cln'] in name_index[key]['cln']) and ( name_index[key]['auth'] != name_index[checked_key]['auth'] and (name_index[checked_key]['obj'] != name_index[key]['obj'] ) ):

						found_fuzzy = True

						hints_str = "Possibly related objects:\n\n-> " + name_index[key]['obj'] + " - '" + key + "' (" + name_index[key]['auth'] + ")\n\n-> " + name_index[checked_key]['obj'] + " - '" + checked_key + "' (" + name_index[checked_key]['auth'] + ")\n"
						hints_str = hints_str

						if not(interactive):
							hints_file.write( hints_str.encode("utf-8", "ignore") + "\n\n" )
							num_hints = num_hints + 1
						else:
							print ( hints_str.encode(sys.stdout.encoding, errors='replace') )
							merged_entry = merge_entries([name_index[checked_key]['obj'], name_index[key]['obj']], list, None)
							print "\n"

							while True:
								cmd = raw_input ('Type "m" to merge, "i" to ignore and "s" to save merged file and resume later, or "?" followed by the object id for information ("*" can be used at the end of object name for matching): ' )

								if  ( cmd == 's' or cmd == 'm' or cmd == 'i' ): break

								if cmd == '':
									print ( "\n" + hints_str )
									continue

								if ( cmd.startswith("?") ):
									cmd_arr = cmd.split()
									if len(cmd_arr) != 2:
										print "\n'?' must be followed by at least on blank and a valid object id (as listed above), e.g. '? xyz:abc'!\n"
									else:
										found = False
										for obj_id in list:
											o_name = str(cmd_arr[1])
											if ( ( o_name.endswith("*") and str(obj_id).startswith(o_name[:-1]) ) or ( o_name.endswith("*") and str(obj_id) == cmd_arr[1][:-1] ) or ( str(obj_id) == o_name ) ):
												print ("\n** Object: " + obj_id + " **")
												print json.dumps( list[obj_id], indent=2 )
												print ("\n")
												found = True
												break

										if not(found):
											print "\nAn object with id '" + cmd_arr[1] + "' does not exist!\n"

							if cmd == 'm': # merge
								if merged_entry != None:
									list["merged_fuzzy_" + str(merged_counter)] = merged_entry
									merged_counter = merged_counter + 1
									remove_entries(list, [name_index[checked_key]['obj'], name_index[key]['obj']])
									print ( "Objects merged." )

							if cmd == 'i': # ignore
								_checked_[ name_index[checked_key]['obj'] + name_index[key]['obj'] ] = True
								print ( "Ignored." )
								print ( "\n----------------------------------------------------------------------------\n" )
								continue

							if cmd == 's': # save
								print ( "Data and list of ignored objects saved." )
								save_checked_file(merged_counter)
								return list

							print ( "\n----------------------------------------------------------------------------\n" )

			if not(found_fuzzy) or not(interactive):
				break

		if not(interactive):
			print ( str(num_hints) + " hints created regarding objects that could be related in file '" + fuzzy_hints_file_name + "'" )
			if (num_ignored): print ( str(num_ignored) + " entries skipped since they were previously ignored in interactive mode." )
		else:
			save_checked_file(merged_counter)
			print ( "No (further) candidate objects found, " + str(num_ignored) + " ignored." )

	# "partial" search for similar keys in id/names...
	elif partial: # -p or -partial flag provided

		num_hints = 0

		if not (interactive):
			hints_file = open( logs_dir + partial_hints_file_name, 'w' )

		print ( 'Reading stopword file...')
		stopwords = {}
		stopwords_arr = open( conf_dir + "stopwords.txt" ).readlines()
		for stopword in stopwords_arr:
			stopwords[stopword.strip().upper()] = True # used rstrip ("\n\r ") before, but strip gets rid of everything (caused problem on e.g. max osx)...

		while True:

			print ( 'Creating partial search index for names and searching for matches in names...\n\n' )
			partial_index = {}
			found_matches = False

			for rec in list:
				# partial index for names
				if list[rec].has_key(altname_str):
					for altname in list[rec][altname_str]:
						if altname.has_key(name_str):
							words = re.split( '[\s#-:;\\/\(\)\[\]\?\*,\._]', altname[name_str] )
							for word in words:
								upper_word = word.upper()
								if len(upper_word) > 2 and not( stopwords.has_key(upper_word) ) and not( _checked_.has_key("partial_check::" + upper_word ) ):
									if partial_index.has_key(upper_word):

										if partial_index[upper_word].has_key(rec):
											partial_index[upper_word][rec].append( [altname[name_str], altname[auth_str]] )
										else:
											partial_index[upper_word][rec] = [ [altname[name_str], altname[auth_str]] ]
											partial_index[upper_word]["mult"] = True
											found_matches = True

									else:
										partial_index[upper_word] = {}
										partial_index[upper_word][rec] = [ [altname[name_str], altname[auth_str]] ]
										partial_index[upper_word]["mult"] = False

			if not(found_matches): break # no more double matches in partial search for names were found, exit loop

			for word in partial_index:
				merge_candidates = []

				if not(partial_index[word]["mult"]):
					continue # only one object contains the word

				hints_str = "The term '" + word + "' appears in at least one of the alternate names of the following objects:\n"

				obj_num = 0
				for obj in partial_index[word]:
					if list.has_key(obj):
						obj_num = obj_num + 1
						merge_candidates.append(obj)
						hints_str = hints_str + "\n" + str(obj_num) + ": " + obj + " - "
						for name_auth in partial_index[word][obj]:
							hints_str = hints_str + "'" + name_auth[0] + "' (" + name_auth[1] + "), "
						hints_str = hints_str.rstrip(", ") + "\n"

				hints_str = hints_str

				if obj_num < 2:
					continue # all other objects of index entry were already merged previously

				if not(interactive):
					hints_file.write( hints_str.encode("utf-8", "ignore") + "\n\n" )
					num_hints = num_hints + 1
				else:
					print ( hints_str.encode(sys.stdout.encoding, errors='replace') )

					merged_entry = merge_entries(merge_candidates, list, None)
					print "\n"

					while True:
						try:
							cmd = raw_input ("Please provide the ids of objects to be merged separated by blanks (e.g. '1 2'), 'a' to merge all objects, 'i' to ignore matches for term '" + word + "' or 's' to save and continue later. Use ? and object id for information: " )

							if cmd == '':
								print ( "\n" + hints_str )
								continue

							if  ( cmd == "s" or cmd == "i" or cmd == "a"): break

							if ( cmd.startswith("?") ):
								cmd_arr = cmd.split()
								if len(cmd_arr) != 2 or int(cmd_arr[1]) < 1 or int(cmd_arr[1]) > len(merge_candidates):
									print "\n'?' must be followed by at least on blank and a valid object id (as listed above), e.g. '? 2'!"
								else:
									obj_id = merge_candidates[int(cmd_arr[1])-1]
									print ("\n** Object: " + obj_id + " **")
									print json.dumps( list[obj_id], indent=2 )
									print ("\n")
							else:
								input_ids = cmd.split()
								ids_valid = True
								if len(input_ids) < 2:
									print "\nERROR: Please provide at least two ids to be merged!"
									ids_valid = False

								for id in input_ids:
									if int(id) > obj_num:
										print "\nERROR: Please only provide numbers between 1 and " + str(obj_num) + "!"
										ids_valid = False
								if ids_valid:
									break

						except ValueError:
							print ( "\nERROR: Some of the ids provided were not numbers!\n" )


					if cmd == "i": # ignore
						_checked_[ "partial_check::" + word.upper() ] = True
						print ( "Matches for term '" + word + "' ignored." )
						print ( "\n----------------------------------------------------------------------------\n" )
						continue

					if cmd == "s": # save
						print ( "Data and list of ignored terms saved." )
						# save "_checked_" file
						save_checked_file(merged_counter)
						return list

					all_merged = False
					if cmd == "a": # merge all objects
						all_merged = True
						to_be_merged = merge_candidates
					else:
						input_ids = cmd.split() # merge provided objects
						to_be_merged = []
						for id in input_ids:
							index = int(id)-1
							to_be_merged.append(merge_candidates[index])

						if ( len(merge_candidates) - len(to_be_merged) ) < 2:
							all_merged = True

					merged_entry = merge_entries(to_be_merged, list, False)

					if merged_entry != None:
						list["merged_partial_" + str(merged_counter)] = merged_entry
						merged_counter = merged_counter + 1
						remove_entries(list, to_be_merged)
						print ( str(len(to_be_merged)) + " objects merged." )

					if not(all_merged):
						if raw_input ("Ignore remaining matches on term '" + word + "'? (y/n): ") == "y" or "Y":
							_checked_[ "partial_check::" + word.upper() ] = True
							print ( "All remaining matches for term '" + word + "' ignored." )


					print ( "\n----------------------------------------------------------------------------\n" )

			if not(interactive): break

		if not(interactive):
			print ( str(num_hints) + " hints created regarding objects that could be related in file '" + partial_hints_file_name + "'" )
		else:
			save_checked_file(merged_counter)
			print ( "No (further) candidate objects found." )

	return list

if fuzzy or partial:
	print ( "Using merged list in file '" + result_file_name + "'." )
	if os.path.isfile(output_dir + result_file_name):
		with open(output_dir + result_file_name) as results_file:
			lists = json.load(results_file)
		if os.path.isfile(output_dir + tmp_checked_file_name):
			_checked_ = json.load(open(output_dir + tmp_checked_file_name))
			print ( "Resuming from previous run from: " + str(_checked_['_timestamp_']) )
	else:
		print ( "\nERROR: File '" + result_file_name + "' could not be found! Please run 'merge_facilitylists.py' without parameters first to perform exact matches." )
		sys.exit()

else:
	if os.path.isfile(output_dir + tmp_checked_file_name) and raw_input ("Delete temporary file '" + tmp_checked_file_name + "'? (Y/N): ") == "Y":
		os.remove (output_dir + tmp_checked_file_name)
		print ("Temporary file '" + tmp_checked_file_name + " deleted!")
	print "Parsing lists..."
	lists = load_all_lists(data_dir)

merged_data = merge_doubles ( lists )
num_objects = str ( len ( merged_data.keys() ) )

result_file = open( output_dir + result_file_name, 'w')
result_file.write( json.dumps ( merged_data ) )
result_file.close()

if not(fuzzy) and not(partial) or ( ( fuzzy or partial) and interactive ):
	print ( "\nSee files '" + result_file_name + "' for result list (" + num_objects + " objects)." )

if not(fuzzy) and not(partial):
	print ( "See " + log_file_name + "' for information and warnings on merged objects." )
