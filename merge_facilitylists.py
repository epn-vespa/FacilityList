from datetime import datetime
from conf.parsers import *
from conf.config import *
from conf.consts import *
from pprint import pprint

import json, pickle, copy, re, sys, os.path

# ********************************** See conf/config.py for configurations of constants, parsers and inputs! **********************************

tmp_checked_file_name = '_checked_.json'
_checked_ = {}

# command line inputs
fuzzy = False
interactive = False
partial = False
update = False
for arg in sys.argv:
	if ( arg == "-f" or arg == "-fuzzy" ): fuzzy = True
	if ( arg == "-i" or arg == "-interactive" ): interactive = True
	if ( arg == "-p" or arg == "-partial" ): partial = True
	if ( arg == "-u" or arg == "-update" ): update = True

def load_all_lists(dir): # see parsers defined in file parsers.py
	setDataDir(dir)
	if (update):
		print ( "Using previously merged list in file '" + result_file_name + "'. This list will be udpated with supplied data..." )
		if os.path.isfile(output_dir + result_file_name):
			with open(output_dir + result_file_name) as results_file:    
				data = json.load(results_file)
		else:
			print ( "\nERROR: File '" + result_file_name + "' could not be found! Thus no list to update..." )
			sys.exit()
		print ("Removing previously merged entries in input file(s)...")
		
	else:
		data = {}
	
	# calling all configured parsers, reading all configured lists from files and/or web services
	for input in configured_inputs:
		if (input.startswith('http:') or input.startswith('https:') or input.endswith('.json') ):
			update_list( data, load_existing_json(input) )
		else:
			try:
				update_list( data, globals()[input]() )
			except:
				print "WARNING: Could not call parser function '" + input + "()': " + str(sys.exc_info()[0]) 
				
	return data


if (fuzzy and partial):
	print "Options 'f' or 'fuzzy' and 'p' or 'partial' can not be used at the same time!"
	sys.exit()

if (interactive and not (fuzzy or partial)):
	print "Option 'i' or 'interactive' can only be used in conjunction with 'f' ('fuzzy') or 'p' ('partial') !"
	sys.exit()
	
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

def update_list(existing_list, new_list):
	# This functions removes all entries in configured input lists that already exist in merged_list.json when parameter '-update' is given.
	# Without parameter "-update" new_list is simply added to the existing list for further merging. 
	# When two entries are equal wrt altname(s) but NOT equal wrt other included data (location,...), the updated information is ignored!
	# Since all previously added enteries from a certain list are "overwritten" (i.e. existing entries are kept!), the entries of the new list only have to be compared with MERGED entries.
	if (len(new_list) == 0):
		print "WARNING: list with zero length included, will be ignored."
		return
		
	if (update):
		# prepare list wih all entries of original list that contain at least one altname derrived of the same athority as new_list for comparing
		authority = new_list.itervalues().next()[KEY_STR_ALTERNATE_NAME][0][KEY_STR_NAMING_AUTHORITY]
		compare_list = []
		cleaned_new_list = {}
		for entry in existing_list:
			if entry in new_list:
				if existing_list[entry] == new_list[entry]:
					del new_list[entry]
				else:
					while True:
						print "\nWARNING: object in updated list from authority '" + authority + "' shares same key (" + entry + ") as in exsiting list but differs regading content: \n"
						print "<<<<<<< Existing Object:"
						print json.dumps( existing_list[entry], indent=2 )
						print ">>>>>>> New Object:"
						print json.dumps( new_list[entry], indent=2 )
						cmd = raw_input ("Type 'o' to keep the OLD object in existing list or 'n' to overwrite with NEW object in updated list:")
						
						if (cmd.upper() == "O"):
							del new_list[entry]
							print "Old entry in existing list is kept."
							break
						elif (cmd.upper() == "N"):
							existing_list[entry] = new_list[entry]
							del new_list[entry]
							print "Entry in existing list is overwritten with entry in new list."
							break
						else:
							print "Input not valid! Type 'o' to keep the OLD object in existing list or 'n' to overwrite with NEW object in updated list:"
					
			for alt_name in existing_list[entry][KEY_STR_ALTERNATE_NAME]:
				if (alt_name[KEY_STR_NAMING_AUTHORITY] == authority):
					compare_list.append(existing_list[entry])
					break
		
		for new_entry in new_list:
			add_entry = True
			for compare_entry in compare_list:
				all_altnames_included = True
				for new_alt_name in new_list[new_entry][KEY_STR_ALTERNATE_NAME]:
					if ( not(altname_in_entry(new_alt_name, compare_entry)) ):
						all_altnames_included = False
						break
				if (all_altnames_included):
					add_entry = False
					
			if (add_entry):
				cleaned_new_list[new_entry] = new_list[new_entry]
		
		existing_list.update(cleaned_new_list)
	else:
		existing_list.update(new_list)
	
def altname_in_entry(altname, entry):
	# checks if altname is equal to at least one of the altnames of entry
	for existing_altname in entry[KEY_STR_ALTERNATE_NAME]:
		if ( existing_altname == altname ):
			return True
			
	return False
		
def merge_entries(doubles_array, list, logfile):
	log_text_1 = "* WARNING: differing values for property '"
	log_text_2 = "' for object: '"
	
	merged_entry = {}
	entry_array = []
	
	double_strings = ""
	for double in doubles_array:
		if not(double in list): continue # FixMe: This should NOT happen, except for the pathological case where two identical lists (expect for authority) are merged
		elem_copy = copy.copy(list[double])
		elem_copy['original_id'] = double
		entry_array.append(elem_copy)
		double_strings = double_strings + double + ", "
			
	if logfile != None: write(logfile, "-> Merged entries: " + double_strings[:-2])
	
	merged_refurls = {}
	
	for entry in entry_array:
		
		# merge alternate names
		if entry.has_key(KEY_STR_ALTERNATE_NAME):
			
			if not(merged_entry.has_key(KEY_STR_ALTERNATE_NAME)):
				merged_entry[KEY_STR_ALTERNATE_NAME]= []

			for name in entry[KEY_STR_ALTERNATE_NAME]:
				if ( not(altname_in_entry(name, merged_entry) ) ): 
					merged_entry[KEY_STR_ALTERNATE_NAME].append (name)

		# merge target list
		if entry.has_key(KEY_STR_TARGET_LIST):

			if not(merged_entry.has_key(KEY_STR_TARGET_LIST)):
				merged_entry[KEY_STR_TARGET_LIST]= copy.copy(entry[KEY_STR_TARGET_LIST])
			else:
				for target in entry[KEY_STR_TARGET_LIST]:
					if not(target in merged_entry[KEY_STR_TARGET_LIST]):
						merged_entry[KEY_STR_TARGET_LIST].append(target)
	
		# merge facility type
		if entry.has_key(KEY_STR_FACILITY_TYPE):
		
			if not(merged_entry.has_key(KEY_STR_FACILITY_TYPE)):
				merged_entry[KEY_STR_FACILITY_TYPE] = entry[KEY_STR_FACILITY_TYPE]
			elif entry[KEY_STR_FACILITY_TYPE] != merged_entry[KEY_STR_FACILITY_TYPE]:
				write(logfile, log_text_1 + KEY_STR_FACILITY_TYPE + log_text_2 + entry['original_id'] + "'")
		
		# merge facility group
		if entry.has_key(KEY_STR_FACILITY_GROUP):
		
			if not(merged_entry.has_key(KEY_STR_FACILITY_GROUP)):
				merged_entry[KEY_STR_FACILITY_GROUP] = entry[KEY_STR_FACILITY_GROUP]
			elif entry[KEY_STR_FACILITY_GROUP] != merged_entry[KEY_STR_FACILITY_GROUP]:
				write(logfile, log_text_1 + KEY_STR_FACILITY_GROUP + log_text_2 + entry['original_id'] + "'")
				
		# merge location
		if entry.has_key(KEY_STR_LOCATION):

			if not(merged_entry.has_key(KEY_STR_LOCATION)):
				merged_entry[KEY_STR_LOCATION] = {}

			if entry[KEY_STR_LOCATION].has_key(KEY_STR_CONTINENT):
				if not(merged_entry[KEY_STR_LOCATION].has_key(KEY_STR_CONTINENT)):
					merged_entry[KEY_STR_LOCATION][KEY_STR_CONTINENT] = entry[KEY_STR_LOCATION][KEY_STR_CONTINENT]
				elif entry[KEY_STR_LOCATION][KEY_STR_CONTINENT] != merged_entry[KEY_STR_LOCATION][KEY_STR_CONTINENT]:
					write(logfile, log_text_1 + KEY_STR_CONTINENT + log_text_2 + entry['original_id'] + "'")
			
			if entry[KEY_STR_LOCATION].has_key(KEY_STR_COUNTRY):
				if not(merged_entry[KEY_STR_LOCATION].has_key(KEY_STR_COUNTRY)):
					merged_entry[KEY_STR_LOCATION][KEY_STR_COUNTRY] = entry[KEY_STR_LOCATION][KEY_STR_COUNTRY]
				elif entry[KEY_STR_LOCATION][KEY_STR_COUNTRY] != merged_entry[KEY_STR_LOCATION][KEY_STR_COUNTRY]:
					write(logfile, log_text_1 + KEY_STR_COUNTRY + log_text_2 + entry['original_id'] + "'")
	
			if entry[KEY_STR_LOCATION].has_key(KEY_STR_COORDINATES):
				if not(merged_entry[KEY_STR_LOCATION].has_key(KEY_STR_COORDINATES)):
					merged_entry[KEY_STR_LOCATION][KEY_STR_COORDINATES] = entry[KEY_STR_LOCATION][KEY_STR_COORDINATES]
				else:

					if entry[KEY_STR_LOCATION][KEY_STR_COORDINATES].has_key(KEY_STR_LAT):
						if not(merged_entry[KEY_STR_LOCATION][KEY_STR_COORDINATES].has_key(KEY_STR_LAT)):
							merged_entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_LAT] = entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_LAT]
						elif merged_entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_LAT] != entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_LAT]:
							write(logfile, log_text_1 + KEY_STR_COORDINATES + "." + KEY_STR_LAT + log_text_2 + entry['original_id'] + "' (" + str(merged_entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_LAT]) + " vs. " + str(entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_LAT]) + ")")

					if entry[KEY_STR_LOCATION][KEY_STR_COORDINATES].has_key(KEY_STR_LON):
						if not(merged_entry[KEY_STR_LOCATION][KEY_STR_COORDINATES].has_key(KEY_STR_LON)):
							merged_entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_LON] = entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_LON]
						elif merged_entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_LON] != entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_LON]:
							write(logfile, log_text_1 + KEY_STR_COORDINATES + "." + KEY_STR_LON + log_text_2 + entry['original_id'] + "' (" + str(merged_entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_LON]) + " vs. " + str(entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_LON]) + ")")
					
					if entry[KEY_STR_LOCATION][KEY_STR_COORDINATES].has_key(KEY_STR_ALT):
						if not(merged_entry[KEY_STR_LOCATION][KEY_STR_COORDINATES].has_key(KEY_STR_ALT)):
							merged_entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_ALT] = entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_ALT]
						elif merged_entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_ALT] != entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_ALT]:
							write(logfile, log_text_1 + KEY_STR_COORDINATES + "." + KEY_STR_ALT + log_text_2 + entry['original_id'] + "' (" + str(merged_entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_ALT]) + " vs. " + str(entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_ALT]) + ")")

					if entry[KEY_STR_LOCATION][KEY_STR_COORDINATES].has_key(KEY_STR_TZ):
						if not(merged_entry[KEY_STR_LOCATION][KEY_STR_COORDINATES].has_key(KEY_STR_TZ)):
							merged_entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_TZ] = entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_TZ]
						elif merged_entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_TZ] != entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_TZ]:
							write(logfile, log_text_1 + KEY_STR_COORDINATES + "." + KEY_STR_TZ + log_text_2 + entry['original_id'] + "' (" + str(merged_entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_TZ]) + " vs. " + str(entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_TZ]) + ")")

					if entry[KEY_STR_LOCATION][KEY_STR_COORDINATES].has_key(KEY_STR_SIN):
						if not(merged_entry[KEY_STR_LOCATION][KEY_STR_COORDINATES].has_key(KEY_STR_SIN)):
							merged_entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_SIN] = entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_SIN]
						elif merged_entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_SIN] != entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_SIN]:
							write(logfile, log_text_1 + KEY_STR_COORDINATES + "." + KEY_STR_SIN + log_text_2 + entry['original_id'] + "' (" + str(merged_entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_SIN]) + " vs. " + str(entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_SIN]) + ")")

					if entry[KEY_STR_LOCATION][KEY_STR_COORDINATES].has_key(KEY_STR_COS):
						if not(merged_entry[KEY_STR_LOCATION][KEY_STR_COORDINATES].has_key(KEY_STR_COS)):
							merged_entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_COS] = entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_COS]
						elif merged_entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_COS] != entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_COS]:
							write(logfile, log_text_1 + KEY_STR_COORDINATES + "." + KEY_STR_COS + log_text_2 + entry['original_id'] + "' (" + str(merged_entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_COS]) + " vs. " + str(entry[KEY_STR_LOCATION][KEY_STR_COORDINATES][KEY_STR_COS]) + ")")
			
		# merge measurement type
		if entry.has_key(KEY_STR_MEASUREMENT_TYPE):

			if not(merged_entry.has_key(KEY_STR_MEASUREMENT_TYPE)):
				merged_entry[KEY_STR_MEASUREMENT_TYPE] = copy.copy(entry[KEY_STR_MEASUREMENT_TYPE])
			else:
				for elem in entry[KEY_STR_MEASUREMENT_TYPE]:
					if not(elem in merged_entry[KEY_STR_MEASUREMENT_TYPE]):
						merged_entry[KEY_STR_MEASUREMENT_TYPE].append(elem)

		# merge reference urls
		if entry.has_key(KEY_STR_REFERENCE_URL):
			
			if not(merged_entry.has_key(KEY_STR_REFERENCE_URL)):
				merged_entry[KEY_STR_REFERENCE_URL] = []

			for elem in entry[KEY_STR_REFERENCE_URL]:
				if ( not(merged_refurls.has_key(elem['url'])) ):
					merged_entry[KEY_STR_REFERENCE_URL].append(elem)
					merged_refurls[elem['url']] = True;
						
		# merge launch date
		if entry.has_key(KEY_STR_LAUNCH_DATE):
		
			if not(merged_entry.has_key(KEY_STR_LAUNCH_DATE)):
				merged_entry[KEY_STR_LAUNCH_DATE] = entry[KEY_STR_LAUNCH_DATE]
			elif entry[KEY_STR_LAUNCH_DATE] != merged_entry[KEY_STR_LAUNCH_DATE]:
				write(logfile, log_text_1 + KEY_STR_LAUNCH_DATE + "' for merged object: '" + entry['original_id'] + "'")
		
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
			if list[rec].has_key(KEY_STR_ALTERNATE_NAME):
				for altname in list[rec][KEY_STR_ALTERNATE_NAME]:
					
					# add id to index
					if altname.has_key(KEY_STR_ID):
						if name_id_index.has_key(altname[KEY_STR_ID]):
							if not (rec in name_id_index[altname[KEY_STR_ID]]):
								name_id_index[altname[KEY_STR_ID]].append(rec)
						else:
							name_id_index[altname[KEY_STR_ID]] = [rec]
						cur_id = altname[KEY_STR_ID];
					else: cur_id = '___##NOT!SET##___'
					
					# add name to index
					if altname.has_key(KEY_STR_NAME) and altname[KEY_STR_NAME] != cur_id:
						if name_id_index.has_key(altname[KEY_STR_NAME]):
							if not (rec in name_id_index[altname[KEY_STR_NAME]]):
								name_id_index[altname[KEY_STR_NAME]].append(rec)
						else:
							name_id_index[altname[KEY_STR_NAME]] = [rec]
		
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
				if list[rec].has_key(KEY_STR_ALTERNATE_NAME):
					for altname in list[rec][KEY_STR_ALTERNATE_NAME]:
						if altname.has_key(KEY_STR_NAME):
							name_index[altname[KEY_STR_NAME]] = {'auth':altname[KEY_STR_NAMING_AUTHORITY], 'obj':rec, 'cln': re.sub ('[/\-*+#,\s\.\'\"]', '', altname[KEY_STR_NAME]).upper()} # match will be case INsensitive!
			
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
								cmd = raw_input ("Type 'm' to merge, 'i' to ignore, 'd' to defer decision, 's' to save merged file and resume later or '?' followed by the object id for information ('*' can be used at the end of object name for matching): " )

								if  ( cmd in ["s", "i", "m", "d" ] ): break

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

							if cmd == "d": # defer
								print ( "\nDecision is deferred - possibly related objects will be presented again in the next iteration." )
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
				if list[rec].has_key(KEY_STR_ALTERNATE_NAME):
					for altname in list[rec][KEY_STR_ALTERNATE_NAME]:
						if altname.has_key(KEY_STR_NAME):
							words = re.split( '[\s#-:;\\/\(\)\[\]\?\*,\._]', altname[KEY_STR_NAME])
							words = set(words) # remove dubilcates of words, so every word is unique and only mentioned once
							for word in words:
								upper_word = word.upper()
								if len(upper_word) > 2 and not( stopwords.has_key(upper_word) ) and not( _checked_.has_key("partial_check::" + upper_word ) ):
									if partial_index.has_key(upper_word):
										
										if partial_index[upper_word].has_key(rec):
											partial_index[upper_word][rec].append([altname[KEY_STR_NAME], altname[KEY_STR_NAMING_AUTHORITY]])
										else:	
											partial_index[upper_word][rec] = [[altname[KEY_STR_NAME], altname[KEY_STR_NAMING_AUTHORITY]]]
											partial_index[upper_word]["mult"] = True
											found_matches = True
										
									else:
										partial_index[upper_word] = {}
										partial_index[upper_word][rec] = [[altname[KEY_STR_NAME], altname[KEY_STR_NAMING_AUTHORITY]]]
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
							cmd = raw_input ( "Please provide the ids of objects to be merged separated by blanks (e.g. '1 2'), 'a' to merge all objects, 'i' to ignore matches for term '" + word + "', 'd' to defer decision or 's' to save and continue later. Use '?' and object number for detailed information: " )

							if cmd == '': 
								print ( "\n" + hints_str )
								continue

							if  ( cmd in ["s", "i", "a", "d" ] ): break
							
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

					if cmd == "d": # defer
						print ( "Matches for term '" + word + "' are deferred and will be presented again in the next iteration." )
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
						cmd = raw_input ("Ignore remaining matches on term '" + word + "'? (y/n): ")
						if cmd == "y" or cmd == "Y":
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
	print ( "Using previously merged list in file '" + result_file_name + "'." )
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