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

# ***************** input parsers and files - add the name of the respective function (no brakets), the funtion must exist in parsers.py!
# ***************** In case of json files or URLs to web services returning JSON in the required format just add the name of the json file 
# ***************** or the URL of respective web service.
# example: 'load_xephem_list', 'load_naif_list', 'matrix_json_export_20170731.json', 'load_ads_list'
configured_inputs = [\
'load_iraf_list' \
]
