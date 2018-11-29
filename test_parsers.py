import json
import jsonschema
import unittest
import conf.parsers

parsers_list_functions = [
    conf.parsers.load_aas_list,
    conf.parsers.load_ads_list,
    conf.parsers.load_dsn_list,
    conf.parsers.load_iraf_list,
    conf.parsers.load_mpc_gavo_list,
    conf.parsers.load_mpc_list,
    conf.parsers.load_naif_list,
    conf.parsers.load_nssdc_list,
    conf.parsers.load_ppi_list,
    conf.parsers.load_telwiserep_list,
    conf.parsers.load_xephem_list,
]


with open('models/facility.json', 'r') as schema_file:
    schema_data = json.load(schema_file)


def facility_schema_validate(record):
    jsonschema.validate(record, schema_data)


class test_parsers_output(unittest.TestCase):
    """
    Test case for parsers output()
    """

    def test_against_json_schema(self):
        for cur_function in parsers_list_functions:
            data = cur_function()
            for key, value in data.iteritems():
                self.assertRaises(jsonschema.ValidationError, facility_schema_validate(value))
