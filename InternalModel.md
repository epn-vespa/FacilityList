# Internal Model for FacilityList

The list-parsing functions are defined in [conf/parsers.py](conf/parsers.py). Each function should return a dictionary following the specification described below.

The dictionary is composed of pairs of (key, value) defined as follows:
* _key_ is composed of a short _authority_ name acting as a name space (e.g., `aas` for the AAS list), followed by an ID derived from the current list, joined with a `:`, for instance: `aas:SORCE`.
* _value_ is a dictionnary object comforing with the [models/facility.json](models/facility.json) JSON schema. 

## Individual Facility elements

The Observatory descriptor follows the model described in the [models/facility.json](models/facility.json) JSON schema. 

The expected properties are:
* `alternateName`: (list) A list of all alternate names dictionaries (see below)
* `facilityType`: (str) A string containing either `spacecraft`or `observatory`
* `location`: (dict) A dictionary containing `continent`, `country`, `place` and/or `coordinates`. The `coordinates` property is a dictionary with `lat` (latitude), `lon` (longitude), `alt` (altitude), `sin` (rho sin(phi) parallax coordinate), `cos` (rho cos(phi) parallax coordinate) and/or `tz` (time zone) properties.
* `measurementType`: (list) A list of string items (following IVOA UCD specification).
* `targetList`: (list) A list of string items with target names.
* `facilityGroup`: (str) A named group to which the Facility belongs to.
* `instrumentList`: (list) A list of string items with instrument names.
* `referenceURL`: (str) A string containing URL to a reference website of documentation.
* `launchDate`: (str) A string containing the launch date for a spacecraft.

Each `alternateName` object contins the following attributes:
* `authorityName`: (str) a naming authority
* `name`: (str) a name for the Facility
* `id`: (str) an ID for the Facility

Note the `name` and `id` are considered as alternate names of the same Facility. 
