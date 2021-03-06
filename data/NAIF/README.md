# NAIF (Navigation and Ancillary Information System)

The NASA/NAIF team is maintaining orbital and pointing information for
space missions, using the NASA/SPICE library.

More info:
[https://naif.jpl.nasa.gov/naif/](https://naif.jpl.nasa.gov/naif/)

## Identifiers
The NAIF/SPICE identifiers are avaialble from [NASA/NAIF IDs page, Spacecraft section](https://naif.jpl.nasa.gov/pub/naif/toolkit_docs/FORTRAN/req/naif_ids.html#Spacecraft)

The data is available in the [naif.txt](naif.txt) file, retrieved on 2018-11-29.

## Parser
The parser script is available in [conf/parsers.py](../../conf/parsers.py), using the `load_naif_list()` function.


## Known Issues

### Space mission with same ID
The NAIF has been reusing ID for missions that don't overlap in time.
Here is the list of those ID/Name ambiguities that must be fixed after
an automated merge pass:

* `-12` used for `Pioneer-12`/`Pioneer Venus Orbiter` (1978 to 1982) and `LADEE` (2013 to 2014)
* `-30` used for `Deep Space 1` (1998 to 2001) and `Viking-2` (1975 to 1980)
* `-53` used for `Mars Odyssey` (2001 to 2018) and `Mars Path Finder` (1997)
* `-47` used for `Suisei`/`Planet-A` (1985 to 1989) and `Genesis` (2001)
* `-66` used for `VEGA 1` (1984 to 1986) and `MARCO-B` (2018)
* `-198` used for `INTEGRAL` (2002 to 2020) and `NISAR`/`NASA-ISRO SAR MISSION` (2020 or later)