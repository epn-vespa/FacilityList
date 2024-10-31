# FacilityList data

Put all your input files here. 

- AAS: [votable](AAS.xml), extracted from [[AAS web portal](http://journals.aas.org/authors/aastex/facility.html)] on May 27th 2015.
- ADS: [text](ADS/ADS_facilities.txt), also available as [votable](ADS/harvard.xml)
- AstroWeb: [html](Astroweb.html)
- DSN: Deep Space Network - [text](DSN.txt)
- IAU-MPC: Minor Planet Center from IAU -  [txt](IAU-MPC.txt) list
- IRAF: [txt](IRAF.txt) or [votable](IRAF.xml)
- NAIF: NASA Navigation and Ancillary Information Facility - [votable](NAIF/NAIF.xml)
- NSSDC: National Space Science Data Centre - [votable](NSSDC.xml)
- SANA: CCSDS/SANA Spacecraft List - [xml](SANA-orig.xml) or [votable](SANA.xml)

In order to be processed by the merging scripts, every input file needs a respective parser function 
in [parser.py](../parsers.py) module.

