# Metadata Mapping Between Data Sources

## Local metadata

| Name | Property | Definition | 
| --- | --- | --- |
| `identifier` | `dc:identifer` | The term to be used in the final vocabulary. |
| `label` | `rdfs:label` | The name of the observation facility. | 
| `description` | `rdfs:comment` | A decription of the observation facility. |
| `aliases` | `skos:altLabel` | Alternate names and known aliases (should include language tag). | 
| `is_part_of` | `dct:isPartOf` | A related resource in which the described resource is physically or logically included. | 
| `has_part` | `dct:hasPart` | A related resource that is included either physically or logically in the described resource. | 
| `naif_id` | | The NAIF ID of the observation facility | 
| `cospar_id` | | The COSPAR/NSSDC ID of the observation facility | 
| `mpc_obscode` | | The MPC IAU ObsCode of the observation facility | 
| `nasa_pds`| | The NASA/PDS context product logical identifier of the observation facility |
| `spase_id`| | The SPASE Resource ID of the observation facility | 
| `uat_id`| | The UAT (Unified Astronomy Thesaurus) URI of the observation facility |

Other identifiers to be considered: VIAF_ID, ISNI, OpenAlex ID, ROR. 

## Source: AAS

```
{
  "identifier": "keyword",
  "label": "full facility name"
}
```

## Source: IRAF

```
{
  "identifier: "observatory",
  "label: "name"
}
```

## Source: NASA/PDS

```
{
  "identifier": "Identification_Area.logical_identifier",
  "label": "Identification_Area.title",
  "description": null,
  "aliases": [
    "Identification_Area.*.alternate_id",
    "Identification_Area.*.alternate_title",
  ],
  "has_part": [
    "Reference_List.Internal_Reference.reference_type.investigation_to_instrument_host",
    "Reference_List.Internal_Reference.reference_type.facility_to_telescope"
  ]
  "is_part_of": [
    "Reference_List.Internal_Reference.reference_type.instrument_host_to_investigation",
    "Reference_List.Internal_Reference.reference_type.telescope_to_facility"
  ],
  "naif_id" : "context_type.naif_host_id"
}
```

## Source: Wikidata

```
{
  "is_part_of": "wdt:P361",
  "has_part": ""
  "VIAF_ID": "wdt:P214",
  "ISNI": "wdt:P361",
  "COSPAR ID: ["wdt:P247", wdt:P8913"],
  "MPC ObsCode": "wdt:P717",
  "NAIF ID": "wdt:P2956",
  "OpenAlex ID": "wdt:P10283",
  "UAT ID": "wdt:P4466":
  "aliases: "skos:altLabel"
} 
```
