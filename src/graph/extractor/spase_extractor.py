"""
SpaseExtractor scraps the SPASE github and stores data into a dictionary.
The output dictionary is compatible with the ontology mapping (see graph.py).

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from pathlib import Path
from typing import Set
from collections import defaultdict
from graph import entity_types
from graph.extractor.cache import CacheManager
from graph.extractor.extractor import Extractor
from graph.extractor import data_fixer
from utils.string_utilities import clean_string, has_cospar_nssdc_id, standardize_uri, cut_acronyms, get_suffix_number
from utils.dict_utilities import merge_into, UnionFind, majority_voting_merge
from utils.location_utilities import distance
import json
import re
import os

from config import CACHE_DIR # type: ignore


class SpaseExtractor(Extractor):

    # Other URLs
    # URL = "https://heliophysicsdata.gsfc.nasa.gov/websearch/dispatcher?action=CDAW_ELEMENT_LIST_PANE_ACTION&element="
    # URL = "https://hpde.io/SMWG/Observatory/index.html" # first version of the code (2025.03.14)
    # URL = "https://github.com/spase-group/spase-info/tree/master/SMWG/Observatory/"
    # URL = "https://github.com/spase-group/spase-info/tree/master/" # 2nd version of the code: without git pull (2025.03.14)
    # URL = "https://github.com/spase-group/spase-info"
    # URL = "https://github.com/hpde/hpde.io/tree/master/"
    URL = "https://github.com/hpde/hpde.io" # 3rd version of the code: with git pull (2025.03.18)

    # Name of the folder after git clone
    GIT_REPO = "hpde.io"

    # URI to save this source as an entity
    URI = "spase_list"

    # Folder name to save cache/ and data/
    CACHE = "SPASE/"

    # URI to save entities from this source
    NAMESPACE = "spase"

    # The Deprecated attribute will not be added
    # to SPASE when merging old and new versions of entities
    # together.
    MULTI_VERSIONING = True

    # Default type used for all unknown types in this resource
    DEFAULT_TYPE = entity_types.PLATFORM

    # List's types.
    # For merging strategies. Prevent merging data from lists
    # that do not have types in common
    POSSIBLE_TYPES = {entity_types.GROUND_OBSERVATORY,
                      entity_types.INVESTIGATION,
                      entity_types.TELESCOPE,
                      entity_types.AIRBORNE,
                      entity_types.SPACECRAFT}

    # No need to disambiguate the type with LLM.
    # Useful for merging strategy: when the type is ambiguous,
    # it is recommanded to not discriminate on types.
    # 1: always known.
    # 0.5: partially known (see individuals)
    # 0: never known.
    TYPE_KNOWN = 1 # Known now that we created specific types to fit SPASE model

    # Folders that we want to keep must contain
    KEEP_FOLDERS = ["Observatory",
                    "Instrument"]

    # Mapping between PDS xml files and our dictionary format
    FACILITY_ATTRS = {"ResourceID": "code",
                      "ResourceName": "label",
                      "Description": "description",
                      #"URL": "url",
                      "AlternateName": "alt_label",
                      "ObservatoryRegion": "location",
                      "ObservatoryGroupID": "is_part_of",
                      "ObservatoryID": "is_part_of",
                      "Agency": "funding_agency",
                      #"Aknowledgement": "",
                      "Latitude": "latitude",
                      "Longitude": "longitude",
                      "Elevation": "altitude",
                      "StartDate": "start_date",
                      "StopDate": "end_date",
                      "PriorIDs": "prior_id",
                      "PriorID": "prior_id",
                      "InstrumentType": "instrument_type",
                      "InvestigationName": "investigation_name"}


    def __init__(self):
        pass


    def __str__(self):
        return self.NAMESPACE


    def extract(self,
                from_cache: bool = True) -> dict:
        """
        Extract the github content into a dictionary.
        """
        # pull if not exist
        CacheManager.git_pull(self.URL,
                              self.GIT_REPO,
                              list_name = self.CACHE)

        # get files from the git repo that are Observatory .json
        files = self._list_files(str(CACHE_DIR / self.CACHE / self.GIT_REPO))

        result = dict()

        for file in files:

            with open(file, "r") as f:
                content = f.read()

            if not content:
                continue

            dict_content = json.loads(content)

            data = dict() # Dict to add to the result dict

            alt_labels = set()

            launch_year = ""

            label = ""

            for rel, values in extract_items(dict_content):
                if rel not in self.FACILITY_ATTRS:
                    continue

                key = self.FACILITY_ATTRS.get(rel)

                if type(values) == str:
                    values = {values}
                for value in values:
                    value = clean_string(value)
                    if value == "None" or value == "-":
                        continue
                    elif "PriorID" in rel:
                        if "\n" in value or len(value) > 150:
                            key = self.FACILITY_ATTRS["Description"]
                        else:
                            value.removesuffix('.') # Remove final '.'
                            prior_id = value
                    elif key == "longitude":
                        # Remove final 'E' (East) to have a valid float value
                        if value[-1] == 'E':
                            value = value[:-1]
                        value = float(value)
                    elif key == "latitude":
                        value = float(value)
                    elif key == "description":
                        if value.startswith("includes observatory/station name,"):
                            continue # Ignore this description
                    elif key in ("start_date", "end_date"):
                        if value.startswith("2000-01-01"):
                            continue # Default date, ignore
                    elif key == "investigation_name":
                        if value == "None Identified":
                            continue # For instruments
                    # Add to the data dict
                    if key == "label":
                        assert len(values) == 1
                        label = value
                        assert "label" not in data
                        data[key] = value
                    elif key == "alt_label":
                        ok, nssdc_id, launch_date = has_cospar_nssdc_id(value)
                        if ok:
                            data["NSSDCA_ID"] = nssdc_id
                            data["COSPAR_ID"] = nssdc_id
                            launch_year = launch_date
                        else:
                            value = value.replace("Observatory Station Code: ", "")
                            alt_labels.add(value)
                    elif key in data:
                        data[key].add(value)
                    else:
                        data[key] = {value}

            # alt labels
            if alt_labels:
                data["alt_label"] = alt_labels

            # launch date
            if launch_year and "launch_date" not in data:
                data["launch_date"] = launch_year


            # url
            href = self.URL + "/tree/master" + file.split(self.GIT_REPO)[1]
            data["url"] = {href}

            # type
            self._get_type(data)

            assert "label" in data
            assert "type" in data

            term = file.split(self.GIT_REPO)[1]
            term = term.removesuffix(".json")
            result[term] = data

        data_fixer.fix(result, self)
        self._self_merge(result)
        self._restore_self_ref(result)
        self._self_merge_instruments(result)
        self._restore_self_ref_part(result)
        self._remove_reflexive_parts(result)
        return result


    def _create_entity_from_missing_id(self,
                                       identifier: str) -> dict:
        """
        Creates a data dictionary for an identifier that does not exist in the
        SPASE database but should still be added to the ontology.

        Args:
            identifier: the ID of the entity in SPASE
        """
        data = dict()
        # Remove .json & add "/"
        # label = identifier.split("/")[-1]
        label = identifier.split(self.KEEP_FOLDERS[0])[-1].split(self.KEEP_FOLDERS[1])[-1]
        label = label.removesuffix(".json")
        label = re.sub(r"[_/]", " ", label).strip()
        data = {"label": label,
                "code": identifier}
        return data


    def _list_files(self,
                    folder: str,
                    visited_folders: Set = None) -> Set:
        """
        Get the list of paths recursively in the folder.
        Only return .json files located in any Observatory folder.

        Args:
            folder: the root folder
            visited_folders: prevent looping in the tree
        """
        result = set()
        if visited_folders is None:
            visited_folders = set()
        for root, dirs, files in os.walk(folder):
            for file in files:
                for FOLDER in self.KEEP_FOLDERS:
                    if f"/{FOLDER}/" in folder + "/" and file.endswith(".json"):
                        result.add(str(Path(root) /  file))
            for dir in dirs:
                dir = str(Path(root) / dir)
                if dir not in visited_folders:
                    visited_folders.add(dir)
                    result.update(self._list_files(dir,
                                  visited_folders = visited_folders))
        return result


    def _get_type(self,
                  data: dict):
        """
        Add the type of the entity to the data dictionary
        """
        location_space = [
            "Asteroid",
            "Comet",
            "Earth.Magnetosheath",
            "Earth.Magnetosphere",
            "Earth.Magnetosphere.Magnetotail",
            "Earth.Magnetosphere.Main",
            "Earth.Magnetosphere.Polar",
            "Earth.Magnetosphere.RadiationBelt",
            "Earth.Moon",
            "Earth.NearSurface",
            "Earth.NearSurface.Atmosphere",
            "Earth.NearSurface.AuroralRegion",
            "Earth.NearSurface.EquatorialRegion",
            "Earth.NearSurface.Ionosphere",
            "Earth.NearSurface.Ionosphere.DRegion",
            "Earth.NearSurface.Ionosphere.ERegion",
            "Earth.NearSurface.Ionosphere.FRegion",
            "Earth.NearSurface.Ionosphere.Topside",
            "Earth.NearSurface.Mesosphere",
            "Earth.NearSurface.Plasmasphere",
            "Earth.NearSurface.PolarCap",
            "Earth.NearSurface.Stratosphere",
            "Earth.NearSurface.Thermosphere",
            "Heliosphere",
            "Heliosphere.Inner",
            "Heliosphere.NearEarth",
            "Heliosphere.Outer",
            "Heliosphere.Remote1AU",
            "Jupiter",
            "Jupiter.Magnetosphere",
            "Jupiter.Magnetosphere.Magnetotail",
            "Mars",
            "Mars.Phobos",
            "Mercury",
            "Pluto",
            "Saturn",
            "Saturn.Magnetosphere",
            "Sun",
            "Sun.Chromosphere",
            "Sun.Corona",
            "Sun.TransitionRegion",
            "Venus"
        ]
        location_ground = [
            "Earth.Surface",
            "Earth"
        ]
        if not "location" in data:
            for code in data["code"]:
                if "NumericalData" in code:
                    data["type"] = entity_types.SURVEY
                elif "/Instrument/" in code:
                    data["type"] = entity_types.INSTRUMENT
                elif "/Observatory/" in code:
                    data["type"] = entity_types.GROUND_FACILITY
            # TODO elif "Instrument" in data["code"]: else: default type (obs facility) & type_confidence to 0
            # (right now only one case has nor Instrument neither NumericalData, but it is an instrument so
            # it will be covered by 'else'.)
        else:
            for l in data["location"]:
                if l in location_ground:
                    data["type"] = entity_types.GROUND_FACILITY
                    break
                elif l in location_space:
                    data["type"] = entity_types.SPACE_FACILITY
                    break
                else:
                    # Still SPACE_FACILITY because there is a location.
                    print("Warning: new unspecified location in spase:", data["location"])
                    print("Please add it to space_extractor")
                    data["type"] = self.DEFAULT_TYPE
            data["type_confidence"] = 1


    def _self_merge(self,
                    result: dict) -> dict:
        """
        Merge spase entities that are the same but come from
        different catalogs. Often, their label's first term is
        the same and they must be compatible (distance, COSPAR ID).

        Returns:
            the old label with the new label to use instead
        """
        reached = 0
        replacement_labels = dict()
        uf = UnionFind()
        groups = defaultdict(set)
        items = sorted(result.items()) # Make sure to always get the same uris
        for uri1, data1 in items:
            if data1.get("type") == entity_types.INSTRUMENT:
                continue
            label1 = data1["label"]
            labels1 = data1.get("alt_label", set())
            labels1.add(label1)
            first_words1 = {label.replace('-', ' ').replace('.', ' ').split(' ')[0] for label in labels1 if label}

            # description1 = data1.get("description", "")
            num1 = get_suffix_number(uri1)
            # word1 = label1.replace('-', ' ').split(' ')[0].lower() # .replace('/', ' ')
            for uri2, data2 in items:
                if uri1 >= uri2:
                    continue
                # Filter on type
                if data1.get("type") != data2.get("type"):
                    continue

                # Prevent two objects from the same folder to match
                """
                urls1 = data1.get("url", [])
                urls2 = data2.get("url", [])
                if type(urls1) == str:
                    urls1 = [urls1]
                if type(urls2) == str:
                    urls2 = [urls2]
                interrupt = False
                for url1 in urls1:
                    for url2 in urls2:
                        if url1.split('/')[7] == url2.split('/')[7]:
                            interrupt = True
                            break
                    if interrupt:
                        break
                if interrupt:
                    continue
                """

                # Merge on identifiers
                to_check = ["NSSDCA_ID", "prior_id", "code"]
                v1 = []
                v2 = []
                for attr in to_check:
                    if attr in data1:
                        v1.extend(data1[attr])
                    if attr in data2:
                        v2.extend(data2[attr])
                if set(v1) & set(v2):
                    uf.union(uri1, uri2)
                    continue

                # Solution: use the longest label as pref label
                num2 = get_suffix_number(uri2)
                if num1 != num2:
                    # Prevent Cluster, Cluster 1 and Cluster 2 to map together
                    # Problem: Cluster-Rumba is the same as Cluster 1.
                    # Same for SWARM, SWARM-A, SWARM-B, SWARM-C
                    continue
                label2 = data2["label"]
                labels2 = data2.get("alt_label", set())
                labels2.add(label2)
                first_words2 = {label.replace('-', ' ').replace('.', ' ').split(' ')[0] for label in labels2 if label}
                # Merge on first words
                # word2 = label2.replace('-', ' ').split(' ')[0].lower() # .replace('/', ' ')
                # Filter out incompatible entries
                #if (word1 != word2 or not word1):# and description1 != description2:
                #    continue
                if not first_words1 & first_words2:
                    continue
                if "NSSDCA_ID" in data1 and "NSSDCA_ID" in data2:
                    if data1["NSSDCA_ID"] != data2["NSSDCA_ID"]:
                        continue
                if uri1.split('/')[-1] == uri2.split('/')[-1]:
                    uf.union(uri1, uri2)
                    continue
                if "latitude" in data1 and "latitude" in data2 and "longitude" in data1 and "longitude" in data2:
                    # Not reached the same amount of time !
                    reached += 1
                    latitude1 = data1["latitude"]
                    longitude1 = data1["longitude"]
                    latitude2 = data2["latitude"]
                    longitude2 = data2["longitude"]
                    if type(latitude1) in (set, list):
                        assert len(latitude1) == 1
                        latitude1 = list(latitude1)[0]
                        longitude1 = list(longitude1)[0]
                    if type(latitude2) in (set, list):
                        assert len(latitude2) == 1
                        latitude2 = list(latitude2)[0]
                        longitude2 = list(longitude2)[0]
                    dist = distance((latitude1, longitude1), (latitude2, longitude2))
                    if dist < 4.0: # change from 4.0 to 30.0 for Fort.Rae & FRA. Better: fix in data_fixer for this case only.
                        uf.union(uri1, uri2)
                        continue
                elif "start_date" in data1 and "start_date" in data2:
                    if data1["start_date"] == data2["start_date"]:
                        uf.union(uri1, uri2)
                        continue
                elif "launch_date" in data1 and "launch_date" in data2:
                    if data1["launch_date"] == data2["launch_date"]:
                        uf.union(uri1, uri2)
                        continue
                alt_labels1 = data1.get("alt_label", set())
                alt_labels2 = data2.get("alt_label", set())
                label1, acr1 = cut_acronyms(label1)
                label2, acr2 = cut_acronyms(label2)
                alt_labels1.add(label1)
                alt_labels2.add(label2)
                alt_labels1 = {l.replace('-', ' ').lower() for l in alt_labels1}
                alt_labels2 = {l.replace('-', ' ').lower() for l in alt_labels2}
                if any(l.startswith("iaga") for l in alt_labels1):
                    for label in alt_labels1.copy():
                        alt_labels1.add(label + " geomagnetic observatory")
                if any(l.startswith("iaga") for l in alt_labels2):
                    for label in alt_labels2.copy():
                        alt_labels2.add(label + " geomagnetic observatory")
                if alt_labels1 & alt_labels2:
                    uf.union(uri1, uri2)
                    continue

        for uri in result:
            groups[uf.find(uri)].add(uri)
        for synset in groups.values():
            if len(synset) <= 1:
                continue
            # keep the longest label
            synset = sorted(synset, key=lambda x: (len(x), x), reverse=True)
            # longest = synset[0]
            for i in range(0, len(synset)):
                longest = synset[i]
                if "Deprecated" not in longest:
                    # Do not keep "deprecated" in the term unless
                    # they are all deprecated (ex: OBSPM exists only in
                    # a /Deprecated folder)
                    break
            data1 = result[longest]

            for label2 in synset:
                if label2 == longest:
                    continue
                data2 = result[label2]
                merge_into(data1, data2)
                del result[label2]
                replacement_labels[label2] = longest
        # TODO now do an UnionFind for ids & prior ids
        return replacement_labels


    def _self_merge_instruments(self,
                                result: dict):
        """
        Merge instruments that have the same host and
        the same label. Call after merging facilities
        and restoring refs.

        Args:
            result: the result dict
        """
        uf = UnionFind()
        all_instruments = set()
        for _, data in sorted(result.items()):
            # has_part
            has_part = sorted(data.get("has_part", []))
            # SWARM -> SWARM-A, SWARM-B, SWARM-C (bug)
            if len(has_part) <= 1:
                continue
            for instrument1 in has_part:
                data1 = result[instrument1]
                if data1["type"] != entity_types.INSTRUMENT:
                    continue
                for instrument2 in has_part:
                    if instrument1 >= instrument2:
                        continue
                    data2 = result[instrument2]

                    if data2["type"] != entity_types.INSTRUMENT:
                        continue

                    # If same label
                    alt_labels1 = data1.get("alt_label", set())
                    alt_labels2 = data2.get("alt_label", set())
                    # TODO try to use alt labels here (as in _self_merge)
                    label1 = data1["label"]
                    label2 = data2["label"]
                    alt_labels1.add(label1)
                    alt_labels2.add(label2)
                    # if label1.lower() == label2.lower():
                    if alt_labels1 & alt_labels2:
                        uf.union(instrument1, instrument2)
                        all_instruments.update([instrument1, instrument2])
                        continue

                    # If same filename
                    urls1 = data1["url"]
                    urls2 = data2["url"]
                    if type(urls1) == str:
                        urls1 = [urls1]
                    if type(urls2) == str:
                        urls2  = [urls2]
                    urls1 = [f1.split('/')[-1] for f1 in urls1 if f1]
                    urls2 = [f2.split('/')[-1] for f2 in urls2 if f2]
                    if set(urls1) & set(urls2):
                        uf.union(instrument1, instrument2)
                        all_instruments.update([instrument1, instrument2])
                        continue
                    # If same identifier / prior id / NSSDCA ID
                    """
                    to_check = ["NSSDCA_ID", "prior_id", "code"]
                    v1 = []
                    v2 = []
                    for attr in to_check:
                        if attr in data1:
                            v1.extend(data1[attr])
                        if attr in data2:
                            v2.extend(data2[attr])
                    if set(v1) & set(v2):
                        uf.union(instrument1, instrument2)
                        all_instruments.update([instrument1, instrument2])
                        continue
                    """
        for instrument2 in all_instruments:
            instrument1 = uf.find(instrument2)
            if instrument1 == instrument2:
                continue
            data1 = result[instrument1]
            data2 = result[instrument2]
            merge_into(data1, data2)
            del result[instrument2]


    def _restore_self_ref(self,
                          result: dict):
        """
        Use the new identifier of the referenced entity
        (isPartOf, hasPart)
        """
        new_by_old = dict()
        for key, data in result.items():
            codes = data.get("code", set())
            prior_ids = data.get("prior_id", set())
            codes.update(prior_ids)
            for code in codes:
                new_by_old[code] = key
        for key, data in result.items():
            is_part_of = data.get("is_part_of", set())
            new_is_part_of = set()
            for part in is_part_of:
                try:
                    part = new_by_old[part]
                except:
                    # FIXME fix this in the data_fixer instead (find the identifiers that do not point to any
                    part = new_by_old[part.replace("/CDPP/", "/CDPP-Archive/")]
                new_is_part_of.add(part)
            data["is_part_of"] = new_is_part_of
            has_part = data.get("has_part", set())
            new_has_part = set()
            for part in has_part:
                try:
                    part = new_by_old[part]
                except:
                    part = new_by_old[part.replace("/CDPP/", "/CDPP-Archive/")]
                new_has_part.add(part)
            data["has_part"] = new_has_part

        # Create reciprocal relation (SPASE only has is_part_of relation)
        for key, data in result.items():
            is_part_of = data.get("is_part_of", set())
            for part in is_part_of:
                if "has_part" in result[part]:
                    result[part]["has_part"].add(key)
                else:
                    result[part]["has_part"] = {key}


    def _restore_self_ref_part(self,
                               result: dict):
        """
        Remove narrowers that are not referenced in the
        result dict.
        """
        for _, data in result.items():
            parts = data.get("has_part", [])
            for part in parts.copy():
                if part not in result:
                    parts.remove(part)


    def _remove_reflexive_parts(self,
                                result):
        """
        Sometimes, because some SPASE entities were
        considered different by the data providers,
        and part of each other, but we consider them the same,
        it results as some of the entities to be part of themselves,
        in which case we remove this relation to itself.
        """
        for key, data in result.items():
            parts = data.get("is_part_of", [])
            for part in parts.copy():
                if part == key:
                    parts.remove(key)
            parts = data.get("has_part", [])
            for part in parts.copy():
                if part == key:
                    parts.remove(key)


def extract_items(d: dict,
                  parent: str = "") -> list[tuple]:
    """
    Flatten a recursive dictionary to a list of (key, value).
    This is necessary to create triplets from for json format.

    Args:
        d: a recursive dictionary.
        parent: the parent XML div type.
    """
    result = []
    for key, value in d.items():
        if isinstance(value, dict):
            result.extend(extract_items(value, parent = key))
        else:
            if parent == "InformationURL" and key != "URL":
                # SPASE: ignore every side information about the url.
                continue
            result.append((key, value))
    return result

if __name__ == "__main__":
    pass
