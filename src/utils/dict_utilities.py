from collections import defaultdict, Counter
from typing import List, Dict, Tuple
from graph.properties import Properties
from rdflib import URIRef, XSD
from datetime import datetime, timezone


def merge_into(newer_entity_dict: Dict,
               prior_entity_dict: Dict):
    """
    Merge data from the prior dict into the newer dict.
    Only keep the most precise latitude & longitude.
    The prior entity dict will not be modified, only the
    newer entity dict will be augmented with the prior entity
    dict's keys and values.

    Args:
        newer_entity_dict: the entity dict to save data in
        prior_entity_dict: the prior entity dict to merge into the newer
    """
    for key, values in prior_entity_dict.copy().items():
        #if key == "prior_id":
        #    continue
        if type(values) != set:
            if type(values) == list:
                values = set(values)
            else:
                values = {values}
        # Label
        if key == "label":
            if not newer_entity_dict.get("label", None) and prior_entity_dict.get("label", None):
                # prior dict has a label but not the newer dict. Transfer this label
                newer_entity_dict["label"] = prior_entity_dict["label"]
            elif "alt_label" in newer_entity_dict:
                 # Keep the old label as an alternate label of the new entity
                if type(newer_entity_dict["alt_label"]) == list:
                    newer_entity_dict["alt_label"] = set(newer_entity_dict["alt_label"])
                newer_entity_dict["alt_label"].update(values)
            else:
                newer_entity_dict["alt_label"] = values
        elif key in newer_entity_dict:
            merge_into = newer_entity_dict[key]
            if isinstance(merge_into, set):
                pass
            elif isinstance(merge_into, list):
                merge_into = set(merge_into)
            else:
                merge_into = {merge_into}
            for value in values:
                if value not in merge_into:
                    merge_into.add(value)
            newer_entity_dict[key] = merge_into
        else:
            newer_entity_dict[key] = values
        if "alt_label" in newer_entity_dict and "label" in newer_entity_dict:
            # Prevent label to be in alt_label.
            if type(newer_entity_dict["alt_label"]) == str:
                newer_entity_dict["alt_label"] = {newer_entity_dict["alt_label"]}
            elif type(newer_entity_dict["alt_label"]) == list:
                newer_entity_dict["alt_label"] = set(newer_entity_dict["alt_label"])
            newer_entity_dict["alt_label"] -= {newer_entity_dict["label"]}
        # TODO do the same for id and prior_id


class UnionFind:
    """
    Disjoint Set Union (Union-Find) data structure.

    Maintains a collection of disjoint sets over arbitrary hashable elements.
    Supports efficient union and find operations with:
      - path compression (during find)
      - union by rank (tree height heuristic)

    Amortized time complexity per operation is effectively O(1).
    """

    def __init__(self):
        # parent[x] = parent of x in the union-find tree.
        # If parent[x] == x, then x is the representative (root) of its set.
        self.parent = {}

        # rank[x] = an upper bound on the height of the tree rooted at x.
        # Used only to decide which root becomes the parent during union.
        self.rank = {}


    def find(self, x):
        """
        Find the representative (root) of the set containing x.

        This method applies path compression:
        after the call, x (and all nodes visited on the path)
        will point directly to the root, flattening the tree
        and speeding up future operations.
        """
        # Lazy initialization: if x has never been seen before,
        # create a new singleton set {x}.
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0
            return x

        # If x is not its own parent, recursively find the root
        # and compress the path by updating parent[x].
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])

        return self.parent[x]

    def union(self, x, y):
        """
        Merge the two sets containing x and y.

        Uses union by rank:
        - the root with smaller rank is attached under the root
          with larger rank
        - if ranks are equal, one root is chosen arbitrarily and
          its rank is incremented

        If x and y are already in the same set, this is a no-op.
        """
        # Find the representatives of both elements
        rx = self.find(x)
        ry = self.find(y)

        # Already in the same set → nothing to do
        if rx == ry:
            return

        # Attach the smaller-rank tree under the larger-rank tree
        if self.rank[rx] < self.rank[ry]:
            self.parent[rx] = ry
        elif self.rank[rx] > self.rank[ry]:
            self.parent[ry] = rx
        else:
            # Same rank: choose one root and increase its rank
            self.parent[ry] = rx
            self.rank[rx] += 1


properties = Properties()
def majority_voting_merge(dicts: list[dict],
                          str_keys: bool = False) -> dict:
    """
    Merge dictionaries' values on their keys. Use it to merge
    synonym sets' values in views generation.
    /!\\ keys must be strings, not URIs, elsewise this will generate errors.

    Args:
        dicts: synonym set dictionaries.
        str_keys: keep new keys as strings and not rdflib Node.

    Latitude/longitude:
        1. Keep the ones with location confidence == 1
        2. Majority voting (with rounding)
        3. Keep the most precise one
    Float values:
        1. Majority voting (with rounding)
        2. Keep the most precise one
    Label:
        1. Wikidata label if wikidata list
        2. Else, do a majority voting
        3. Keep all labels as alt labels
    Location-related values (except latitude / longitude):
        1. Keep the ones with location confidence == 1
        2. Keep the one that come from the entity that we kept latitude/longitude from
    String values:
        1. Keep all of them in the first place
        2. Reificate them (RDF-star ? blank nodes ?) to keep their provenance
        2. Later (in another function), summarize them
    Datetime values:
        1. Keep the most precise of each year
    Remove:
        - Source information (aas_list etc)
        - Type confidence
        - Location confidence
    """
    if not str_keys:
        conv_attr = lambda x: properties.convert_attr(x)
    else:
        conv_attr = lambda x: properties.get_attr_name(x)
    result_dict = defaultdict(list)
    all_keys = set()
    for d in dicts:
        all_keys.update(d.keys())
    lists_with_reliable_location = None
    location_info_by_source = defaultdict(lambda: defaultdict(list))

    wikidata_preflabel = None
    all_labels = []
    except_labels = set()

    for key in all_keys:
        key_uri = properties.convert_attr(key)
        key_type = properties.get_type(key)
        values = _get_values_from_dicts(dicts, key) # dict of values by source {aas: {"alt_label": {"alt_label1", "alt_label2"]}}}
        values_f = flatten(values.values()) # All values as a list of values, without source
        if not values_f:
            continue
        if type(key) != str:
            key = properties.get_attr_name(key)
        if not key_type:
            for v in values_f:
                if v:
                    if type(v) == tuple:
                        key_type = type(v[0])
                    else:
                        key_type = type(v)
                    break
        if not values:
            continue
        #if key in ["latitude", "longitude"]:
        #    pass
        elif key == "label":
            if properties.OBS["wikidata_list"] in values:
                wikidata_preflabel = list(values[properties.OBS["wikidata_list"]])[0]
            all_labels.extend(values_f)
        elif key == "alt_label":
            all_labels.extend(values_f)
        elif key in ["COSPAR_ID", "NSSDCA_ID", "NAIF_ID", "code"]:
            #for value in values_f:
            except_labels.update(values_f)
            # keep all
            result_dict[key_uri] = values_f
        elif key in ["city", "country", "continent", "state", "address", "latitude", "longitude"]:
            for source, v in values.items():
                location_info_by_source[source][key].extend(v)
        elif key == "location_confidence":
            sort_by_loc_conf = [(k, v) for k, v in sorted(values.items(), key = lambda item: item[1], reverse = True)]
            if sort_by_loc_conf:
                best_location_confidence = sort_by_loc_conf[0][1]
            lists_with_reliable_location = [lst for lst, loc in sort_by_loc_conf if loc == best_location_confidence]
        elif key in ["type_confidence"]:
            # Ignore
            pass
        elif key in [properties.convert_attr("modified"), "modified"]:
            values = _keep_most_recent_date(values_f)
        elif key_type in [XSD.float, float]:
            # best_value = _majority_vote_rounding([list(v)[0] for v in values.values()])
            best_value = _majority_vote_rounding(values_f)
            result_dict[key_uri] = best_value
        elif key_type in [XSD.string, str]:
            # Add everything
            result_dict[key_uri].extend(values_f)#values.values())
        elif key_type == URIRef:
            # Add everything
            result_dict[key_uri].extend(values_f)#values.values())
        elif key_type == XSD.dateTime: # DCAT.startDate, DCAT.endDate, OBSF.launch_date
            values = _majority_vote_date(values_f)#values.values())
            result_dict[key_uri].extend(values)
    # Set location from the location with the highest location confidence
    if lists_with_reliable_location:
        # select locations
        for l in lists_with_reliable_location:
            location_dict = location_info_by_source[l]
            for k, v in location_dict.items():
                # k = properties.convert_attr(k)
                result_dict[k].extend(v)
        if properties.latitude in result_dict:
            # majority voting latitude
            lat = _majority_vote_rounding(result_dict[properties.latitude])
            result_dict[properties.latitude] = lat
        elif "latitude" in result_dict:
            lat = _majority_vote_rounding(result_dict["latitude"])
            result_dict["latitude"] = lat
        if properties.longitude in result_dict:
            long = _majority_vote_rounding(result_dict[properties.longitude])
            result_dict[properties.longitude] = long
        elif "longitude" in result_dict:
            long = _majority_vote_rounding(result_dict["longitude"])
            result_dict["longitude"] = long
        for key in ["city", "country", "continent", "state", "address", properties.city, properties.country, properties.continent, properties.state, properties.address]:
            values = result_dict.get(key, None)
            if values:
                value = _majority_vote_exact(values)
                result_dict[key] = value

    # Pref label
    if wikidata_preflabel:
        pref_label = wikidata_preflabel
    else:
        # Remove codes, IDs... from alt labels
        all_labels_2 = [l for l in all_labels if l not in except_labels and (type(l) != tuple or l[0] not in except_labels)]
        if all_labels_2:
            all_labels = all_labels_2 # else it means that all the labels were identifiers/codes
        pref_label = _majority_vote_exact(all_labels)
        # TODO change the pref label selection method to get a more standardized label (without acronym, telescome name... and not keep the longest label with most votes)
    all_labels = set(all_labels) - {pref_label}
    result_dict[conv_attr("label")] = pref_label
    result_dict[conv_attr("alt_label")] = all_labels
    return result_dict


def _get_values_from_dicts(dicts: list[dict],
                           key: str) -> dict:
    """
    Get all values that correspond to a certain key from
    a collection of dictionaries.

    Args:
        dicts: list of dictionaries of data
        key: key to extract data from
    """
    values = dict()
    for d in dicts:
        source = d.get(properties.convert_attr("source"), d.get("source"))
        if source and type(source) == set:
            source = list(source)[0]
        value = d.get(key, None)
        if value:
            if type(value) not in (list, set): # no tuple (tuple is for val, lang)
                value = [value]
            values[source] = value
    return values


def _majority_vote_exact(values: list):
    """
    Keep the value that has the most occurrence (1st criteria)
    and that is the longest after str conversion (2nd criteria).
    For labels (alt labels), keep languages.

    Args:
        values: list of values
    """
    if len(values) == 1:
        return values[0]
    if len(values) == 0:
        return None
    # Get rid of languages (and keep English labels only for labels with languages)
    values_with_lang = values.copy()
    values = []
    only_english_values = []
    for v in values_with_lang:
        if type(v) == tuple and len(v) == 2 and type(v[0]) == str:
            if v[1] == None or v[1][:2] in ["en", "ca"]: # ca is for Catalan but seems to be used for Canadian (en-ca) in WD too
                only_english_values.append(v[0])
            values.append(v[0])
        else:
            values.append(v)
    #if only_english_values:
    #    values = only_english_values # Keep English value if they exist
    counts = Counter(values)
    if not counts:
        return None
    max_count = max(counts.values())
    most_common = [v for v, c in counts.items() if c == max_count]

    if len(most_common) > 0 and only_english_values:
        # choose the ones in the English labels
        most_common_en = [v for v in most_common if v in only_english_values]
        if most_common_en:
            most_common = most_common_en
        else:
            # No most common found in English; return with only English
            best_value = _majority_vote_exact(only_english_values)
            return best_value

    # Longest value in this cluster.
    # If equality, keep the biggest string (2nd sorting criteria)
    best_value = max(most_common, key = lambda v: (len(str(v)), v))

    return best_value


def _majority_vote_rounding(values: list[float]):
    """
    Create clusters by rounding and return the
    value with the most decimals
    """
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    clusters = []
    for v in values:
        if type(v) == tuple:
            # val, lang
            v = v[0]
        if v is None:
            continue
        len_v = len(str(v).split('.')[-1])
        added = False
        for c in clusters:
            for v2 in c:
                len_v2 = len(str(v2).split('.')[-1])
                if len_v > len_v2:
                    if round(v, len_v2) == round(v2, len_v2):
                        c.append(v)
                        added = True
                        break
                else:
                    if round(v, len_v) == round(v2, len_v):
                        c.append(v)
                        added = True
                        break
            if added:
                break
        if not added:
            clusters.append([v])
    if not clusters:
        return None
    # Choose the cluster with the most values
    lengths = [len(c) for c in clusters]
    best_cluster = clusters[lengths.index(max(lengths))]
    # Longest value in this cluster
    longest = -1
    best_value = None
    for value in best_cluster:
        length = len(str(value))
        if length > longest:
            longest = length
            best_value = value
        elif length == longest:
            if value > best_value:
                best_value = value # to keep the function deterministic
    return best_value


def _majority_vote_date(values: list) -> list:
    """
    Get dates that are the most represented (group by year) and
    that are the most precise (have month and day different from 01/01)

    Args:
        values: list of URIRef or iso format date
    """
    if len(values) == 1:
        return values
    count_by_year = defaultdict(int)
    dates_by_year = defaultdict(list)
    for dates in values:
        if type(dates) not in (list, set, tuple):
            dates = [dates]
        for date in dates:
            if not date:
                continue
            if type(date) == str:
                date_iso = datetime.fromisoformat(date)
            else:
                date_iso = date
            count_by_year[date_iso.year] += 1
            dates_by_year[date_iso.year].append(date_iso)
    if not count_by_year:
        return values

    sorted_counts = sorted(count_by_year.items(), key = lambda x: x[1], reverse = True)
    max_count = sorted_counts[0][1]

    # All years that share the maximum vote count
    best_years = [year for year, count in sorted_counts if count == max_count]

    # Collect precise dates if available, otherwise Jan 1 of the year
    precise_dates = []
    other_dates = []
    for year in best_years:
        dates = dates_by_year.get(year)
        for date in dates:
            if type(date) == str:
                date_iso = datetime.fromisoformat(date)
            else:
                date_iso = date
            if date_iso.month != 1 and date_iso.day != 1:
                precise_dates.append(date)
            else:
                other_dates.append(date)

    if precise_dates:
        return set(precise_dates)
    else:
        return set(other_dates)


def _keep_most_recent_date(values: list):
    """
    For last modified values
    """
    #if type(values[0]) == str:
    #    values = [datetime.fromisoformat(d) for d in values]
    def normalize(dt: datetime) -> datetime:
        if dt.tzinfo is None:
            return dt.replace(tzinfo = timezone.utc)
        return dt.astimezone(timezone.utc)

    normalized = [normalize(dt) for dt in values]
    return max(normalized)


def flatten(values: list) -> list:
    """
    Get the values only from a dict of {list: value}.
    """
    res = []
    for v in values:
        res.extend(v)
    return res