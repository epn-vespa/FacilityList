from typing import List, Dict, Tuple


def merge_into(newer_entity_dict: Dict,
               prior_entity_dict: Dict):
    """
    Merge data from the prior dict into the newer dict.
    Only keep the most precise latitude & longitude.

    Args:
        newer_entity_dict: the entity dict to save data in
        prior_entity_dict: the prior entity dict to merge into the newer
    """
    for key, values in prior_entity_dict.copy().items():
        if key == "prior_id":
            continue
        if key == "label":
            if not isinstance(values, set):
                values = {values}
            if "alt_label" in newer_entity_dict:
                 # Keep the old label as an alternate label of the new entity
                if type(newer_entity_dict["alt_label"]) == list:
                    newer_entity_dict["alt_label"] = set(newer_entity_dict["alt_label"])
                # set
                newer_entity_dict["alt_label"].update(values)
            else:
                newer_entity_dict["alt_label"] = values
        elif key in newer_entity_dict:
            merge_into = newer_entity_dict[key]
            if not isinstance(values, list) and not isinstance(values, set):
                values = [values]
            for value in values:
                if isinstance(merge_into, set):
                    merge_into.add(value)
                    continue
                elif not isinstance(merge_into, list):
                    merge_into = [merge_into]
                if value not in merge_into:
                    if key in ["latitude", "longitude"]:
                        # Keep the most precise value
                        old_value = newer_entity_dict[key]
                        if isinstance(old_value, list):
                            old_value = old_value[0]
                        if (len(str(value)) > len(str(old_value)) and
                            str(value).startswith(str(old_value))):
                            merge_into = [value]
                        elif (len(str(old_value)) > len(str(value)) and
                              str(old_value).startswith(str(value))):
                            merge_into = [old_value]
                        elif len(str(value)) == len(str(old_value)):
                            if value != old_value:
                                merge_into = [value, old_value] # Keep both
                            else:
                                merge_into = [old_value]
                        elif value != old_value:
                            # Keep both
                            merge_into = [value, old_value]
                    else:
                        merge_into.append(value)
            newer_entity_dict[key] = merge_into
        else:
            newer_entity_dict[key] = values
        if "alt_label" in newer_entity_dict and "label" in newer_entity_dict:
            # Prevent label to be in alt_label.
            if type(newer_entity_dict["alt_label"]) == str:
                newer_entity_dict["alt_label"] = {newer_entity_dict["alt_label"]}
            newer_entity_dict["alt_label"] -= {newer_entity_dict["label"]}


def extract_items(d: Dict,
                  parent: str = "") -> List[Tuple]:
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