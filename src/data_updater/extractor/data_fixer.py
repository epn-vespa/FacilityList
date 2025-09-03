"""
Fix data errors in sources.
"""
import json
from config import DATA_DIR # type: ignore
from data_updater.extractor.extractor import Extractor
from utils.utils import merge_into

def fix(result: dict,
        source: Extractor):
    """
    Fix a result dict (after retrieving the whole data dicts for
    the source). Some entities can be deleted, renamed or updated.

    Note: better to call it before classifying entities with LLM
    if there are data to be deleted.

    Keyword arguments:
    result -- the result dict of an extractor
    source -- the extractor
    """
    filename = source.NAMESPACE + "_data.json"
    filename = str(DATA_DIR / "fixes" / filename)
    with open (filename, 'r') as file:
        fixes = json.load(file)

        # Rename data entries
        for key, fixed_data in fixes.get("rename", {}).items():
            if key not in result:
                print(f"Error in {str(filename)}: {key} not in the result dict. Perhaps the resource label changed ?")
                continue
            result[fixed_data] = result[key]
            del result[key]

        # Data update (fix)
        for key, fixed_data in fixes.get("update", {}).items():
            if key not in result:
                print(f"Error in {str(filename)}: {key} not in the result dict. Perhaps the resource label changed ?")
                continue
            source_data = result[key]
            new_key = key
            for attr, new_value in fixed_data.items():
                source_data[attr] = new_value
                if attr == "label":
                    # fix label
                    new_key = new_value
            if new_key != key:
                # Label was fixed, so update identifier
                if new_key not in result:
                    result[new_key] = source_data
                else:
                    merge_into(source_data, result[new_key])
                del result[key]

        # Delete data entries
        for key in fixes.get("delete", []):
            if key not in result:
                print(f"Warning: {key} not in {filename}, cannot be deleted. Perhaps the resource label changed ?")
                continue
            del result[key]


def link_has_part(result: dict):
    """
    Create has_part relations for all is_part_of relations that
    are linked to an entity. This is useful as we do not want to
    re-write the 'has_part' relations in a data fix file, thus we only
    need to add the new is_part_of relations.

    /!\ this method should also remove the is_part_of that are not in
    the result dict.

    Keyword arguments:
    result -- the result dict of an extractor
    """
    for key, value in result.items():
        if "is_part_of" in value:
            is_part_of = value["is_part_of"]
            if type(is_part_of) == str:
                is_part_of = [is_part_of]
            for part in is_part_of:
                # Check if this part exists in result.
                # If yes, make sure it has an has_part to key. If not, delete it.
                if part in result:
                    # Add this item's key as the has_part of its broader entity.
                    value_part = result[part]
                    if "has_part" in value_part:
                        has_part = value_part["has_part"]
                        if key not in has_part:
                            if type(has_part) == str:
                                value_part["has_part"] = [has_part, key]
                            else:
                                value_part["has_part"].append(key)
                    else:
                        value_part["has_part"] = [key]
                else:
                    if type(value["is_part_of"]) == str:
                        del value["is_part_of"]
                    else:
                        # Delete the is_part_of (linked to nothing in the result dict)
                        is_part_of = set(is_part_of)
                        is_part_of.remove(part)
                        value["is_part_of"] = list(is_part_of)
        if "has_part" in value:
            has_part = value["has_part"]
            if type(has_part) == str:
                has_part = [has_part]
            for part in has_part:
                # Check if this part exists in result.
                # If yes, make sure it has an has_part to key. If not, delete it.
                if part in result:
                    value_part = result[part]
                    if "is_part_of" in value_part:
                        is_part_of = value_part["is_part_of"]
                        if key not in is_part_of:
                            if type(is_part_of) == str:
                                value_part["is_part_of"] = [is_part_of, key]
                            else:
                                value_part["is_part_of"].append(key)
                    else:
                        value_part["is_part_of"] = [key]
                else:
                    if type(value["has_part"]) == str:
                        del value["has_part"]
                    else:
                        # Delete the has_part (linked to nothing in the result dict)
                        has_part = set(has_part)
                        has_part.remove(part)
                        value["has_part"] = list(has_part)