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
    the source). Some entities can be deleted, added or updated.

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
