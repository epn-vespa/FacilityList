"""
Fix data errors in sources.
"""
import json
from config import DATA_DIR # type: ignore
from data_updater.extractor.extractor import Extractor

def fix(result: dict,
        source: Extractor) -> str:
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
        for key, fixed_data in fixes["update"].items():
            if key not in result:
                print(f"Error in {str(filename)}: {key} not in the result dict. Perhaps the resource label changed ?")
                continue
            source_data = result[key]
            for attr, new_value in fixed_data.items():
                source_data[attr] = new_value

        # Delete data entries
        for key in fixes["delete"]:
            if key not in result:
                print(f"Warning: {key} not in {filename}, cannot be deleted. Perhaps the resource label changed ?")
                continue
            del result[key]
