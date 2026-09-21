"""
Script that takes a previous release folder with:
    - a linked.ttl file:  previous result of update + links created by mapping.
    - a synsets.json file: to track entities origins of each synonym sets. This file stores definitive definitions, labels and terms for each synonym set. It is reviewed by humans, contrarily to the LLM caches that should remain the same. This file also keeps the history of deprecation over previous versions, that should appear in the latest version.
    - a mapping.ttl file: to apply mappings from the previous update. (not necessary if any of linked.ttl or synsets.json is set)
    - a merged.ttl file:  result of the previous merge to update the last modification date if any change is detected after the merging process.

The update will take its roots from the cache and version manager to check for individual sources' metadata differences. If any difference is detected, the individual entities' modified date will be modified to the start time of this code run. This will have a direct incidence on the state of linked.ttl, but not necessarily impact the synsets.json file as the mappings may remain the same, nor the merged.ttl file, as some metadata could still be eliminated during the majority voting merge.
"""
### Step 1: call update.py (-f previous_release_folder -l lists)
### generates an updated ontology

### Step 2: call apply_mappings.py on the updated ontology if mapping.ttl is in the input folder

### Step 3: apply synsets json to check that all previous mappings exist, as well as all previous versions (deprecated)

### Step 4: call map_ontologies.py with modified date set to the latest date, so that only the latest entities are mapped (with every entity from the other set that is not in the list yet)

### Step 5: call generate_views.py from the newly created mapped.ttl file


from argparse import ArgumentParser
from pathlib import Path

from graph.extractor.extractor_lists import ExtractorLists
import update
import apply_links
import map_ontologies
import generate_views
from config import OUTPUT_DIR
import config

from datetime import date, datetime


def main(input_folder: str,
         lists: list[str],
         strategy_file: str,
         first_release: bool = False,
         skip_update: bool = False,
         skip_mapping: bool = False):
    if not first_release and not input_folder:
        raise AttributeError("No input folder provided (use -f input_folder, or use --first-release to ignore)")
    today = date.today()
    now = datetime.now()
    output_folder = OUTPUT_DIR / f"{today}"
    output_folder.mkdir(parents = True, exist_ok = True)
    output_linked = output_folder / "linked.ttl"
    output_updated = output_folder / "updated.ttl"
    """
    Pre-check files status
    """
    if input_folder:
        input_folder = Path(input_folder)
        if input_folder.exists():
            input_updated = input_folder / "updated.ttl"
            if not input_updated.exists():
                input_updated = None
            input_linked = input_folder / "linked.ttl"
            if input_linked.exists():
                input_updated = input_linked
            else:
                input_linked = None
    """
    Step 1. update the ontology based on the previous version (linked.ttl)
    """
    """
    if input_folder:
        input_folder = Path(input_folder)
        input_linked_ontology_file = input_folder / "linked.ttl"
        input_updated_ontology_file = input_folder / "updated.ttl"
        if not input_linked_ontology_file.exists():
            input_linked_ontology_file = input_updated_ontology_file
    else:
        input_linked_ontology_file = None
        input_updated_ontology_file = None
    """
    if not skip_update:
        update.main(lists,
                    input_updated,
                    output_updated,
                    from_cache = True,
                    remove_deprecated = False)
    else:
        output_updated = input_updated # TODO remove this if we call update
    """
    Step 2. use apply_links to the newly updated ontology
    to merge the old mappings.ttl in the new folder.
    """
    if not first_release:
        print("input_folder=", input_folder)
        output_updated = apply_links.main(from_folder = input_folder,
                                          to_folder = output_folder)
    """
    Step 3. (re-)map ontologies: only entities that were modified after this update.
    """
    if not skip_mapping:
        if first_release:
            modified_after = None
        else:
            modified_after = now
        map_ontologies.main([str(output_updated)], # Awaits a list of strings
                            output_dir = output_folder,
                            strategy_file = strategy_file,
                            human_validation = False,
                            modified_after = modified_after)

    # Here some manual intervention on mappings can be performed using manual_review.py
    """
    Step 4. generate outputs
    """
    # Generate labels & pref labels for synonym sets
    generate_views.main(input_ontology = output_linked,
                        output_merged = "merged.ttl",
                        community_views = [],
                        previous_output_facilities = input_folder / "facilities.ttl",
                        previous_output_instruments = input_folder / "instruments.ttl")


if __name__ == "__main__":
    parser = ArgumentParser(prog="make_release",
                            description="Create a new release from a previous one.")
    parser.add_argument("-f",
                        "--folder",
                        type = str,
                        required = False,
                        help = "Previous update folder",
                       )

    parser.add_argument("-l",
                        "--lists",
                        dest = "lists",
                        default = ["all"],
                        choices = ["all"] + list(ExtractorLists.EXTRACTORS_BY_NAMES.keys()),
                        nargs = '*',
                        type = str,
                        required = False,
                        help = "Name of the lists to extract. 'all' will" +
                        "extract from all of the lists.")

    parser.add_argument("--first-release",
                        dest = "first_release",
                        action = "store_true",
                        required = False,
                        help = "Name of the lists to extract. 'all' will" +
                        "extract from all of the lists.")

    parser.add_argument("-s",
                        "--strategy-file",
                        dest="strategy_file",
                        required=False,
                        type=str,
                        default=str(Path(__file__).parent.parent / "conf" / "default_strategy.conf"),
                        help="Folder to save the output turtle files.")

    parser.add_argument("--skip-update",
                        dest="skip_update",
                        action="store_true",
                        help="If set, skips the update step.")

    parser.add_argument("--skip-mapping",
                        dest="skip_mapping",
                        action="store_true",
                        help="If set, skips the mapping step.")

    args = parser.parse_args()
    main(args.folder,
         args.lists,
         args.strategy_file,
         args.first_release,
         args.skip_update,
         args.skip_mapping)

