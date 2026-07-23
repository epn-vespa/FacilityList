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

from datetime import date


def main(input_folder,
         lists,
         strategy_file):
    today = date.today()
    output_folder = Path("src") / "output" / f"today"
    output_folder.mkdir(parents = True, exist_ok = True)

    """
    Step 1. update the ontology based on the previous version (linked.ttl)
    """
    input_linked_ontology_file = input_folder / "linked.ttl"
    output_updated_ontology_file = output_folder / "updated.ttl"
    output_linked_ontology_file = output_folder / "linked.ttl"
    update.main(lists,
                input_linked_ontology_file,
                output_updated_ontology_file,
                from_cache = True,
                remove_deprecated = False)

    """
    Step 2. use apply_links to the newly updated ontology
    to keep mappings.ttl in the new file
    """
    apply_links.main(input_ontology_path = output_updated_ontology_file,
                     input_sssom_ontology_path = input_folder / "mapping.ttl",
                     output_ontology_path = output_linked_ontology_file,
                     llm_manual_only = False)

    """
    Step 3. (re-)map ontologies: only entities that were modified after this update.
    """
    map_ontologies.main(output_linked_ontology_file,
                        output_dir = output_folder,
                        strategy_file = strategy_file,
                        human_validation = False,
                        modified_after = today)

    # Here some manual intervention on mappings can be performed using manual_review.py

    """
    Step 4. generate outputs
    """
    # Generate labels & pref labels for synonym sets
    generate_views.main(input_ontology = output_linked_ontology_file,
                        output_merged = "merged.ttl",
                        community_views = None)

    """
    Step 5. Add an Ontology version to output ontologies based on the previous version number
    """


if __name__ == "__main__":
    parser = ArgumentParser(prog="make_release",
                            description="Create a new release from a previous one.")
    parser.add_argument("-f",
                        "--folder",
                        description = "Previous update folder",
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

    parser.add_argument("-s",
                        "--strategy-file",
                        dest="strategy_file",
                        required=False,
                        type=str,
                        default = str(Path(__file__).parent.parent / "conf" / "default_strategy.conf"),
                        help="Folder to save the output turtle files.")

    main(parser.folder,
         parser.lists,
         parser.strategy_file)

