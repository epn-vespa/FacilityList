import subprocess
import json
from pathlib import Path

HPDE_IO_DATA_PATH = Path(__file__).parent / 'input_data' / 'hpde.io'


def hpde_data_git_pull():
    print("Updating HPDE data:")
    messages = subprocess.Popen(
        ["git", "pull"],
        cwd=HPDE_IO_DATA_PATH,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    ).communicate()
    for item in messages:
        item_utf8 = item.decode("utf-8")
        if item_utf8 != "":
            print(item_utf8)


def browse_hpde_data(
        resource_type: str = "Observatory",
        deprecated: bool = False
):
    # list directories for each naming authority
    directories = list(HPDE_IO_DATA_PATH.glob(f'*/{resource_type}/'))
    if deprecated:
        directories.extend(list(HPDE_IO_DATA_PATH.glob(f'Deprecated/*/{resource_type}/')))
    for directory in directories:
        for item in directory.glob("**/*.json"):
            yield item


def process_hpde_record(hpde_file_path: Path) -> dict:
    with open(hpde_file_path) as fp:
        input_data = json.load(fp)
    item_uri = input_data['Spase']["Observatory"]["ResourceID"]
    res_head = input_data['Spase']["Observatory"]["ResourceHeader"]
    metadata = {
        "ResourceID": item_uri,
        "ResourceName": res_head["ResourceName"]
    }
    if "AlternateName" in res_head.keys():
        metadata["AlternateName"] = res_head["AlternateName"]
    if "PriorID" in res_head.keys():
        metadata["PriorID"] = res_head["PriorID"]
    if "ObservatoryGroupID" in input_data['Spase']["Observatory"].keys():
        metadata["ObservatoryGroupID"] = input_data['Spase']["Observatory"]["ObservatoryGroupID"]
    return metadata


if __name__ == '__main__':
    hpde_data_git_pull()
    spase_metadata = {}
    for file_path in browse_hpde_data():
        record = process_hpde_record(file_path)
        spase_metadata[record["ResourceID"]] = record

    output_file = HPDE_IO_DATA_PATH.parent / "spase.json"
    with open(output_file, "w") as f:
        json.dump(spase_metadata, f, indent=4)
    print(f"Extracted metadata writen in {output_file}")
