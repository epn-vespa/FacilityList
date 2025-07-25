
"""
Cache manager to prevent requesting websites many times and keep
operational local versions of the web pages. This could also be achieved using
https://web.archive.org/web/date/url instead of the actual page.
The cache is saved locally in the /cache folder. In order to actualize the
pages' content, change the cache's file name or remove it.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

import glob
from pathlib import Path
import pickle
import re
import requests
import logging
import os
import subprocess
import json
import datetime

from config import CACHE_DIR, LOGS_DIR, DATA_DIR # type: ignore
from data_updater.extractor.extractor import Extractor

# Set up the basic configuration for logging
LOG = LOGS_DIR / 'error.log'# "../../cache/error.log"
if os.path.exists(LOG):
    os.remove(LOG) # Clear log file
logging.basicConfig(filename=LOG, level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class CacheManager():

    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
        AppleWebKit/537.36 (KHTML, like Gecko) \
        Chrome/58.0.3029.110 \
        Safari/537.3'}

    """
    # For selenium
    HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Referer': 'https://heliophysicsdata.gsfc.nasa.gov/websearch/dispatcher'}
    """

    def get_page(url: str,
                 list_name: str,
                 params: str = None,
                 from_cache: bool = True,
                 data: dict = None,
                 data_str: str = "") -> str:
        """
        Get a page from cache if it is saved in cache, else scrap it online.
        If data & data_str are set, send a POST request instead of GET.

        Keyword arguments:
        url -- the URL of the page.
        list_name -- used to access the right folder of the cache.
        from_cache -- whether to get content from cache if it exists.
        data -- the extractor's data dict in case of a POST request.
        data_str -- a string to save the response in a specific cache file
                    as the url of the page may not change.
        """
        cache_path = CacheManager._get_cache_path(url, list_name, data_str)
        content = ""
        if from_cache and glob.glob(cache_path):
            with open(cache_path, 'r') as file:
                content = file.read()
        if not content:
            if not data:
                content = CacheManager.get(url, params = params)
            else:
                content = CacheManager.post(url, data)
            if content:
                CacheManager.save_cache(content, cache_path)
        return content


    def _get_cache_path(url: str,
                        list_name: str,
                        data_str: str = "") -> str:
        """
        Get the cache's path from the url.
        The cache folder is located at the root of the project: /cache

        Keyword arguments:
        url -- the URL of the page.
        list_name -- used to access the right folder of the cache.
        data_str -- a string to save the response in a specific cache file
                    as the url of the page may not change.
        """
        # Create folder CACHE
        (CACHE_DIR / list_name).mkdir(parents = True,
                                      exist_ok = True)

        # Create folder list_name
        cache_path = re.sub(r"[^\w\d]+", "_", url)
        if cache_path[-1] == '_':
            cache_path = cache_path[:-1]
        cache_path = cache_path.lower()
        cache_path = CACHE_DIR / list_name / cache_path
        if data_str:
            cache_path.mkdir(parents = True,
                             exist_ok = True)
            cache_path = cache_path / data_str
        cache_path = str(cache_path)
        return cache_path


    def save_cache(content: str,
                   cache_path: str) -> None:
        """
        Save the page in the cache folder.

        Keyword arguments:
        content -- the content of the page.
        cache_path -- where to save the cache.
        """
        with open(cache_path, 'w') as file:
            file.write(content)


    def get(url: str,
            params: dict = None) -> str:
        """
        Scrap the web page's content. Encode it using the response's charset.

        Keyword arguments:
        url -- the url to scrap from.
        """
        try:
            response = requests.get(
                    url,
                    params = params,
                    headers = CacheManager.HEADERS)
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)

        if response.ok:
            return response.text
        else:
            error = f"Request to {url} failed with status code {response.status_code}"
            logging.info(error)
            return ""


    def post(url: str,
             data: dict) -> str:
        try:
            response = requests.post(url,
                                     data,
                                     headers = CacheManager.HEADERS)
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)

        if response.ok:
            return response.text
        else:
            error = f"Request to {url} failed with status code {response.status_code}"
            logging.info(error)
            return ""


    def git_pull(url: str,
                 git_repo: str,
                 list_name: str):
        """
        Pull a github repository and save it in the cache.
        The name of the created folder is git_repo.
        If the folder does not exist, do git clone instead.

        Keyword arguments:
        url -- the github repository's url (clone)
        git_repo -- the name of the repository / folder of the repository
        list_name -- used to access the right folder of the cache.
        """
        try:
            if url.endswith('/'):
                url = url[:-1]
            url += ".git"
            git_repo_folder = str(CACHE_DIR / list_name / git_repo)
            if glob.glob(git_repo_folder):
                command = ["git", "pull"]
                cwd = git_repo_folder
            else:
                command = ["git", "clone", url]
                cwd = str(CACHE_DIR / list_name)
            messages = subprocess.Popen(
                command,
                cwd = cwd,
                stdout = subprocess.PIPE,
                stderr = subprocess.PIPE
            ).communicate()
            for item in messages:
                item_utf8 = item.decode("utf-8")
                if item_utf8 != "":
                    logging.warning(item_utf8)
        except:
            logging.warning(' '.join(command), "failed for", git_repo)


class VersionManager():
    """
    A class that saves version dictionaries and
    contains utility functions to check for updates.
    """

    _TODAY = datetime.datetime.now(tz=datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    VERSION_MANAGER = CACHE_DIR / "version_manager"
    if not os.path.exists(VERSION_MANAGER):
        VERSION_MANAGER.mkdir(parents = True,
                              exist_ok = True)

    def get_newer_keys(prev_version_file: str,
                       new_version: dict,
                       list_name: str) -> set:
        """
        Get a list of keys which are newer compared to the last version date
        of the version file and necessitate to be refreshed.

        Keyword arguments:
        prev_version_file -- json file containing previous versions of uris
        new_version -- dict with newer versions
        list_name -- used to access the right folder of the cache.
        """
        result = set()

        prev_version_file = str(DATA_DIR / list_name / prev_version_file)

        if not os.path.exists(prev_version_file):
            # We create the path to save the versions from the
            # newer version.
            with open(prev_version_file, "w") as f:
                prev_version = {
                    "processing_date": VersionManager._TODAY,
                    "previous_date": VersionManager._TODAY,
                    "results_count": 0,
                    "results": {}
                }
                json.dump(prev_version, f, indent = 4)
            # return new_version.keys()
        else:
            with open(prev_version_file, "r") as f:
                prev_version = json.load(f)

        for uri in new_version["results"]:
            if uri not in prev_version["results"]:
                result.add(uri)
            elif (prev_version["results"][uri]["modified_date"]
                 < new_version["results"][uri]["modified_date"]):
                result.add(uri)
        return result


    def refresh(last_version_file: str,
                new_version: dict,
                list_name: str):
        """
        Replace URIs that are in new_version dict into last_version_file.

        Keyword arguments:
        last_version_file -- the json file containing last versions for each uri
        new_version -- a dict of {uri: {last-modified, name}}
        list_name -- the cache folder of the list to save data in
        """
        # Open last_version_file with read+write access
        save_last_version = False

        last_version_file = VersionManager._get_data_path(last_version_file,
                                                          list_name)
        if os.path.exists(last_version_file):
            with open(last_version_file, "r") as f:
                content = f.read()
                if content:
                    # Merge new_version with content
                    try:
                        last_version = json.loads(content)
                        for key, value in new_version.items():
                            last_version["results"][key] = value
                        if last_version["processing_date"] != VersionManager._TODAY:
                            # Only update date for a different run.
                            last_version["previous_date"] = last_version["processing_date"]
                            last_version["processing_date"] = VersionManager._TODAY
                        last_version["results_count"] = len(last_version["results"])
                        save_last_version = True
                    except json.JSONDecodeError:
                        # If the file is malformated
                        logging.error("FATAL - Error while decoding", last_version_file)
                        raise(json.JSONDecodeError)
                        # to ignore this and still continue even if
                        # the last_version_file is corrupted:
                        return # Still continue to run code
            if save_last_version:
                with open(last_version_file, "w") as f:
                    json.dump(last_version, f, indent = 4)
                    return
        # If the file does not exist or is malformated:
        with open(last_version_file, "w") as f:
            new_version = {
                "processing_date": VersionManager._TODAY,
                "previous_date": VersionManager._TODAY,
                "results_count": len(new_version),
                "results": new_version["results"]
            }
            json.dump(new_version, f, indent = 4)


    def _get_data_path(file: str,
                       list_name: str) -> str:
        """
        Get the data folder's path from the url.
        The data folder is located at the root of the project: /data

        Keyword arguments:
        file -- the name of the file to access.
        list_name -- used to access the right folder of the data folder.
        """
        return str(DATA_DIR / list_name / file)


    def compare_versions(new_data: dict,
                         extractor: Extractor):
        """
        Load, compare and save data dict for version management.
        Add a Deprecated relation on removed entities.
        Add to_merge True to new entities.
        Add a latest modification date on new entities, old entities
        and modified entities.
        Update content of modified entities.

        Keyword arguments:
        new_data -- the newly extracted data dictionary
        extractor -- the extractor used to extract the new_data dict
        """
        # First download
        filename = VersionManager.VERSION_MANAGER / (extractor.NAMESPACE + ".pkl")
        old_data = None
        if os.path.exists(filename):
            try:
                with open(filename, 'rb') as file:
                    old_data = pickle.load(file)
            except:
                logging.error("Error while decoding", filename)
                print(f"Error while decoding {filename}")
                old_data = None

        if not old_data:
            for value in new_data.values():
                value["modified"] = VersionManager._TODAY
        else:
            deleted = old_data.keys() - new_data.keys()
            added = new_data.keys() - old_data.keys()
            updated = set(new_data.keys()).intersection(old_data.keys())
            for uri in added:
                new_data[uri]["modified"] = VersionManager._TODAY
            for uri in deleted:
                new_data[uri] = old_data[uri]
                new_data[uri]["deprecated"] = ":__"
                # TODO try to use ivoasem:useInstead <#ICRS>
            # Check for updated content
            for uri in updated:
                for key in set(new_data[uri].keys()).union(old_data[uri].keys()) - {"modified"}:
                    if (key != "modified" and (key not in old_data[uri] or
                        key not in new_data[uri] or
                        old_data[uri][key] != new_data[uri][key])):
                        new_data[uri]["modified"] = VersionManager._TODAY
                        break # modified
        with open(filename, 'wb') as file:
            pickle.dump(new_data, file) # Replace version

if __name__ == "__main__":
    pass
