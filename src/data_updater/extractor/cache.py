
"""
Cache manager to prevent requesting websites many times and keep
operational local versions of the web pages. This could also be achieved using
https://web.archive.org/web/date/url instead of the actual page.
The cache is saved locally in the /cache folder. In order to actualize the
pages' content, change the cache's file name or remove it.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

from pathlib import Path
import glob
import re
import requests
import logging
import os
import subprocess
import json
import datetime

# Set up the basic configuration for logging
LOG = Path(__file__).parent.parent.parent.parent / 'cache' / 'error.log'# "../../cache/error.log"
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

    CACHE = str(Path(__file__).parent.parent.parent.parent / 'cache') + '/' # "../../cache/"

    def get_page(url: str,
                 list_name: str,
                 from_cache: bool = True) -> str:
        """
        Get a page from cache if it is saved in cache, else scrap it online.

        Keyword arguments:
        url -- the URL of the page.
        list_name -- used to access the right folder of the cache.
        from_cache -- whether to get content from cache if it exists.
        """
        cache_path = CacheManager._get_cache_path(url, list_name)
        content = ""
        if from_cache and glob.glob(cache_path):
            with open(cache_path, 'r') as file:
                content = file.read()
        if not content:
            content = CacheManager.scrap(url)
            if content:
                CacheManager.save_cache(content, cache_path)
        return content


    def _get_cache_path(url: str,
                        list_name: str) -> str:
        """
        Get the cache's path from the url.
        The cache folder is located at the root of the project: /cache

        Keyword arguments:
        url -- the URL of the page.
        list_name -- used to access the right folder of the cache.
        """
        # Create folder CACHE
        Path(CacheManager.CACHE + list_name).mkdir(parents = True,
                                                   exist_ok = True)

        # Create folder list_name
        cache_path = re.sub(r"[^\w\d]+", "_", url)
        if cache_path[-1] == '_':
            cache_path = cache_path[:-1]
        cache_path = cache_path.lower()
        cache_path = CacheManager.CACHE + list_name + cache_path + ".html"
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


    def scrap(url: str) -> str:
        """
        Scrap the web page's content. Encode it using the response's charset.

        Keyword arguments:
        url -- the url to scrap from.
        """
        try:
            response = requests.get(
                    url,
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
        if url.endswith('/'):
            url = url[:-1]
        url += ".git"
        git_repo_folder = CacheManager.CACHE + list_name + git_repo
        if glob.glob(git_repo_folder):
            command = ["git", "pull"]
            cwd = git_repo_folder
        else:
            command = ["git", "clone", url]
            cwd = CacheManager.CACHE + list_name
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

class VersionManager():
    """
    A class that saves version dictionaries and
    contains utility functions to check for updates.
    """

    DATA = str(Path(__file__).parent.parent.parent.parent / 'data') + '/' # "../../data/"

    _TODAY = datetime.datetime.now(tz=datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    def get_newer_keys(last_version_file: str,
                       new_version: dict,
                       list_name: str) -> set:
        """
        Get a list of keys which are newer compared to the last version date
        of the version file and necessitate to be refreshed.

        Keyword arguments:
        last_version_file -- the json file containing last versions for each uri
        new_version -- the dict with newer versions
        list_name -- used to access the right folder of the cache.
        """
        result = set()

        last_version_file = VersionManager.DATA + list_name + last_version_file

        if not os.path.exists(last_version_file):
            # We create the path to save the versions from the
            # newer version.
            return new_version.keys()
        else:
            with open(last_version_file, "r") as f:
                last_version = json.load(f)

        for uri in new_version["results"]:
            if uri not in last_version["results"]:
                result.add(uri)
            elif (last_version["results"][uri]["modified_date"]
                 < new_version["results"][uri]["modified_date"]):
                result.add(uri)
        return result


    def refresh(last_version_file: str,
                new_version: dict,
                list_name: str):
        """
        Replace URIs that are in newer_version dict into last_version_file.

        Keyword arguments:
        last_version_file -- the json file containing last versions for each uri
        new_version -- a dict of {uri: {last-modified, name}}
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
        return VersionManager.DATA + list_name + file


if __name__ == "__main__":
    pass