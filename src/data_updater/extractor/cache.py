
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

    def get_page(url: str) -> str:
        """
        Get a page from cache if it is saved in cache, else scrap it online.

        Keyword arguments:
        url -- the URL of the page.
        """
        cache_path = CacheManager.get_cache_path(url)
        content = ""
        if glob.glob(cache_path):
            with open(cache_path, 'r') as file:
                content = file.read()
        if not content:
            content = CacheManager.scrap(url)
            if content:
                CacheManager.save_cache(content, cache_path)
        return content


    def get_cache_path(url: str) -> str:
        """
        Get the cache's path from the url.
        The cache folder is located at the root of the project: /cache

        Keyword arguments:
        url -- the URL of the page.
        """
        cache_path = re.sub(r"[^\w\d]+", "_", url)
        if cache_path[-1] == '_':
            cache_path = cache_path[:-1]
        cache_path = cache_path.lower()
        cache_path = CacheManager.CACHE + cache_path + ".html"
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
                 git_repo: str):
        """
        Pull a github repository and save it in the cache.
        The name of the created folder is git_repo.
        If the folder does not exist, do git clone instead.
        """
        if url.endswith('/'):
            url = url[:-1]
        url += ".git"
        git_repo_folder = CacheManager.CACHE + git_repo
        if glob.glob(git_repo_folder):
            command = ["git", "pull"]
        else:
            command = ["git", "clone", url]
        messages = subprocess.Popen(
            command,
            cwd=CacheManager.CACHE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        ).communicate()
        for item in messages:
            item_utf8 = item.decode("utf-8")
            if item_utf8 != "":
                logging.warning(item_utf8)


if __name__ == "__main__":
    pass
