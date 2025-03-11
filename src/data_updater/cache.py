
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
import re
import requests

class CacheManager():

    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
        AppleWebKit/537.36 (KHTML, like Gecko) \
        Chrome/58.0.3029.110 \
        Safari/537.3'}

    CACHE = "../../cache/"


    def get_page(self,
                 url: str) -> str:
        """
        Get a page from cache if it is saved in cache, else scrap it online.

        Keyword arguments:
        url -- the URL of the page.
        """
        cache_path = self.get_cache_path(url)
        content = ""
        if glob.glob(cache_path):
            with open(cache_path, 'r') as file:
                content = file.read()
        if not content:
            content = self.scrap(url)
            self.save_cache(content, cache_path)
        if not content:
            raise("No page content for:", url)
        return content


    def get_cache_path(self,
                       url: str) -> str:
        """
        Get the cache's path from the url.
        The cache folder is located at the root of the project: /cache

        Keyword arguments:
        url -- the URL of the page.
        """
        cache_path = re.sub(r"[^\w\d]+", "_", url)
        if cache_path[-1] == '_':
            cache_path = cache_path[:-1]
        cache_path = CACHE + cache_path + ".html"
        cache_path = cache_path.lower()
        return cache_path

    def save_cache(self,
                   content: str,
                   cache_path: str) -> None:
        """
        Save the page in the cache folder.

        Keyword arguments:
        content -- the content of the page.
        cache_path -- where to save the cache.
        """
        with open(cache_path, 'w') as file:
            file.write(content)

    def scrap(self,
              url: str) -> dict:
            try:
                response = requests.get(
                        url,
                        headers = HEADERS)
            except requests.exceptions.RequestException as e:
                raise SystemExit(e)

            if response.ok:
                return response.text
            else:
                print(f"Request to {url} failed with status code {response.status_code}")
                return None

if __name__ == "__main__":
    pass