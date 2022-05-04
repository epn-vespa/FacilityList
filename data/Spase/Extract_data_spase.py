import httplib2
import requests
from bs4 import BeautifulSoup, SoupStrainer
import urllib
import json
from lxml import html

spase_url = 'https://heliophysicsdata.gsfc.nasa.gov/websearch/dispatcher'