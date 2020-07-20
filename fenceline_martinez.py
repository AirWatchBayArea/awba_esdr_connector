import json, logging, requests, re

from collections import namedtuple, deque
from datetime import datetime, timedelta
from requests_toolbelt.adapters import appengine
from bs4 import BeautifulSoup

from uploader import Uploader

Feed = namedtuple('Feed', ['id', 'name', 'lat', 'lon'])
appengine.monkeypatch()

BASE_URL = "http://www.fenceline.org/martinez/grapher.php?t=2&g=7&dt=0"

WHITE_SPACE_PATTERN = re.compile(r'(\s)+')
PUNCTUATION_PATTERN = re.compile(r'[.!?\-,]')

def get_request_headers():
    return {
        'Origin': 'http://www.fenceline.org/martinez/grapher.php?t=2&g=7&dt=0',
        'Referer': 'http://www.fenceline.org/martinez',
    }

def get_request_url():
    return BASE_URL

def parse_data_from_row(raw):
	data = str(PUNCTUATION_PATTERN.sub('_', 
		       WHITE_SPACE_PATTERN.sub('_', raw.strip())))
	try:
		data = int(data)
	except ValueError:
		pass
	return data

class FencelineMartinezUploader(Uploader):
	def fetch_current_data(self):
		html = requests.get(
					get_request_url(),
					headers=get_request_headers()).content
		soup = BeautifulSoup(html, 'html.parser')
		import pdb; pdb.set_trace()
		ftir_table = soup(id='table-ftir')[0]
		uv_table = soup(id='table-uv')[0]

		