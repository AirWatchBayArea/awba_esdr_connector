import json, logging, requests, re

from collections import namedtuple, deque
from datetime import datetime, timedelta
from requests_toolbelt.adapters import appengine
from bs4 import BeautifulSoup

from uploader import Uploader

Feed = namedtuple('Feed', ['id', 'name', 'lat', 'lon'])
appengine.monkeypatch()

BASE_URL = "http://www.fenceline.org/martinez"

def get_request_headers():
    return {
        'Origin': 'http://www.fenceline.org/martinez',
        'Referer': 'http://www.fenceline.org/martinez',
    }

def get_request_url():
    return BASE_URL

class FencelineMartinezUploader(Uploader):
	def fetch_current_data(self):
		html = requests.get(
					get_request_url(),
					headers=get_request_headers()).content
		soup = BeautifulSoup(html, 'html.parser')
		