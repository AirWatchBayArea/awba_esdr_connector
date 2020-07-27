import json, logging, requests, re, os, pytz

from collections import namedtuple, deque
from datetime import datetime, timedelta
from requests_toolbelt.adapters import appengine
from bs4 import BeautifulSoup

from uploader import Uploader

Feed = namedtuple('Feed', ['id', 'name', 'lat', 'lon'])
appengine.monkeypatch()

BASE_URL = 'http://www.fenceline.org/martinez'

NUMBER_PATTERN = re.compile(r'([0-9]+(?:\.[0-9]+)?)')
WHITE_SPACE_PATTERN = re.compile(r'(\s)+')
PUNCTUATION_PATTERN = re.compile(r'[.!?\-,]')

FEED_ID_PREFIX = 'martinez_fenceline'
FEED_NAME_FORMAT = 'Martinez fenceline_org %s'

class FencelineMartinezUploader(Uploader):
	def __init__(self, esdr, product):
		super(FencelineMartinezUploader, self).__init__(esdr, product)
		self.site_feed_map = {
			'Site A': Feed(
				self.makeId(FEED_ID_PREFIX, lat=38.0220028, lon=-122.1283278),
				FEED_NAME_FORMAT % 'Site A',
				38.0220028,
				-122.1283278
			),
			'Site B': Feed(
				self.makeId(FEED_ID_PREFIX, lat=38.0197694, lon=-122.1294889),
				FEED_NAME_FORMAT % 'Site B',
				38.0197694,
				-122.1294889
			),
			'Site C': Feed(
				self.makeId(FEED_ID_PREFIX, lat=38.0164611, lon=-122.12645),
				FEED_NAME_FORMAT % 'Site C',
				38.0164611,
				-122.12645
			),
			'Site D': Feed(
				self.makeId(FEED_ID_PREFIX, lat=38.0150056, lon=-122.121425),
				FEED_NAME_FORMAT % 'Site D',
				38.0150056,
				-122.121425
			),
			'Site F': Feed(
				self.makeId(FEED_ID_PREFIX, lat=38.0131667, lon=-122.1064),
				FEED_NAME_FORMAT % 'Site F',
				38.0131667,
				-122.1064
			),
			'Site G': Feed(
				self.makeId(FEED_ID_PREFIX, lat=38.013525, lon=-122.0971111),
				FEED_NAME_FORMAT % 'Site G',
				38.013525,
				-122.0971111
			),
			'Path #1' : Feed(
				self.makeId(FEED_ID_PREFIX, lat=38.0157333, lon=-122.1239374),
				FEED_NAME_FORMAT % 'Path #1',
				38.0157333,
				-122.1239374
			),
			'Path #2' : Feed(
				self.makeId(FEED_ID_PREFIX, lat=38.0136778, lon=-122.1109576),
				FEED_NAME_FORMAT % 'Path #2',
				38.0136778,
				-122.1109576
			),
			'Path #3' : Feed(
				self.makeId(FEED_ID_PREFIX, lat=38.0133496, lon=-122.1017286),
				FEED_NAME_FORMAT % 'Path #3',
				38.0133496,
				-122.1017286
			),
			'Path #4' : Feed(
				self.makeId(FEED_ID_PREFIX, lat=38.0153314, lon=-122.0963823),
				FEED_NAME_FORMAT % 'Path #4',
				38.0153314,
				-122.0963823
			),
			'Wind': Feed(
				self.makeId(FEED_ID_PREFIX, lat=38.0143833, lon=-122.1037333),
				FEED_NAME_FORMAT % 'Wind',
				38.0143833,
				-122.1037333
			),
		}

	def get_now(self):
		timezone = pytz.timezone('America/Los_Angeles')
		aware = datetime.now(tz=timezone)
		return (aware - datetime(1970, 1, 1, tzinfo=pytz.utc)).total_seconds()

	def get_request_headers(self):
		return {
			'Origin': 'http://www.fenceline.org/martinez',
			'Referer': 'http://www.fenceline.org/martinez',
		}

	def get_request_url(self):
		return BASE_URL

	def get_table_row(self, table):
		rows = table.find('tbody').find_all('tr')
		for row in rows:
			values = [el.get_text() for el in row.find_all('td')]
			yield [self.parse_data(value) for value in values]

	def parse_data(self, raw):
		found_number = re.search(NUMBER_PATTERN, raw)
		if found_number:
			data = found_number.groups(0)[0]
		else:
			data = str(PUNCTUATION_PATTERN.sub('_', 
					   WHITE_SPACE_PATTERN.sub('_', raw.strip())))
			if 'No_Detection' in data:
				return None
		try:
			data = int(data)
		except ValueError:
			pass
		try:
			data = float(data)
		except ValueError:
			pass
		return data

	def get_sites(self, table):
		return [
			el.get_text().strip() for el in table.find_all('th')
		]

	def get_wind_deg(self, compass_code):
		compass_code_map = {
			'NNE': 22.5,
			'NE': 45.0,
			'ENE': 67.5,
			'E': 90.0,
			'ESE': 112.5,
			'SE': 135.0,
			'SSE': 157.5,
			'S': 180.0,
			'SSW': 202.5,
			'SW': 225.0,
			'WSW': 247.5,
			'W': 270.0,
			'WNW': 292.5,
			'NW': 315.0,
			'NNW': 337.5,
			'N': 360.0,
		}
		return compass_code_map.get(compass_code, None)

	def get_row_data(self, table):
		sites = self.get_sites(table)
		data = iter(zip(sites, row) for row in self.get_table_row(table))
		for row in data:
			row = iter(row)
			_, chemical = next(row)
			for site, value in row:
				yield site, chemical, value

	def fetch_current_data(self):
		html = requests.get(
					self.get_request_url(),
					headers=self.get_request_headers()).content
		soup = BeautifulSoup(html, 'html.parser')
		date_time = int(self.get_now())

		# Fetch wind data.
		wind_speed = self.parse_data(soup(text=re.compile(r'\s*Wind\sSpeed:\s*'))[0].parent()[0].get_text())
		wind_dir = self.get_wind_deg(soup(text=re.compile(r'\s*Wind\sBlowing\sFrom:\s*'))[0].parent()[0].get_text())

		feed_data = {}
		for table in (soup(id='table-ogd')[0],
					  soup(id='table-ftir')[0],
					  soup(id='table-uv')[0]):
			for site, chemical, value in self.get_row_data(table):
				feed_data.setdefault(self.site_feed_map[site], {})[chemical] = value
		feed_data[self.site_feed_map['Wind']] = {
			'Wind_Speed_MPH': wind_speed,
			'Wind_Direction_degrees': wind_dir,
		}
		for feed, data in feed_data.iteritems():
			data['time'] = date_time
			yield self.getFeed(*feed), self.makeEsdrUpload(data), data
		