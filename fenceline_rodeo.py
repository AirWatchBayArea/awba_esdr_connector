import json, logging, requests, re

from collections import namedtuple, deque
from datetime import datetime, timedelta
from requests_toolbelt.adapters import appengine
from bs4 import BeautifulSoup

from uploader import Uploader

Feed = namedtuple('Feed', ['id', 'name', 'lat', 'lon'])
appengine.monkeypatch()

BASE_URL = "http://www.fenceline.org/rodeo/data.php"

NORTH_LAT_LON = ()
SOUTH_LAT_LON = ()

WHITE_SPACE_PATTERN = re.compile(r'(\s)+')
PUNCTUATION_PATTERN = re.compile(r'[.!?\-,]')

chemical_id_map = {
	'1_3_Butadiene': 'butd_%s_f',
	'Carbonyl_Sulfide': 'cs_%s_f',
	'Total_Hydrocarbons': 'but_%s_f',
	'Carbon_Monoxide': 'co_%s_f',
	'Ethanol': 'etho_%s_f',
	'Ethylene': 'ethy_%s_f',
	'Nitrous_Oxide': 'nto_%s_f',
	'Ammonia': 'nh3_%s_f',
	'Mercaptan': 'mer_%s_f',
	'Methane	': 'meth_%s_f',
	'MTBE': 'mtbe_%s_f',
	'Benzene': 'ben_%s_u',
	'Carbon_Disulfide': 'cs2_%s_u',
	'Ozone': 'o3_%s_u',
	'Sulfur_Dioxide': 'so2_%s_u',
	'Toluene': 'tol_%s_u',
	'Xylene': 'xyl_%s_u',
}

def get_request_headers():
    return {
        'Origin': 'http://www.fenceline.org/rodeo/data.php',
        'Referer': 'http://www.fenceline.org/rodeo',
    }

def get_request_url():
    return BASE_URL

def get_row_data(element, row_id):
	return [parse_data_from_row(el.get_text()) for el in element.find(text=re.compile(r'\s*'+ row_id + r'\s*')).find_parent('tr').find_all('td')]

def parse_data_from_row(raw):
	data = str(PUNCTUATION_PATTERN.sub('_', 
		       WHITE_SPACE_PATTERN.sub('_', raw.strip())))
	try:
		data = int(data)
	except ValueError:
		pass
	return data

class FencelineRodeoUploader(Uploader):
	def fetch_current_data(self):
		html = requests.get(
					get_request_url(),
					headers=get_request_headers()).content
		soup = BeautifulSoup(html, 'html.parser')
		ftir_table = soup(text=re.compile(r'\s*FTIR\sSystems\s*'))[0].find_parent('table')
		ftir_date = get_row_data(ftir_table, 'Date')
		ftir_time = get_row_data(ftir_table, 'Time')
		uv_table = soup(text=re.compile(r'\s*UV\sSystems\s*'))[0].find_parent('table')
		uv_date = get_row_data(uv_table, 'Date')
		uv_time = get_row_data(uv_table, 'Time')
		north_data = {
			key: parse_data_from_row(soup.select('#' + (value % 'n'))[0].get_text()) for key, value in chemical_id_map.iteritems()
		}
		south_data = {
			key: parse_data_from_row(soup.select('#' + (value % 's'))[0].get_text()) for key, value in chemical_id_map.iteritems()
		}
		print(north_data, south_data)

		
