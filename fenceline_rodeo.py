#!/usr/bin/python
# -*- coding: utf-8 -*-
import json, logging, requests, re, os, pytz

from collections import namedtuple, deque
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

from requests_toolbelt.adapters import appengine
appengine.monkeypatch()

from uploader import Uploader

Feed = namedtuple('Feed', ['id', 'name', 'lat', 'lon'])
BASE_URL = "http://www.fenceline.org/rodeo/data.php"

LatLon = namedtuple('LatLon', ['lat', 'lon'])
NORTH_LAT_LON = LatLon(38.044924, -122.247935)
SOUTH_LAT_LON = LatLon(38.03855, -122.25653)

NORTH_ID_PREFIX = 'rodeo_north'
NORTH_NAME = 'Rodeo North Fenceline fenceline_org'

SOUTH_ID_PREFIX = 'rodeo_south'
SOUTH_NAME = 'Rodeo South Fenceline fenceline_org'

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
	'Methane': 'meth_%s_f',
	'MTBE': 'mtbe_%s_f',
	'Benzene': 'ben_%s_u',
	'Carbon_Disulfide': 'cs2_%s_u',
	'Ozone': 'o3_%s_u',
	'Sulfur_Dioxide': 'so2_%s_u',
	'Toluene': 'tol_%s_u',
	'Xylene': 'xyl_%s_u',
	'Hydrogen_Sulfide': 'h2s_%s_t',
}

# Sets must be mutually exclusive with each other.
ftir = set([
	'1_3_Butadiene',
	'Carbonyl_Sulfide',
	'Total_Hydrocarbons',
	'Carbon_Monoxide',
	'Ethanol',
	'Ethylene',
	'Nitrous_Oxide',
	'Ammonia',
	'Mercaptan',
	'Methane',
	'MTBE',
])

uv = set([
	'Benzene',
	'Carbon_Disulfide',
	'Ozone',
	'Sulfur_Dioxide',
	'Toluene',
	'Xylene',
])

tdl = set(['Hydrogen_Sulfide'])

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

def make_time(date, time):
	# Example date: 2020_05_17
	# Example time: 15:14:23
	date_time_obj = datetime.strptime('{} {}'.format(date, time), '%Y_%m_%d %H:%M:%S')
	timezone = pytz.timezone('America/Los_Angeles')
	aware = timezone.localize(date_time_obj)
	return (aware - datetime(1970, 1, 1, tzinfo=pytz.utc)).total_seconds()


class FencelineRodeoUploader(Uploader):
	def fetch_current_data(self):
		html = requests.get(
					get_request_url(),
					headers=get_request_headers()).content
		soup = BeautifulSoup(html, 'html.parser')
		# Featch Weather Data (North Fenceline)
		weather_table = soup(text=re.compile(r'\s*Weather\sConditions\s*'))[0].find_parent('table')
		_, weather_date = get_row_data(weather_table, 'Date')
		_, weather_time = get_row_data(weather_table, 'Time')
		weather_datetime = make_time(weather_date, weather_time)
		temperature = parse_data_from_row(weather_table.find(id='temp').get_text())
		humidity = parse_data_from_row(weather_table.find(id='hum').get_text())
		dew_point = parse_data_from_row(weather_table.find(id='dew').get_text())
		wind_speed = parse_data_from_row(weather_table.find(id='wspeed').get_text())
		wind_direction_text = weather_table.find(id='wdir').get_text()
		wind_direction_search = re.search(r'([0-9]+)', wind_direction_text)
		wind_direction = parse_data_from_row(wind_direction_search.group(1)) if wind_direction_search else None
		north_weather_data = {
			'time': weather_datetime,
			'Temperature_F': temperature,
			'Humidity': humidity,
			'Dew_Point_F': dew_point,
			'Wind_Speed_MPH': wind_speed,
			'Wind_Direction_degrees': wind_direction
		}

		# Fetch FTIR Data
		ftir_table = soup(text=re.compile(r'\s*FTIR\sSystems\s*'))[0].find_parent('table')
		_, ftir_south_date, ftir_north_date = get_row_data(ftir_table, 'Date')
		_, ftir_south_time, ftir_north_time = get_row_data(ftir_table, 'Time')
		uv_table = soup(text=re.compile(r'\s*UV\sSystems\s*'))[0].find_parent('table')
		_, uv_south_date, uv_north_date = get_row_data(uv_table, 'Date')
		_, uv_south_time, uv_north_time = get_row_data(uv_table, 'Time')
		tdl_table = soup(text=re.compile(r'\s*TDL\sSystems\s*'))[0].find_parent('table')
		_, tdl_south_date, tdl_north_date = get_row_data(tdl_table, 'Date')
		_, tdl_south_time, tdl_north_time = get_row_data(tdl_table, 'Time')
		north_data = {
			key: parse_data_from_row(soup.select('#' + (value % 'n'))[0].get_text()) for key, value in chemical_id_map.iteritems()
		}
		south_data = {
			key: parse_data_from_row(soup.select('#' + (value % 's'))[0].get_text()) for key, value in chemical_id_map.iteritems()
		}

		# Upload North Fenceline data:
		north_id = self.makeId(NORTH_ID_PREFIX, NORTH_LAT_LON.lat, NORTH_LAT_LON.lon)
		north_feed = Feed(north_id, NORTH_NAME, NORTH_LAT_LON.lat, NORTH_LAT_LON.lon)
		
		# Upload Weather Data:
		if north_weather_data:
			yield self.getFeed(*north_feed), self.makeEsdrUpload(north_weather_data), north_weather_data
		
		# Upload FTIR North:
		ftir_north_data = {
			"FTIR_" + key: value for key, value in north_data.iteritems() if key in ftir and value != 'ND'
		}
		if ftir_north_data:
			ftir_north_data['time'] = make_time(ftir_north_date, ftir_north_time)
			yield self.getFeed(*north_feed), self.makeEsdrUpload(ftir_north_data), ftir_north_data

		# Upload FTIR North:
		uv_north_data = {
			"UV_" + key: value for key, value in north_data.iteritems() if key in uv and value != 'ND'
		}
		if uv_north_data:
			uv_north_data['time'] = make_time(uv_north_date, uv_north_time)
			yield self.getFeed(*north_feed), self.makeEsdrUpload(uv_north_data), uv_north_data

		# Upload TDL North:
		tdl_north_data = {
			"TDL_" + key: value for key, value in north_data.iteritems() if key in tdl and value != 'ND'
		}
		if tdl_north_data:
			tdl_north_data['time'] = make_time(tdl_north_date, tdl_north_time)
			yield self.getFeed(*north_feed), self.makeEsdrUpload(tdl_north_data), tdl_north_data

		# Upload South Fenceline data:
		south_id = self.makeId(SOUTH_ID_PREFIX, SOUTH_LAT_LON.lat, SOUTH_LAT_LON.lon)
		south_feed = Feed(south_id, SOUTH_NAME, SOUTH_LAT_LON.lat, SOUTH_LAT_LON.lon)
		
		# Upload FTIR South:
		ftir_south_data = {
			"FTIR_" + key: value for key, value in south_data.iteritems() if key in ftir and value != 'ND'
		}
		if ftir_south_data:
			ftir_south_data['time'] = make_time(ftir_south_date, ftir_south_time)
			yield self.getFeed(*north_feed), self.makeEsdrUpload(ftir_north_data), ftir_north_data

		# Upload UV South:
		uv_south_data = {
			"UV_" + key: value for key, value in south_data.iteritems() if key in uv and value != 'ND'
		}
		if uv_south_data:
			uv_south_data['time'] = make_time(uv_south_date, uv_south_time)
			yield self.getFeed(*south_feed), self.makeEsdrUpload(uv_south_data), uv_south_data

		# Upload TDL South:
		tdl_south_data = {
			"TDL_" + key: value for key, value in south_data.iteritems() if key in tdl and value != 'ND'
		}
		if tdl_south_data:
			tdl_south_data['time'] = make_time(tdl_south_date, tdl_south_time)
			yield self.getFeed(*south_feed), self.makeEsdrUpload(tdl_south_data), tdl_south_data
		
