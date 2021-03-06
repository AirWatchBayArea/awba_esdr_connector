# Copyright 2016 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import webapp2

from datetime import datetime

from chevron import ChevronUploader
from connector import Connector
from fenceline_martinez import FencelineMartinezUploader
from fenceline_rodeo import FencelineRodeoUploader
from purpleair import PurpleAirUploader
from valero import ValeroUploader

class PurpleAirConnector(Connector):
	UPLOADER = PurpleAirUploader
	PRODUCT_NAME = 'AWBA_PurpleAir'
	BAY_AREA_PURPLE_AIR = set()

	def scrape(self):
		bay_area_purple_air_devices = iter(self.uploader.get_purple_air_device(device_id) for device_id in self.BAY_AREA_PURPLE_AIR)
		for devices in bay_area_purple_air_devices:
			for device in devices:
				feed_data = self.uploader.parse_device(device)
				if feed_data:
					yield feed_data
				# Uncomment to upload all data between two dates.
				# yield self.uploader.upload_thingspeak_data(device, datetime(2019, 5, 1), datetime(2019, 5, 3))

# Split up Benicia and Vallejo end points for better parallelism.
class PurpleAirBeniciaConnector(PurpleAirConnector):
	BAY_AREA_PURPLE_AIR = {
		  3765, 3939,
		  3964, 11988,
		  11990, 20187,
		  22451, 23933,
		  27237, 38503,
		  39655, 39665,
		  39667, 39717,
		  39745, 43787,
		  45905, 54335,
		  65217, 69607,
		  71763, 79147,
		  92121
	}

class PurpleAirVallejoConnector(PurpleAirConnector):
	BAY_AREA_PURPLE_AIR = {
		4491, 38429,
		6578, 23597,
		5127, 1870,
		2480,
	}

class ValeroConnector(Connector):
	UPLOADER = ValeroUploader
	PRODUCT_NAME = 'AWBA_Valero'

	def scrape(self):
		devices = self.uploader.fetch_devices()
		for feed_data in self.uploader.parse_devices(devices):
			if feed_data:
				yield feed_data
		devices = self.uploader.fetch_wind_devices()
		for feed_data in self.uploader.parse_wind_devices(devices):
			if feed_data:
				yield feed_data

class ChevronConnector(Connector):
	UPLOADER = ChevronUploader
	PRODUCT_NAME = 'AWBA_Chevron'

	def scrape(self):
		devices = self.uploader.fetch_devices()
		for feed_data in self.uploader.parse_devices(devices):
			if feed_data:
				yield feed_data
		devices = self.uploader.fetch_wind_devices()
		for feed_data in self.uploader.parse_wind_devices(devices):
			if feed_data:
				yield feed_data

class FencelineRodeoConnector(Connector):
	UPLOADER = FencelineRodeoUploader
	PRODUCT_NAME = 'AWBA_FencelineRodeo'

	def scrape(self):
		for data in self.uploader.fetch_current_data():
			if data:
				yield data

class FencelineMartinezConnector(Connector):
	UPLOADER = FencelineMartinezUploader
	PRODUCT_NAME = 'AWBA_FencelineMartinez'

	def scrape(self):
		for data in self.uploader.fetch_current_data():
			if data:
				yield data

app = webapp2.WSGIApplication([
	('/chevron', ChevronConnector),
	('/fenceline/martinez', FencelineMartinezConnector),
	('/fenceline/rodeo', FencelineRodeoConnector),
	('/purpleair', PurpleAirConnector),
	('/purpleair/benicia', PurpleAirBeniciaConnector),
	('/purpleair/vallejo', PurpleAirVallejoConnector),
	('/valero', ValeroConnector),
], debug=True)