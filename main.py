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

from connector import Connector
from purpleair import PurpleAirUploader
from valero import ValeroUploader
from fenceline_martinez import FencelineMartinezUploader
from fenceline_rodeo import FencelineRodeoUploader


class PurpleAirConnector(Connector):
	UPLOADER = PurpleAirUploader
	PRODUCT_NAME = 'AWBA_PurpleAir'
	BAY_AREA_PURPLE_AIR = {
		3765, 20187,
		11988, 11990,
		20207, 23933,
		3939, 23933,
		22451, 20053,
	}

	def scrape(self):
		bay_area_purple_air_devices = iter(self.uploader.get_purple_air_device(device_id) for device_id in self.BAY_AREA_PURPLE_AIR)
		for devices in bay_area_purple_air_devices:
			for device in devices:
				feed_data = self.uploader.parse_device(device)
				if feed_data:
					yield feed_data
				# Uncomment to upload all data between two dates.
				# yield self.uploader.upload_thingspeak_data(device, datetime(2019, 5, 1), datetime(2019, 5, 3))

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
		yield self.uploader.fetch_current_data()

app = webapp2.WSGIApplication([
	('/purpleair', PurpleAirConnector),
	('/valero', ValeroConnector),
	('/fencelinerodeo', FencelineRodeoConnector),
	('/fencelinemartinez', FencelineMartinezConnector),
], debug=True)