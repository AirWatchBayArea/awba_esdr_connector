class Uploader(object):

	def __init__(self, esdr, product):
		self.esdr = esdr
		self.product = product
		self.feedCache = {}

	def getFeed(self, id, name, lat, lon):
		if id in self.feedCache:
			return self.feedCache[id]
		device = self.esdr.get_device_by_serial_number(self.product, id)
		if not device:
			self.esdr.create_device(self.product, id, name=name)
			device = self.esdr.get_device_by_serial_number(self.product, id)

		feed = self.esdr.get_feed(device)
		if not feed:
			self.esdr.create_feed(device, lat=lat, lon=lon)
			feed = self.esdr.get_feed(device)
		self.feedCache[id] = feed
		return feed

	def makeId(self, deviceId, lat, lon):
		id = '%s_%06d%s%06d%s' % (deviceId, round(1000 * abs(lat)), 'NS'[lat < 0], round(1000 * abs(lon)), 'EW'[lon < 0])
		return id.replace('.','_')

	def makeEsdrUpload(self, data):
		# Move time to the beginning since that's how ESDR likes it
		keys = ['time'] + sorted(set(data.keys()).difference(set(['time'])))
		values = [data[key] for key in keys]

		# TIME is implicit for ESDR;  don't list in channel_names
		return {'channel_names':keys[1:], 'data':[values]}