import webapp2, json, logging, os, random, time

from esdr import Esdr
from collections import OrderedDict

def fuzzy_wait(secs):
     def decorator(func):
         def wrapper(*args, **kwargs):
             time.sleep(random.randint(0, secs))
             return func(*args, **kwargs)
         return wrapper
     return decorator

class Connector(webapp2.RequestHandler):
	UPLOADER = object
	PRODUCT_NAME = 'ESDR Product'

	@fuzzy_wait(10)
	def get(self):
		self.response.headers['Content-Type'] = 'application/json; charset=utf-8'
		self.initialize_connector()
		try:
			response = []
			for data in self.scrape():
				if data:
					feed, esdr_data, raw_data = data
					self.upload(feed, esdr_data)
					response.append(OrderedDict({'feed':  '%s (%s)' % (feed['name'], feed['id']), 'esdr_data': esdr_data, 'raw_data': raw_data}));
			self.response.write(json.dumps(response))
		except Exception as e:
			logging.error(e, exc_info=True)
			self.response.write(json.dumps({'error': str(e)}))

	def initialize_connector(self):
		setattr(self, 'esdr', Esdr('awba_auth/auth.json'))
		product = self.esdr.get_or_create_product(self.PRODUCT_NAME)
		setattr(self, 'uploader', self.UPLOADER(self.esdr, product))

	def scrape(self):
		raise NotImplementedError()

	@fuzzy_wait(3)
	def upload(self, feed, data):
		logging.info('Uploading to %s (%s)' % (feed['id'], feed['name']))
		if os.getenv('SERVER_SOFTWARE', '').startswith('Google App Engine/') and self.request.headers.get('X-AppEngine-Cron'):
		  # Production and App Engine cron job:
		  self.esdr.upload(feed, data)
		  logging.info('Uploaded to %s (%s)' % (feed['id'], feed['name']))
		else:
		  logging.info('... Skipped upload in dev mode.')