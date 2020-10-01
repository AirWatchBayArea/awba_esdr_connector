import json, os, random, re, requests, unicodedata, urllib, sys
import logging
from requests_toolbelt.adapters import appengine

appengine.monkeypatch()

# TODO: Getting the refresh_token should happen online with an OAuth dialog, and the refresh token
# and client secret should be stored here instead of the username and password.
# Or we should have a onetime username/password dialog in Python, and store same.

# ~/.config/esdr/auth.json should look like:
# {
#    "grant_type" : "password",
#    "client_id" : <client id>,
#    "client_secret" : <client secret>,
#    "username" : <email address>,
#    "password" : <actual password>
# }

class Esdr:
    def __init__(self, auth_file, prefix='https://esdr.cmucreatelab.org', user_agent='esdr-library.py'):
        self.prefix = prefix
        self.auth_file = auth_file
        self.tokens = None
        self.user_agent = user_agent

    @staticmethod
    def save_client(destination, display_name, username='EDIT ME', password='EDIT ME'):
        client_secret = '%032x' % random.randrange(16**32)
        client_name = re.sub('\W+', '_', display_name)

        data = {'displayName' : display_name,
                'clientName' :  client_name,
                'clientSecret' : client_secret}

        auth = {
            'grant_type': 'password',
            'client_id': data['clientName'],
            'client_secret': client_secret,
            'username': username,
            'password': password
        }
        
        open(destination, 'w').write(json.dumps(auth))
        logging.info('Wrote %s to %s' % (json.dumps(auth), destination))
        
        logging.info('Please create a client here: https://esdr.cmucreatelab.org/home/clients')
        logging.info('with these parameters:')
        logging.info('Display Name: %s' % display_name)
        logging.info('Client ID: %s' % client_name)
        logging.info('Client Secret: %s' % client_secret)
        
        if username == 'EDIT ME' or password == 'EDIT ME':
            logging.info()
            logging.info('and edit usernamd and password in %s' % destination)
        
        logging.info()
        logging.info('Instantiate ESDR client like so:')
        logging.info("esdr = Esdr('%s')" % destination)
    
    @staticmethod
    # Replace sequences of non-word characters with '_'
    def make_identifier(name):
         name = ''.join(c for c in unicodedata.normalize('NFD', unicode(name))
                      if unicodedata.category(c) != 'Mn')
         return re.sub(r'\W+', '_', name).strip('_')

    def api(self, http_type, path, json_data=None, oauth=True):
        kwargs = {
            'headers': {
                'User-Agent': self.user_agent
            },
            'timeout': 4 * 60
        }
        
        if json_data:
            if http_type == 'GET':
                # get params
                kwargs['params'] = json_data
            else:
                # post JSON directly
                kwargs['json'] = json_data

        if oauth:
            kwargs['headers']['Authorization'] = 'Bearer %s' % self.get_access_token()

        url = self.prefix + path

        fn = {'GET':requests.get, 'POST':requests.post, 'PUT':requests.put}[http_type]
        
        maxRetries = 5
        for attempt in range(1, maxRetries+1):
            try:
                r = fn(url, **kwargs)
                if attempt > 1:
                    logging.info('ESDR.api: Attempt %d succeeded' % attempt)
                break
            except (requests.Timeout, requests.ConnectionError):
                logging.info('ESDR.api: Timeout during attempt %d.' % attempt)
                if attempt == maxRetries:
                    logging.info('ESDR.api: No more retries, raising exception')
                    raise
                else:
                    logging.info('ESDR.api: Retrying.')
        
        r.raise_for_status()
        return r.json()

    def get_access_token(self):
        if not self.tokens:
            self.get_tokens()
        return self.tokens['access_token']
    
    def get_tokens(self):
        try:
            auth = json.load(open(self.auth_file))
        except Exception as e:
            raise Exception('While trying to read authorization file %s, %s' % (self.auth_file, e), sys.exc_info()[2])
        self.tokens = self.api('POST',
                               '/oauth/token',
                               json.load(open(self.auth_file)),
                               oauth=False)

    def query(self, path, args):
        response = self.api('GET', path, args)
        return response['data']['rows']
    
    def query_first(self, path, args):
        rows = self.query(path, args)
        if len(rows) == 0:
            return None
        else:
            return rows[0];

    def get_or_create_product(self, prettyName, vendor=None, description=None, default_channel_specs={}):
        name = re.sub('\W+', '_', prettyName)
        if not vendor:
            vendor = name
        if not description:
            description = prettyName
        product = self.get_product_by_name(name)
        if not product:
            self.create_product(name, prettyName, vendor, description, default_channel_specs)
            product = self.get_product_by_name(name)
        return product
        
    def create_product(self, name, pretty_name, vendor, description, default_channel_specs={}):
        return self.api('POST', '/api/v1/products', {
            'name': name,
            'prettyName': pretty_name,
            'vendor': vendor,
            'description': description,
            'defaultChannelSpecs': default_channel_specs
        })
    
    def get_product_by_name(self, name):
        return self.query_first('/api/v1/products', {'where':'name=%s' % name})

    def get_product_by_id(self, id):
        return self.query_first('/api/v1/products', {'where':'id=%d' % id})
            
    def get_or_create_device(self, product, serial_number, name=None):
        device = self.get_device_by_serial_number(product, serial_number)
        if not device:
            self.create_device(product, serial_number, name=name)
            device = self.get_device_by_serial_number(product, serial_number)
        return device
    
    def get_device_by_serial_number(self, product, serial_number):
        response = self.api('GET', '/api/v1/devices', {'whereAnd': 'productId=%d,serialNumber=%s' % (product['id'], serial_number)})
        if response['data']['totalCount'] == 0:
            return None
        elif response['data']['totalCount'] == 1:
            return response['data']['rows'][0]
        else:
            raise Exception('get_device_by_serial_number: found more than one device?')
        
    def create_device(self, product, serial_number, name=None):
        if name == None:
            name = serial_number
        logging.info('Creating device serialNumber %s, name %s' % (serial_number, name))
        device = self.api('POST',
                          '/api/v1/products/%d/devices' % product['id'], 
                          {
                              'name':name,
                              'serialNumber':serial_number
                          })['data']
        return device
    
    def get_or_create_feed(self, device, lat=None, lon=None):
        feed = self.get_feed(device, lat=lat, lon=lon)
        if not feed:
            self.create_feed(device, lat=lat, lon=lon)
            feed = self.get_feed(device)
        return feed
    
    def get_feed(self, device, lat=None, lon=None):
        rows = self.query('/api/v1/feeds', {'where':'deviceId=%d' % device['id']})
        #if a device has been moved and thus has multiple feeds, return the feed corresponding to the passed-in location
        if lat and lon:
            for row in rows:
                if row['latitude'] == lat and row['longitude'] == lon:
                    return row
            return None 
        #null case
        elif len(rows) == 0:
            return None
        #if a device has multiple feeds but no location was passed in,
        #or a device has only one feed, return the first
        return rows[0]  
    
    def create_feed(self, device, lat=None, lon=None):
        product = self.get_product_by_id(device['productId'])
        name = (device['name'] + ' ' + product['name'])
        fields = {
                    'name': name,
                    'exposure':'outdoor',
                    'isPublic':1,
                    'isMobile':0
                 }
        if lat != None:
            fields['latitude'] = lat
        if lon != None:
            fields['longitude'] = lon
        logging.info('Creating feed %s' % fields)
        response = self.api('POST', '/api/v1/devices/%d/feeds' % device['id'], fields)
        return response
    
    # data is of form
    # {'channel_names': ['a', 'b', 'c'],
    #  'data': [[1417524480, 1, 1, 1],
    #           [1417524481, 2, 3, 4],
    #           [1417524482, 3, 5, None]]}
    # None translates into NULL, which is a no-op
    # False translates into false, which will delete a sample already present at that time

    def upload(self, feed, data):
        return self.api('PUT', '/api/v1/feeds/%s' % feed['id'], data)
    
    def get_tile_prefix(self, feed, channel_name):
        return self.prefix + '/api/v1/feeds/%d/channels/%s/tiles' % (feed['id'], channel_name)

    def get_or_create_feed_from_device_info(self, device_info):
        product = esdr.get_or_create_product(device_info['product'])

        device = esdr.get_device_by_serial_number(product, device_info['serialNumber'])
        if not device:
            esdr.create_device(product, device_info['serialNumber'])
            device = esdr.get_device_by_serial_number(product, device_info['serialNumber'])

        feed = esdr.get_feed(device)
        if not feed:
            lat = None
            lon = None
            if 'lat' in device_info:
                lat = device_info['lat']
                lon = device_info['lon']
            esdr.create_feed(device, lat=lat, lon=lon)
            feed = esdr.get_feed(device)
        return feed




#Create product:
#esdr.api('POST', '/api/v1/products', {
#   "name" : "ACHD",
#   "prettyName" : "ACHD",
#   "vendor" : "ACHD",
#   "description" : "A sensor operated by the Allegheny County Health Department (ACHD)",
#   "defaultChannelSpecs" : {}
#})

#esdr = Esdr()
#product = esdr.get_product_by_name('TestProduct')
#device = esdr.get_or_create_device(product, 'TestDevice')
#feed = esdr.get_or_create_feed(device)
#data = {'channel_names': ['a', 'b', 'c'],
#        'data': [[1417524480, 1, 1, 1],
#                 [1417524481, 2, 3, 4],
#                 [1417524482, 3, 5, 7]]}
#
#esdr.upload(feed, data)