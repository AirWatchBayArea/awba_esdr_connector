import json, logging, requests, os

from collections import namedtuple
from datetime import datetime, timedelta

from requests_toolbelt.adapters import appengine
appengine.monkeypatch()

from uploader import Uploader

Feed = namedtuple('Feed', ['id', 'name', 'lat', 'lon'])

AUTH = {
            "username": "cvrPublicApp",
            "password": "uK3PB9Q3WWn9Xr*&",
        }

BASE_URL = "https://insight2-data.sonomatech.com/api"

def log(func):
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        logging.info(result)
        return result
    return wrapper

def login():
    response = requests.post(
        BASE_URL + '/Auth/User/login',
        data=AUTH,
        headers=get_request_headers(),
        )
    response.raise_for_status()
    return response.json()

def handle_auth(func):
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            return result
        except:
            args[0].token = login();
            return func(*args, **kwargs)
    return wrapper

def get_request_headers():
    return {
        'Origin': 'https://richmondairmonitoring.org',
        'Referer': 'https://richmondairmonitoring.org/measurements.html',
    }

def get_request_url():
    return BASE_URL + "/data/filterAsJson"

def get_wind_request_url():
    return BASE_URL + "/WindData/filterAsJson"

def sanitize_date(date):
    return (date - timedelta(minutes=date.minute % 5,
                             seconds=date.second,
                             microseconds=date.microsecond)).strftime('%Y-%m-%dT%H:%M:%S')

def get_dates(ago=timedelta(seconds=0)):
    selected_date = datetime.utcnow() - ago;
    start_date = selected_date - timedelta(minutes=3)
    end_date = selected_date + timedelta(minutes=3)
    return tuple(sanitize_date(date) for date in [selected_date, start_date, end_date])

class ChevronUploader(Uploader):

    def __init__(self, esdr, product):
        super(ChevronUploader, self).__init__(esdr, product)
        self.token = login()

    def build_request_body(self, token, parameters):
        selected_date_time, start_date_time, end_date_time = get_dates()
        return {
            'input' : json.dumps({
                "dataStreams": [],
                "siteIds": [46,47,48, 145, 118,124,125],
                "parameters": parameters,
                "durationId": 2,
                "aggregateId": 0,
                "publicDataOnly": False,
                "primaryDataOnly": False,
                "validDataOnly": False,
                "selectedDateTime": selected_date_time,
                "startDateTime": start_date_time,
                "endDateTime": end_date_time,
                "isUtc": True,
            }),
            "token": token,
            "type" : "defaultJson",
            "fillMissingPoints": False,
        }

    def build_wind_request_body(self, token):
        # Must offset by 1 minute to get data.
        selected_date_time, start_date_time, end_date_time = get_dates(ago=timedelta(minutes=1))
        return {
            'input' : json.dumps(
                {
                    "dataStreams": [],
                    "siteIds": [46, 47, 48, 145],
                    "parameters": [289,24],
                    "durationId": 2,
                    "aggregateId": 0,
                    "pocs": [1],
                    "publicDataOnly": False,
                    "primaryDataOnly": False,
                    "validDataOnly": True,
                    "selectedDateTime": selected_date_time,
                    "endDateTime": end_date_time,
                    "startDateTime": start_date_time,
                    "isUtc": True,
                    "overwriteValue": 0,
                    "overwriteValueOpCodes": [74,76],
                    "isMulticolorSeries": True
                }),
            "token": token,
            "type" : "windDataJson",
        }

    @handle_auth
    def fetch_devices(self):
        params_list = [
            [2, 9, 40, 154, 155, 156, 158, 159, 176, 206, 209],
            [216, 279, 280, 281, 282, 283, 284, 285, 286, 287],
            [288, 302, 304, 310]
        ]
        data = []
        for params in params_list:
            response = requests.post(
                get_request_url(),
                data=self.build_request_body(self.token, params),
                headers=get_request_headers()
            )
            response.raise_for_status()
            if 'error' in response and response['error']:
                raise Exception(response['messages'][0])
            if 'isFailure' in response and response['isFailure']:
                raise Exception('The server responded with a failure.')
            data += response.json()['data']
        return data
        

    @handle_auth
    def fetch_wind_devices(self):
        response = requests.post(
            get_wind_request_url(),
            data=self.build_wind_request_body(self.token),
            headers=get_request_headers()
        )
        response.raise_for_status()
        if 'error' in response and response['error']:
            raise Exception(response['messages'][0])
        if 'isFailure' in response and response['isFailure']:
            raise Exception('The server responded with a failure.')
        return response.json()['windData']

    def parse_wind_devices(self, devices):
        raw_data_cache = {}
        feed_time_cache = {}
        for device in devices:
            try:
                lat = float(device['latitude'])
                lon = float(device['longitude'])
            except:
                # Ignore if no parsable lat and lon
                pass
            if device["qcCode"] == 9: # Invalid QC
                pass
            id = self.makeId(device['siteId'], lat, lon)
            name = device['siteName']
            feed = Feed(id, name, lat, lon)
            time = (datetime.strptime(device['utc'], "%Y-%m-%d %H:%M:%S") - datetime(1970,1,1)).total_seconds()
            raw_data_cache.setdefault((feed, time), []).append(device)
            data = feed_time_cache.setdefault((feed, time), {})
            data['time'] = time
            windSpeed = device['windSpeed']
            if device['unitName'] == 'm/s':
                data['Wind_Speed_MS'] = windSpeed
                # Convert to miles per hour.
                windSpeed *= 2.237;
                data['Wind_Speed_MPH'] = windSpeed
            elif device['unitName'] == 'mph':
                data['Wind_Speed_MPH'] = windSpeed
            data['Wind_Direction'] = device['windDirection']
        # Capture the max wind speed in a 5 minute sliding-window interval.
        feedtime, data = max(feed_time_cache.iteritems(), key=lambda x: x[1]['Wind_Speed_MPH'])
        feed, time = feedtime
        yield self.getFeed(*feed), self.makeEsdrUpload(data), raw_data_cache[(feed, time)]

    def parse_devices(self, devices):
        raw_data_cache = {}
        feed_time_cache = {}
        for device in devices:
            try:
                lat = float(device['latitude'])
                lon = float(device['longitude'])
            except:
                # Ignore if no parsable lat and lon
                pass
            id = self.makeId(device['siteId'], lat, lon)
            name = device['siteName']
            time = (datetime.strptime(device['utc'], "%Y-%m-%d %H:%M:%S") - datetime(1970,1,1)).total_seconds()
            param_name = device['parameterName'].replace('-', '_')
            qc_feed = Feed(id + '_qc', name + '_qc', lat, lon)
            raw_data_cache.setdefault((qc_feed, time), []).append(device)
            qc_data = feed_time_cache.setdefault((qc_feed, time), {})
            qc_data['time'] = time
            qc_data[param_name + '_qcCode'] = device['qcCode']
            if not device['qcCode'] == 9: # Invalid QC
                feed = Feed(id, name, lat, lon)
                raw_data_cache.setdefault((feed, time), []).append(device)
                data = feed_time_cache.setdefault((feed, time), {})
                data['time'] = time
                value = device['value']
                data[param_name] = value
        for feedtime, data in feed_time_cache.iteritems():
            feed, time = feedtime
            yield self.getFeed(*feed), self.makeEsdrUpload(data), raw_data_cache[(feed, time)]
