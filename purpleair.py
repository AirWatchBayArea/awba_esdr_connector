import datetime, json, requests, logging, os

from uploader import Uploader

from requests_toolbelt.adapters import appengine
appengine.monkeypatch()

class PurpleAirUploader(Uploader):

    def get_all_purple_air_devices(self):
        body = requests.get('https://www.purpleair.com/json')
        # Surprisingly, the JSON is in latin1 rather than utf-8
        # decoded = body.decode('latin1')
        js = body.json()
        devices = js['results'][0]
        for device in devices:
            yield device

    def get_purple_air_device(self, device_id):
        body = requests.get('https://www.purpleair.com/json?show=%d' % device_id)
        # Surprisingly, the JSON is in latin1 rather than utf-8
        # decoded = body.decode('latin1')
        js = body.json()
        devices = js['results']
        for device in devices:
            yield device

    def make_date_param(self, date):
        return date.strftime('%Y-%m-%d%%20%H:%M:%S')  

    def get_thingspeak_url(self, device, start, end, rounding=2):
        thingspeak_id = device['THINGSPEAK_PRIMARY_ID']
        api_key = device['THINGSPEAK_PRIMARY_ID_READ_KEY']
        return 'https://thingspeak.com/channels/{0}/feed.json?api_key={1}&offset=0&average=&round={2}&start={3}&end={4}'.format(thingspeak_id, api_key, rounding, self.make_date_param(start), self.make_date_param(end))

    def get_thingspeak_data(self, device, start, end, rounding=2):
        body = requests.get(self.get_thingspeak_url(device, start, end, rounding))
        return body.json()

    def upload_thingspeak_data(self, device, start, end, rounding=2):
        try:
            lat = float(device['Lat'])
            lon = float(device['Lon'])
        except:
            # Ignore if no parsable lat and lon
            return
        id = '%s_%06d%s%06d%s' % (device['ID'], 
                                  round(1000 * abs(lat)), 'NS'[lat < 0], 
                                  round(1000 * abs(lon)), 'EW'[lon < 0])
        id = id.replace('.','_')

        thingspeak_data = self.get_thingspeak_data(device, start, end, rounding)
        channel_map = {value:key for key, value in thingspeak_data['channel'].items() if 'field' in key}

        for feed in thingspeak_data['feeds']:
            data = {}
            try:
                data['time'] = (datetime.datetime.strptime(feed['created_at'], "%Y-%m-%dT%H:%M:%SZ") - datetime.datetime(1970,1,1)).total_seconds()
            except Exception as e:
                logging.error(e)
                return

            for key in ['PM2.5 (CF=1)', 'RSSI', 'Uptime', 'Humidity', 'Temperature']:
                translated_key = key
                if key == 'PM2.5 (CF=1)':
                    translated_key = 'PM2_5'
                elif key == 'Temperature':
                    translated_key = 'temp_f'
                elif key == 'Humidity':
                    translated_key = 'humidity'
                try:
                    data[translated_key] = float(feed[channel_map[key]])
                except:
                    pass
            # Move time to the beginning since that's how ESDR likes it
            keys = ['time'] + sorted(set(data.keys()).difference(set(['time'])))
            values = [data[key] for key in keys]
            if values:
                # TIME is implicit for ESDR;  don't list in channel_names
                esdr_upload = {'channel_names':keys[1:], 'data':[values]}
                name = device['Label']
                # feed = self.getFeed(id, name, lat, lon)
                # self.esdr.upload(feed, esdr_upload)
        logging.info('Uploaded to %s (%s)' % (id, name))

    def parse_device(self, device):
        try:
            lat = float(device['Lat'])
            lon = float(device['Lon'])
        except:
            # Ignore if no parsable lat and lon
            return
        id = self.makeId(device['ID'], lat, lon)
        data = {}

        for key in ['PM2_5Value', 'RSSI', 'Uptime', 'humidity', 'pressure', 'temp_f']:
            translated_key = key
            if key == 'PM2_5Value':
                translated_key = 'PM2_5'
            try:
                data[translated_key] = float(device[key])
            except:
                pass

        try:
            stats = json.loads(device['Stats'])
            for key in stats.keys():
                if key == 'lastModified' or key == 'timeSinceModified':
                    continue
                data['stats_' + key] = stats[key]
        except:
            # Stats stopped being reported Jan 2018
            pass

        try:
            data['time'] = stats['lastModified'] / 1000.0
        except:
            # Skip if no time recorded
            return

        esdr_upload = self.makeEsdrUpload(data)
        name = device['Label']
        feed = self.getFeed(id, name, lat, lon)
        return feed, esdr_upload, data