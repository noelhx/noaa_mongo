import csv, gzip, os
import pandas as pd

from datetime import datetime, timedelta
from ftplib import FTP
from pymongo import MongoClient, GEOSPHERE

from math import sin, cos, sqrt, atan2, radians # only for calculating distance function
def ll_dist(lon1, lat1, lon2, lat2):
    R = 6373.0
    lat1 = radians(lat1)
    lon1 = radians(lon1)
    lat2 = radians(lat2)
    lon2 = radians(lon2)
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = (sin(dlat/2))**2 + cos(lat1) * cos(lat2) * (sin(dlon/2))**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    distance = R * c
    return distance

class NOAAMongoManager:
    def __init__(self, temp_dir= '.temp', mongo_host='localhost', mongo_db='noaa'):
        self.temp_dir= 	temp_dir
        self.m_client=  MongoClient(mongo_host)
        self.m_db	 =	self.m_client[ mongo_db ]
        self.hx 	 =	self.m_db['hx']
        self.record_count = 0
    def setup_dirs(self):
        os.mkdir(self.temp_dir)
        os.mkdir('%s/noaa_raw' % self.temp_dir)
    def noaa_hx(self, start_year, end_year):
        ftp = FTP('ftp.ncdc.noaa.gov')     # connect to host, default port
        ftp.login()
        ftp.retrbinary('RETR /pub/data/ghcn/daily/ghcnd-stations.txt', open("%s/%s" % (self.temp_dir, 'stations.txt'), 'wb').write)
        ftp.cwd('/pub/data/ghcn/daily/by_year/')
        for year in range(start_year, end_year+1):
            file_name = "%i.csv.gz" % year
            retr_str  = 'RETR ' + file_name
            print "retrieved file: %s" % file_name
            ftp.retrbinary(retr_str, open("%s/noaa_raw/%s" % (self.temp_dir, file_name), 'wb').write)
        ftp.close()
    def load_stations(self):
        with open("%s/%s" % (self.temp_dir, 'stations.txt'), 'rb') as f:
            self.stations = { row[0:11]: [ float(row[21:30]), float(row[12:20])] for row in f.readlines()}  # Mongo GSON objects must be (long, lat). Stupid.
    def _make_hx_doc(self, row):
        _id = row[0]+row[1]
        doc = { '_id' 		: row[0]+row[1],
                'date'		: datetime.strptime(row[1], '%Y%m%d'),
                row[2]		: int( row[3] ),
                'station'	: row[0],
                'loc'		: {'type':'Point', 'coordinates': self.stations[row[0]] }
                }
        self.hx.update({'_id': _id }, {'$set': doc} , upsert=True)
    def uptake_hx(self, fname):
        print 'uploading file: %s into mongodb.noaa.hx' % fname
        with gzip.open('%s/noaa_raw/%s' % (self.temp_dir, fname), 'rb') as f:
            for row in csv.reader(f):
                self._make_hx_doc(row)
                self.record_count +=1
    def deploy(self, start_year, end_year, make_dirs=False):
        if make_dirs:
            self.setup_dirs()
        self.noaa_hx(start_year, end_year)
        self.load_stations()
        for fname in os.listdir('%s/noaa_raw' % self.temp_dir):
            self.uptake_hx(fname)
    def _get_avail_keys(self):
        keys = list()
        print 'generating list of fields...'
        for doc in self.hx.find():
            keys = list(set(keys + doc.keys()))
        for key in ['_id', 'date', 'station', 'loc']:
            keys.remove(key)
        self.avail_keys= keys
    def create_indexes(self, keys):
        for key in keys:
            self.create_index(key)
    def create_index(self, key, refresh_avail_keys=False):
        if refresh_avail_keys:
            self._get_avail_keys()
        if key in self.avail_keys:
            print 'creating compound index on date->%s->location' %key
            self.hx.create_index([('date', 1), (key, 1), ('loc', GEOSPHERE)])
        else:
            print '%s does not exist in hx' % key
        print 'done'
    def dump_all_indexes(self, compact_on_complete=True):
        print 'dumping all indexes... this may take a while'
        self.hx.drop_indexes()
        if compact_on_complete:
            self.m_db.command('compact', 'hx')
        print 'all indexes dumped'
    def fetch_df_on_date(self, date, point, key, closest_n=20):
        """
        :param date:        datetime.date
        :param point:       tuple(long, lat)- long, lat are floats
        :param closest_n:   int - the closest n stations
        :return: df:        a data frame of the stations you have requested
        """
        cur = self.hx.find({ '$and': [
				{'date': date},
				{key: {'$exists':True}},
				{'loc': {'$near':{'$geometry': { 'type': "Point", "coordinates": [ point[1], point[0]] } } }}
			]}, limit=closest_n)
        df = pd.DataFrame([i for i in cur])
        return df
    def avg_val(self, df, key, point):
        """
        :param df:  pandas df- recieved from fetch_df_on_date
        :param key: string- the NOAA key you are computing, e.g. 'SNOW'
        :return: value: float- the calculated value at the given point
        """
        df['inv_dist']= df['loc'].apply(lambda row: 1/ll_dist(row['coordinates'][0],row['coordinates'][1],point[1],point[0] ))
        sum_inv_dist = df['inv_dist'].sum()
        df['wgt'] = df['inv_dist'].apply(lambda row: row/sum_inv_dist)
        value = df[key].multiply(df['wgt']).sum()
        assert isinstance(value, float)
        return value
    def weather_for_date(self, start_date, end_date, key, point, closest_n):
        """
        :param start_date:  datetime
        :param end_date:    datetime
        :param key:         string
        :param point:       tuple(lng,lat)
        :param closest_n:   int
        :return: pd.df
        """
        date_range = (datetime(2013, 12, 3), datetime(2013,12,8))
        days_needed = (date_range[1] - date_range[0]).days
        dates_needed = [ date_range[0] + timedelta(days= x) for x in range(0, days_needed) ]
        output = pd.DataFrame([{'date': date, 'val': self.avg_val(self.fetch_df_on_date( date, point, key, closest_n), key, point) } for date in dates_needed])
        return output

