import csv, gzip, os

from datetime import datetime
from ftplib import FTP
from pymongo import MongoClient, GEOSPHERE

class BuildNOAAMongo:
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
		#ftp.dir()
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

    def dump_all_indexes(self, compact_on_complete=True):
        print 'dumping all indexes... this may take a while'
        self.hx.drop_indexes()
        if compact_on_complete:
            self.m_db.command('compact', 'hx')
        print 'all indexes dumped'