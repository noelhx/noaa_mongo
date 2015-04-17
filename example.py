from datetime import datetime
from build_mongo import NOAAMongoManager
# Build an NOAA historical database from 2012-present.

nmm = NOAAMongoManager()
#bnm.deploy(2012,2015, False) # This take 115GB, if you don't have much space, just do 2015 or something
#bnm.create_indexes()
#bnm.dump_all_indexes()
#nmm.avail_keys = ['SNOW']
#nmm.create_index('SNOW', refresh_avail_keys=False)

point = (39.9662208,-83.7865371)
sp = datetime.now()
df= nmm.weather_for_date(datetime(2013,1,1), datetime(2014,12,31), 'SNOW', point, 20)
print datetime.now()- sp
#2m38s

point2 = (39.95,-83.78)
sp = datetime.now()
df= nmm.weather_for_date(datetime(2013,1,1), datetime(2014,12,31), 'SNOW', point2, 5)
print datetime.now()- sp
#1m40s
