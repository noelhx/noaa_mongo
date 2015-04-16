from datetime import datetime
from build_mongo import NOAAMongoManager
# Build an NOAA historical database from 2012-present.

nmm = NOAAMongoManager()
#bnm.deploy(2012,2015, False)
#bnm.create_indexes()
#bnm.dump_all_indexes()
#nmm.avail_keys = ['SNOW']
#nmm.create_index('SNOW', refresh_avail_keys=False)

point = (39.9662208,-83.7865371)
df= nmm.weather_for_date(datetime(2013,1,1), datetime(2013,1,31), 'SNOW', point, 20)
print df

