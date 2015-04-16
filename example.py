from build_mongo import BuildNOAAMongo

# Build an NOAA historical database from 2012-present.
bnm = BuildNOAAMongo(mongo_host='10.30.10.5')
#bnm.deploy(2012,2015, False)
#bnm.create_indexes()

bnm.create_index('SNOW')

