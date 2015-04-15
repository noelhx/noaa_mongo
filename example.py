from build_mongo import BuildNOAAMongo

# Build an NOAA historical database from 2012-present.
bnm = BuildNOAAMongo()
bnm.deploy(2012,2015, False)
bnm.create_indexes()
