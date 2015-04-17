from build_mongo import NOAAMongoManager

nmm = NOAAMongoManager()
nmm.deploy(2012,2015, make_dirs=True) # This take 115GB, if you don't have much space, just do 2015 or something
nmm.avail_keys = ['SNOW']    # getting a list of avail keys takes forever, in the future I'll store that in the mongo and it will be set up as it builds
nmm.create_index('SNOW', refresh_avail_keys=False)

