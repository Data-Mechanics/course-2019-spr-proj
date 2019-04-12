from gmplot import gmplot
from sklearn.cluster import KMeans
import json
import dml
import uuid
import prov.model
from datetime import datetime
from pyproj import Proj, transform
from numpy.random import uniform
from numpy import array
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
import math
import statistics

import folium
from folium import plugins
import pymongo
from geopy.distance import vincenty
import random
import matplotlib.pyplot as plt


class avgPrice_stdevPrice(dml.Algorithm):
    contributor = 'ekmak_gzhou_kaylaipp_shen99'
    reads = ['ekmak_gzhou_kaylaipp_shen99.zillow_getsearchresults_data']
    writes = ['ekmak_gzhou_kaylaipp_shen99.avg_price_stdev_price']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.now()

        #connect to database
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ekmak_gzhou_kaylaipp_shen99','ekmak_gzhou_kaylaipp_shen99')

        # read in zillow search result data 
        zillow_data = repo.ekmak_gzhou_kaylaipp_shen99.zillow_getsearchresults_data.find()
        priceAvg = []
        priceStdev = []

        # get coordinates from zillow data 
        for info in zillow_data: 
            try: 
                # zpid = info['zpid']
                valuation = int(info['zestimate']['amount'])

                # ensure values are not NaN 
                if valuation: 
                    priceAvg.append(['price', valuation])
                    # results.append(['zpid'], zpid)
                    priceStdev.append(valuation)
            except: 
                continue

        #calculate average price
        totalPrice = 0
        count = 0
        for row in priceAvg:
            totalPrice += row['price']
            count += 1
        avgPrice = totalPrice / count

        #calculate standard deviation
        stdevPrice = statistics.stdev(priceStdev)

        
        # Store information in db
        repo.logout()
        endTime = datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ekmak_gzhou_kaylaipp_shen99','ekmak_gzhou_kaylaipp_shen99')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_agent = doc.agent('alg:ekmak_gzhou_kaylaipp_shen99#avgPrice_stdevPrice',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        this_entity = doc.entity('dat:ekmak_gzhou_kaylaipp_shen99#avgPrice_stdevPrice',
                            {prov.model.PROV_LABEL: 'average and standard deviation of housing prices', prov.model.PROV_TYPE: 'ont:DataSet'})

        zillow_data = doc.entity('dat:ekmak_gzhou_kaylaipp_shen99#zillow_getsearchresults_data', {prov.model.PROV_LABEL: 'Zillow Search Data', prov.model.PROV_TYPE: 'ont:DataSet'})

        get_zillow_data = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(zillow_data, this_script)

        doc.usage(get_zillow_data, zillow_data, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

        doc.wasAttributedTo(price_clusters, this_script)
        
        doc.wasGeneratedBy(price_clusters, get_cluster_medians, endTime)
        
        doc.wasDerivedFrom(price_clusters, zillow_data, get_cluster_medians, get_cluster_medians, get_cluster_medians)

        repo.logout()
                  
        return doc
