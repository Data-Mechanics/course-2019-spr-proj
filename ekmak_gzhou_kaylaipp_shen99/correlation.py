#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 12 13:14:57 2019

@author: ellenmak
"""

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
import math

import folium
from folium import plugins
import pymongo
from geopy.distance import vincenty
import random
import matplotlib.pyplot as plt



class correlation(dml.Algorithm):
    contributor = 'ekmak_gzhou_kaylaipp_shen99'
    reads = ['ekmak_gzhou_kaylaipp_shen99.zillow_getsearchresults_data']
    writes = ['ekmak_gzhou_kaylaipp_shen99.price_location_corr']



    @staticmethod
    def execute(trial=False):
        startTime = datetime.now()

        #connect to database
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ekmak_gzhou_kaylaipp_shen99','ekmak_gzhou_kaylaipp_shen99')

        # read in zillow search result data 
        zillow_data = repo.ekmak_gzhou_kaylaipp_shen99.zillow_getsearchresults_data.find()
        lat_list = []
        long_list = []
        count = 0
        all_coords = []   #holds x,y lat long coords 
        coords_with_labels = []
        results = []   #in format (zpid, valuation, x, y)
        price = []

        # get coordinates from zillow data 
        for info in zillow_data: 
            try: 
                zpid = info['zpid']
                lon = float(info['full_address']['latitude'])
                lat = float(info['full_address']['longitude'])
                valuation = int(info['zestimate']['amount'])

                # ensure values are not NaN 
                if valuation and lon and lat: 
                    price.append([valuation])
                    all_coords.append([lat, lon])
                    #price.append([valuation, lat, lon])
                    results.append((zpid, valuation, lat, lon))
            except: 
                continue
        
        
        # convert price list to np array and reshape 
        price = array(price)
        all_coords = array(all_coords)


        corr(price,all_coords)

        # Store information in db
        repo.logout()
        endTime = datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ekmak_gzhou_kaylaipp_shen99', 'ekmak_gzhou_kaylaipp_shen99')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:ekmak_gzhou_kaylaipp_shen99#price_location_corr', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        zillow_data = doc.entity('dat:ekmak_gzhou_kaylaipp_shen99#zillow_getsearchresults_data', {prov.model.PROV_LABEL: 'Zillow Search Data', prov.model.PROV_TYPE: 'ont:DataSet'})

        get_zillow_data = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(zillow_data, this_script)

        doc.usage(get_zillow_data, zillow_data, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

        get_cluster_medians = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        price_clusters = doc.entity('dat:ekmak_gzhou_kaylaipp_shen99#correlation',
                                     {prov.model.PROV_LABEL: 'Cluster centers for price valuations', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(price_clusters, this_script)
        doc.wasGeneratedBy(price_clusters, get_cluster_medians, endTime)
        doc.wasDerivedFrom(price_clusters, zillow_data, get_cluster_medians, get_cluster_medians, get_cluster_medians)

        repo.logout()

        return doc

def permute(x):
    shuffled = [xi for xi in x]
    shuffle(shuffled)
    return shuffled

def avg(x): # Average
    return sum(x)/len(x)

def stddev(x): # Standard deviation.
    m = avg(x)
    return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

def cov(x, y): # Covariance.
    return sum([(xi-avg(x))*(yi-avg(y)) for (xi,yi) in zip(x,y)])/len(x)

def corr(x, y): # Correlation coefficient.
    if stddev(x)*stddev(y) != 0:
        return cov(x, y)/(stddev(x)*stddev(y))

if __name__ == "__main__":
    correlation.execute()
    # doc = optimization.provenance()

