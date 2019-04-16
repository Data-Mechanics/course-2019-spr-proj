#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 14 18:42:34 2019

@author: zhukaikang
"""

import pandas as pd
import requests
import json
import dml
import prov.model
import datetime
import uuid
import csv
from io import StringIO
import json
import pymongo
import numpy as np
from sklearn.cluster import KMeans

class cau_kmeans(dml.Algorithm):
    contributor = 'mmao95_Dongyihe_weijiang_zhukk'
    reads = [contributor + '.colleges_and_universities']
    writes = [contributor + '.cau_kmeans']
    
    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()
        contributor = 'mmao95_Dongyihe_weijiang_zhukk'
        reads = [contributor + '.colleges_and_universities']
        writes = [contributor + '.cau_kmeans']

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(contributor, contributor)
        
        #get CAU_list from mongodb
        CAU_list = list(repo[reads[0]].find())
        CAU_df = pd.DataFrame(CAU_list)
        CAU_list = np.array(CAU_df).tolist()
        for _ in range(2):
            del CAU_list[-1]
        # define relational models

        def union(R, S):
            return R + S

        def difference(R, S):
            return [t for t in R if t not in S]

        def intersect(R, S):
            return [t for t in R if t in S]

        def project(R, p):
            return [p(t) for t in R]

        def select(R, s):
            return [t for t in R if s(t)]

        def product(R, S):
            return [(t, u) for t in R for u in S]

        def aggregate(R, f):
            keys = {r[0] for r in R}
            return [(key, f([v for (k, v) in R if k == key])) for key in keys]
        
        cities = set()
        num = 0
        for i in range(len(CAU_list)):
            if CAU_list[i][1] not in cities:
                cities.add(CAU_list[i][1])
                num += 1
        #print(cities)
        #print(num)
        cau_list = [(latitude, longitude) for [Address, City,
                                               latitude, longitude, Name, Zipcode, id] in CAU_list]
        
        X = cau_list
        print(type(X[0][0]))
        kmeans = KMeans(n_clusters=num, random_state=0).fit(X)
        
        cau_kmeans = project(CAU_list, lambda t: [t[1], t[4], t[0], 0])
        for i in range(len(cau_kmeans)):
            cau_kmeans[i][3] = kmeans.labels_[i]
        
        columnName = ['Neighborhood', 'Name', 'Address', 'kmeans_id']
        df = pd.DataFrame(columns=columnName, data=cau_kmeans)
        data = json.loads(df.to_json(orient="records"))
        
        repo.dropCollection('cau_kmeans')
        repo.createCollection('cau_kmeans')
        repo[writes[0]].insert_many(data)
        
        repo.logout()
        
        endTime = datetime.datetime.now()
        
        return {"start": startTime, "end": endTime}
      
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        contributor = 'mmao95_Dongyihe_weijiang_zhukk'
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(contributor, contributor)

        # The scripts are in <folder>#<filename> format.
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        # The data sets are in <user>#<collection> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        # The event log.
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace('bdp', 'https://www.50states.com/bio/mass.htm')

        this_script = doc.agent('alg:' + contributor + '#cau_kmeans', {
                                prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label': '311, Service Requests',
                                                prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        get_names = doc.activity(
            'log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_names, this_script)
        doc.usage(get_names, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Computation': 'Data cleaning'
                   }
                  )

        fp = doc.entity('dat:' + contributor + '#cau_kmeans', {
                        prov.model.PROV_LABEL: 'CAU Kmeans', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(fp, this_script)
        doc.wasGeneratedBy(fp, get_names, endTime)
        doc.wasDerivedFrom(fp, resource, get_names, get_names, get_names)

        repo.logout()

        return doc

    
dict = cau_kmeans.execute()
#print(type(dict['start']))
#doc = cau_kmeans.provenance(prov.model.ProvDocument(), dict['start'], dict['end'])


