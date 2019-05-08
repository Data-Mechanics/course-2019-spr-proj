#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 16 11:06:34 2019

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

class streetbook_kmeans(dml.Algorithm):
    contributor = 'mmao95_dongyihe_weijiang_zhukk'
    reads = [contributor + '.streetbook_filtered',
              contributor + '.streetbook_alternate']
    writes = [contributor + '.streetbook_kmeans']
    
    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()
        contributor = 'mmao95_dongyihe_weijiang_zhukk'
        reads = [contributor + '.streetbook_filtered',
              contributor + '.streetbook_alternate']
        writes = [contributor + '.streetbook_kmeans']

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(contributor, contributor)
        
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
        
        filtered_list = list(repo[reads[0]].find())
        filtered_df = pd.DataFrame(filtered_list)
        filtered_list = np.array(filtered_df).tolist()
        
        alternate_list = list(repo[reads[1]].find())
        alternate_df = pd.DataFrame(alternate_list)
        alternate_list = np.array(alternate_df).tolist()
        
        alternate = [street_name for [redundant_time, street_name, id_] in alternate_list]
        #print(alternate)
        #for i in range(len(alternate_list)):
            #if alternate_list[i][1] in 
        kmeans_data = [(full_name, zipcode) for [full_name, street_name, zipcode, id_] in filtered_list if street_name in alternate]
        kmeans_data1 = [(zipcode,) for [full_name, zipcode] in kmeans_data]
        kmeans = KMeans(n_clusters=26, random_state=0).fit(kmeans_data1)
        streetbook_kmeans = [[full_name, zipcode, 0] for [full_name, zipcode] in kmeans_data]
        for i in range(len(streetbook_kmeans)):
            streetbook_kmeans[i][2] = kmeans.labels_[i]
        #print(kmeans_data[0:2])
        column_names = ['full_name', 'zipcode', 'kmeans_cluster']
        df = pd.DataFrame(columns=column_names, data=streetbook_kmeans)
        data = json.loads(df.to_json(orient="records"))
        
        repo.dropCollection('streetbook_kmeans')
        repo.createCollection('streetbook_kmeans')
        repo[writes[0]].insert_many(data)
        
        repo.logout()
        
        endTime = datetime.datetime.now()
        
        return {"start": startTime, "end": endTime}
    
    @staticmethod    
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        contributor = 'mmao95_dongyihe_weijiang_zhukk'
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

        this_script = doc.agent('alg:'+contributor+'#streetbook_kmeans', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        res_sf = doc.entity('dat:'+contributor+'#streetbook_filtered', {prov.model.PROV_LABEL:'Streetbook Filtered', prov.model.PROV_TYPE:'ont:DataSet'})
        res_sa = doc.entity('dat:'+contributor+'#streetbook_alternate', {prov.model.PROV_LABEL:'Streetbook Alternate', prov.model.PROV_TYPE:'ont:DataSet'})
        filter_names = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(filter_names, this_script)
        doc.usage(filter_names, res_sf, startTime, None,
            {prov.model.PROV_TYPE: 'ont:Computation',
            'ont:Computation':'Selection, Differentiate'
            }
        )
        doc.usage(filter_names, res_sa, startTime, None,
            {prov.model.PROV_TYPE: 'ont:Computation',
            'ont:Computation':'Selection, Differentiate'
            }
        )
        result = doc.entity('dat:'+contributor+'#streetbook_kmeans', {prov.model.PROV_LABEL:'Streetbook with K Means', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(result, this_script)
        doc.wasGeneratedBy(result, filter_names, endTime)
        doc.wasDerivedFrom(result, res_sf, filter_names, filter_names, filter_names)
        doc.wasDerivedFrom(result, res_sa, filter_names, filter_names, filter_names)
        repo.logout()

        return doc
    
