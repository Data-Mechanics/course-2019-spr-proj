#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 30 12:35:39 2019

@author: zhukaikang
"""

import json
import pandas as pd
import numpy as np

import requests
import dml
import prov.model
import datetime
import uuid
import csv
from io import StringIO
import json
import pymongo

class select_candidate(dml.Algorithm):
    contributor = 'mmao95_dongyihe_weijiang_zhukk'
    reads = []
    writes = []

    @staticmethod
    def execute(trial=False):
        contributor = 'mmao95_dongyihe_weijiang_zhukk'
        reads = []
        writes = []

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(contributor, contributor)
        
        from urllib.request import urlopen
        u = urlopen('http://datamechanics.io/data/neighbourhood1.json')
        resp = json.loads(u.read())
        
        zip_json = resp
        
        #print(len(zip_json))
        d = {}
        sum = 0
        for key in zip_json:
            #print(key)
            sum += 1
            street_names = (zip_json[key]).split(' ')
            
            #print(type(street_names[0]))
            keys = set()
            for i in street_names:
                keys.add(i)
                
            redundant_street = []
            for item in keys:
                redundant_street.append([item, 0])
            
            for item in redundant_street:
                for street in street_names:
                    if item[0] == street:
                        item[1] += 1
                        
            
            max = ''
            max_num = 0
            for item in redundant_street:
                if item[1] > max_num:
                    max_num = item[1]
                    max = item[0]
            #d[key] = max+' ' + str(max_num)
            d[key] = max
        
        #print(d['02210'])
        street_lat_long = pd.read_csv(
                    "http://datamechanics.io/data/roads_2013_jzi.csv").values.tolist()
        
        # process street latitude and longitude dataset
        street_lat_long_data = [(fullName, location, length, zi) for (fId, location, s, c, tl, tf, tf2, m, fullName,
                                                                  sm, lf, lt, rf, rt, zi, zipr, length, classGroup, r,
                                                                  cluster, m, zone, bg, ct) in street_lat_long]
        
        # print(street_lat_long[0])
        lat_long_df = pd.DataFrame(street_lat_long_data)
        lat_long_df.columns = ['fullName', 'location', 'length','zipl']
        lat_long_df = lat_long_df.dropna()
        
        lat_long_list = np.array(lat_long_df).tolist()
        
        res = []
        for i in range(len(zip_json)):
            res.append([])
        #print(res)
        
        sum = 0
        #print('0' + str(int(lat_long_list[0][3])))
        #print(str(lat_long_list[0][0].split()[0]))
        
        for key in d:
            for j in range(len(lat_long_list)):
                if key == '0' + str(int(lat_long_list[j][3])) and d[key] == str(lat_long_list[j][0].split()[0]):
                    #print(1)
                    res[sum].append(lat_long_list[j])
            sum += 1       
        #print(res[0])
        #print(res[18])
        
        for k in range(len(res)):
            keys = {r[0] for r in res[k]}
            #print(len(keys))
            new_res = []
            for i in range(len(keys)):
                new_res.append(['','',0,0])
            #print(new_res)
            
            
            sum = 0
            for key in keys:
                #print(type(key))
                for i in range(len(res[k])):
                    if str(key) == str(res[k][i][0]):
                        #print(1)
                        new_res[sum][0] = key
                        new_res[sum][1] += res[k][i][1]
                        new_res[sum][2] += res[k][i][2]
                        new_res[sum][3] = res[k][i][3]
                sum += 1
            
            #print(new_res)
            candidate = []
            min_length = 1000000000
            for i in range(len(new_res)):
                if new_res[i][2] < min_length:
                    min_length = new_res[i][2]
                    candidate = new_res[i]
            
            if res[k] != []:
                print('The candidate for ' + str(res[k][0][3]) + ':' + str(candidate))
                print(' ')
        
        #print('total key is ')
        #print(sum)
        #print(d)
        #with open('candidate_zip.txt', 'w') as outfile:
            #json.dump(d, outfile)
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}
    
    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
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
        
        

        repo.logout()

        return doc
    
