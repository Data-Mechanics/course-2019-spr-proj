import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

import pandas as pd
import numpy as np
from ast import literal_eval
from scipy.cluster.vq import vq, kmeans, whiten
import scipy.stats
from geopy.distance import distance

def goodCoords(t):
    return abs(t[0]) > 0.0 and t[0] != -1 and abs(t[1]) > 0.0 and t[1] != -1

stations = {'A1': (42.361751,-71.060117),
            'A15': (42.361751,-71.060117),
            'A7': (42.3710345,-71.0388061),
            'B2': (42.3284313,-71.0862445),
            'B3': (42.2846826,-71.0916905),
            'C6': (42.361751,-71.060117),
            'C11': (42.2980276,-71.0590886),
            'D4': (42.3394369,-71.0692247),
            'D14': (42.3493611,-71.1505902),
            'E5': (42.2867829,-71.1484246),
            'E13': (42.299275,-71.115235),
            'E18': (42.2564288,-71.1242782)}

class boston_crime_police_distance(dml.Algorithm):
    contributor = 'hek_kquirk'
    reads = ['hek_kquirk.boston_crime_incidents']
    writes = ['hek_kquirk.boston_crime_police_distance']
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('hek_kquirk', 'hek_kquirk')

        # Drop/recreate mongo collection
        repo.dropCollection("boston_crime_police_distance")
        repo.createCollection("boston_crime_police_distance")

        # Get crime incident locations and convert to float tuples
        incident_locs_raw = repo['hek_kquirk.boston_crime_incidents'].find({}, {'Location': 1, 'DISTRICT': 1})
        distances = [distance(l, stations[s]) for l,s in ((literal_eval(inc['Location']), inc['DISTRICT']) for inc in incident_locs_raw)
                         if goodCoords(l) and s in stations]
        print(len(distances))
        
        distances = np.histogram(distances, [i/5 for i in range(15)])
        print(distances)

        print(scipy.stats.pearsonr(distances[1][1:],distances[0]))
        
        repo['hek_kquirk.boston_crime_police_distance'].metadata({'complete':True})
        print(repo['hek_kquirk.boston_crime_police_distance'].metadata())
        
        repo.logout()
        
        endTime = datetime.datetime.now()
        
        return {"start":startTime, "end":endTime}
    
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
        repo.authenticate('hek_kquirk', 'hek_kquirk')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        repo.logout()
        
        return doc

'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

## eof
