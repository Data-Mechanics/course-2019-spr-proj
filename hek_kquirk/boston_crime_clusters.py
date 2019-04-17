import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

import numpy as np
from ast import literal_eval
from scipy.cluster.vq import vq, kmeans, whiten

def goodCoords(t):
    return abs(t[0]) > 0.0 and t[0] != -1 and abs(t[1]) > 0.0 and t[1] != -1


class boston_crime_clusters(dml.Algorithm):
    contributor = 'hek_kquirk'
    reads = ['hek_kquirk.boston_crime_incidents']
    writes = ['hek_kquirk.boston_crime_clusters']
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('hek_kquirk', 'hek_kquirk')

        # Drop/recreate mongo collection
        repo.dropCollection("boston_crime_clusters")
        repo.createCollection("boston_crime_clusters")

        # Get crime incident locations and convert to float tuples
        incident_locs_raw = repo['hek_kquirk.boston_crime_incidents'].find({}, {'Location': 1})
        incident_locs = [literal_eval(inc['Location']) for inc in incident_locs_raw
                               if goodCoords(literal_eval(inc['Location']))]

        #One cluster per police station
        num_clusters = 11
        incident_locs_np = np.array(incident_locs)
        res = kmeans(incident_locs_np, num_clusters)

        #Create kml file (for google maps)
        with open('clusters.kml', 'w') as f:
            f.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
            f.write("<kml xmlns=\"http://www.opengis.net/kml/2.2\">\n")
            f.write("<Document>\n")

            for r in res[0]:
                f.write("<Placemark>\n")
                f.write("<Point>\n")
                f.write("<coordinates>" + str(r[1]) + "," + str(r[0]) + ",0</coordinates>")
                f.write("</Point>\n")
                f.write("</Placemark>\n")
                
            f.write("</Document>\n")
            f.write("</kml>")
            
        repo['hek_kquirk.boston_crime_clusters'].insert_many(res[0])

        repo['hek_kquirk.boston_crime_clusters'].metadata({'complete':True})
        print(repo['hek_kquirk.boston_crime_clusters'].metadata())

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
