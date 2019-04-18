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
        incident_locs_raw = repo['hek_kquirk.boston_crime_incidents'].find({}, {'Location': 1, 'DISTRICT': 1})
        if trial:
            incident_locs_raw = incident_locs_raw[:1000]
        
        locs_dict = {}
        for k in stations:
            locs_dict[k] = []

        #Sort coordinates by district into locs_dict
        for l, s in ((literal_eval(inc['Location']), inc['DISTRICT']) for inc in incident_locs_raw):
            if goodCoords(l) and s in stations:
                locs_dict[s].append(l)
        
        results = dict.fromkeys(k for k in stations)
        
        #Run kmeans and obtain one cluster per police station
        for k in locs_dict:
            incident_locs_np = np.array(locs_dict[k])
            results[k] = kmeans(incident_locs_np, 1)

        with open('clusters.kml', 'w') as d:
            #Create kml file (for google maps)
            d.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
            d.write("<kml xmlns=\"http://www.opengis.net/kml/2.2\">\n")
            d.write("<Document>\n")
            for k in results:
                d.write("<Placemark>\n")
                d.write("<Point>\n")
                d.write("<coordinates>" + str(results[k][0][0][1]) + "," + str(results[k][0][0][0]) + ",0</coordinates>")
                d.write("</Point>\n")
                d.write("</Placemark>\n")
            d.write("</Document>\n")
            d.write("</kml>")

        #Write KML file to database
        with open('clusters.kml', 'r') as f:
            d = f.read()
            doc = {"_id": "boston_crime_clusters.kml", "contents": d}
            repo['hek_kquirk.boston_crime_clusters'].insert(doc)

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
        
        this_script = doc.agent('alg:hek_kquirk#boston_crime_clusters', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource = doc.entity('dat:hek_kquirk#boston_crime_incidents', {'prov:label':'Boston Crime Incidents', prov.model.PROV_TYPE:'ont:DataResource'})
        get_boston_crime_clusters = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_boston_crime_clusters, this_script)
        doc.usage(get_boston_crime_clusters, resource, startTime, None,
                  {
                      prov.model.PROV_TYPE:'ont:Computation'
                  }
        )
        
        boston_crime_clusters = doc.entity('dat:hek_kquirk#boston_crime_clusters', {prov.model.PROV_LABEL:'Boston Crime Clusters', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(boston_crime_clusters, this_script)
        doc.wasGeneratedBy(boston_crime_clusters, get_boston_crime_clusters, endTime)
        doc.wasDerivedFrom(boston_crime_clusters, resource, get_boston_crime_clusters, get_boston_crime_clusters, get_boston_crime_clusters)

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
