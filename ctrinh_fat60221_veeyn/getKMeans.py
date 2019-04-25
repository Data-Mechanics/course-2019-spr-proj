import urllib.request
import json
import csv
import dml
import prov.model
import datetime
import uuid
from io import StringIO
from collections import OrderedDict
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans

class getKMeans (dml.Algorithm):
    contributor = 'ctrinh_fat60221_veeyn'
    reads = []
    writes = ['ctrinh_fat60221_veeyn.kMeans']
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ctrinh_fat60221_veeyn', 'ctrinh_fat60221_veeyn')
        
        #obtaining all the longitudes and latitudes of coffee shops
        url = 'http://datamechanics.io/data/min_dist.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)

        if trial == True:
            centroids = []
            for x in range(5):
                centroids.append([float(r['Green Line B'][x]['coffee_shop']['coordinates']['longitude']),float(r['Green Line B'][x]['coffee_shop']['coordinates']['latitude'])])

            for x in range(5):
                centroids.append([float(r['Green Line C'][x]['coffee_shop']['coordinates']['longitude']),float(r['Green Line C'][x]['coffee_shop']['coordinates']['latitude'])])

            for x in range(5):
                centroids.append([float(r['Green Line D'][x]['coffee_shop']['coordinates']['longitude']),float(r['Green Line D'][x]['coffee_shop']['coordinates']['latitude'])])

            for x in range(5):
                centroids.append([float(r['Green Line E'][x]['coffee_shop']['coordinates']['longitude']),float(r['Green Line E'][x]['coffee_shop']['coordinates']['latitude'])])
        else:
            centroids = []
            for x in range(len(r['Green Line B'])):
                centroids.append([float(r['Green Line B'][x]['coffee_shop']['coordinates']['longitude']),float(r['Green Line B'][x]['coffee_shop']['coordinates']['latitude'])])

            for x in range(len(r['Green Line C'])):
                centroids.append([float(r['Green Line C'][x]['coffee_shop']['coordinates']['longitude']),float(r['Green Line C'][x]['coffee_shop']['coordinates']['latitude'])])

            for x in range(len(r['Green Line D'])):
                centroids.append([float(r['Green Line D'][x]['coffee_shop']['coordinates']['longitude']),float(r['Green Line D'][x]['coffee_shop']['coordinates']['latitude'])])

            for x in range(len(r['Green Line E'])):
                centroids.append([float(r['Green Line E'][x]['coffee_shop']['coordinates']['longitude']),float(r['Green Line E'][x]['coffee_shop']['coordinates']['latitude'])])

        centroids = np.array(centroids)

        #obtaining all the longitude and latitudes of mbta stops
        url2 = "http://datamechanics.io/data/final_mbta.csv"
        r2 = pd.read_csv(url2)
        mbta_stops = r2.to_dict('records')

        data = []
        weighted_data = []

        #weight by arrival times to have clustering gravitate towards more busy train stops
        def getWeight(arrivalCount):
            return round(arrivalCount/100)

        for i in range(len(mbta_stops)):
            data.append([float(mbta_stops[i]['LATITUDE']), float(mbta_stops[i]['LONGITUDE'])])

        for i in range(len(mbta_stops)):
            if float(int(mbta_stops[i]['ARRIVAL_COUNT']) > 100):
                w_value = getWeight(int(mbta_stops[i]['ARRIVAL_COUNT']))
                for x in range(w_value):
                    weighted_data.append([float(mbta_stops[i]['LATITUDE']), float(mbta_stops[i]['LONGITUDE'])])
            else:
                weighted_data.append([float(mbta_stops[i]['LATITUDE']), float(mbta_stops[i]['LONGITUDE'])])


        weighted_data=np.array(weighted_data)
        data=np.array(data)
        kmeans = KMeans(n_clusters=len(centroids), init=centroids)
        kmeans.fit(weighted_data)

        clusters = []
        for i in range(len(kmeans.cluster_centers_)):
            n = {}
            n['latitude'] = kmeans.cluster_centers_[i][0]
            n['longitude'] = kmeans.cluster_centers_[i][1]
            clusters.append(n)

        repo.dropCollection("ctrinh_fat60221_veeyn.kMeans")
        repo.createCollection("ctrinh_fat60221_veeyn.kMeans")
        for x in range(len(clusters)):
            repo['ctrinh_fat60221_veeyn.kMeans'].insert_one(clusters[x])
        repo['ctrinh_fat60221_veeyn.kMeans'].metadata({'complete':True})
    

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
        repo.authenticate('ctrinh_fat60221_veeyn', 'ctrinh_fat60221_veeyn')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        

        this_script = doc.agent('alg:ctrinh_fat60221_veeyn#getKMeans', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('alg:ctrinh_fat60221_veeyn#getKMeans', {'prov:label':'K-Means', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'py'})
        get_KMeans = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_KMeans, this_script)
        doc.usage(get_KMeans, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )

        getKMeans = doc.entity('dat:ctrinh_fat60221_veeyn#getKMeans', {prov.model.PROV_LABEL:'Optimized Coffee Shop Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(getKMeans, this_script)
        doc.wasGeneratedBy(getKMeans, get_KMeans, endTime)
        doc.wasDerivedFrom(getKMeans, resource, get_KMeans, get_KMeans, get_KMeans)

        repo.logout()

        return doc


# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
# getKMeans.execute()
# doc = getKMeans.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof