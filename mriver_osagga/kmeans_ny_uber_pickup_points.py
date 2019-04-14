import urllib.request
import json
import csv
import dml
import prov.model
import datetime
import uuid
from io import StringIO
from sklearn.cluster import KMeans
import numpy as np
import matplotlib.pyplot as plt

class kmeans_ny_uber_pickup_points(dml.Algorithm):
    contributor = 'mriver_osagga'
    reads = ['mriver_osagga.ny_uber_data_clean']
    writes = ['mriver_osagga.optimal_ny_uber_pickup_points']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('mriver_osagga', 'mriver_osagga')

        data = list(repo['mriver_osagga.ny_uber_data_clean'].find())
        
        # Projection to create an in-memory list with the pickup points
        pickup_p = np.array([ ( float(val['pickup_point'][0]), float(val['pickup_point'][1])) for val in data])

        k = 50
        kmeans = KMeans(n_clusters=k, random_state=0).fit(pickup_p)
        y_predict = kmeans.predict(pickup_p)

        plt.scatter(pickup_p[:, 0], pickup_p[:, 1], c=y_predict, s=50, cmap='viridis')
        centers = kmeans.cluster_centers_
        plt.scatter(centers[:, 0], centers[:, 1], c='black', s=200, alpha=0.5);
        plt.show()
        
        kmeans = np.ndarray.tolist(kmeans.cluster_centers_)
        r_p = [{"mean_{}".format(i): kmeans[i] } for i in range(k)]
        r = json.loads(json.dumps(r_p))

        repo.dropCollection("optimal_ny_uber_pickup_points")
        repo.createCollection("optimal_ny_uber_pickup_points")
        repo['mriver_osagga.optimal_ny_uber_pickup_points'].insert_many(r)
        repo['mriver_osagga.optimal_ny_uber_pickup_points'].metadata(
            {'complete': True})
        print(repo['mriver_osagga.optimal_ny_uber_pickup_points'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('mriver_osagga', 'mriver_osagga')
        # The data sets are in <user>#<collection> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')

        this_script = doc.agent('alg:mriver_osagga#kmeans_ny_uber_pickup_points', {
                                prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        
        resource = doc.entity('dat:ny_uber_data_clean', {
                              'prov:label': 'Uber trip pickup points (June, 2014)', prov.model.PROV_TYPE: 'ont:DataSet'})
        
        kmeans_ny_uber_pickup = doc.activity(
            'log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(kmeans_ny_uber_pickup, this_script)
        doc.usage(kmeans_ny_uber_pickup, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Uber+trip+data&$select=name'
                   }
                  )

        optimal_ny_uber_pickup_points = doc.entity('dat:mriver_osagga#optimal_ny_uber_pickup_points', {
                                             prov.model.PROV_LABEL: 'Uber Trip Optimal Pickup Locations', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(optimal_ny_uber_pickup_points, this_script)
        doc.wasGeneratedBy(optimal_ny_uber_pickup_points,
                           kmeans_ny_uber_pickup, endTime)
        doc.wasDerivedFrom(kmeans_ny_uber_pickup, resource,
                           optimal_ny_uber_pickup_points, optimal_ny_uber_pickup_points, optimal_ny_uber_pickup_points)

        repo.logout()

        return doc


# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
# kmeans_ny_uber_pickup_points.execute()
# doc = clean_bos_neighborhoods.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))
# eof
