import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from scipy.cluster.vq import kmeans as km
from math import *


class kmeans(dml.Algorithm):
    contributor = 'kgrewal_shin2'
    reads = ['kgrewal_shin2.unclaimed_streets']
    writes = ['kgrewal_shin2.street_kmeans']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kgrewal_shin2', 'kgrewal_shin2')

        repo.dropCollection('street_kmeans')
        repo.createCollection('street_kmeans')

        street_info = repo.kgrewal_shin2.unclaimed_streets.find()

        K = []
        for street in street_info:
            lat = street['LAT']
            long = street['LNG']
            K.append((lat, long))

        mean, labels = km(K, 7)

        means = []
        for m in mean:
            means.append({'lat': m[0], 'lon': m[1]})

        mean_counts = []
        for m in mean:
            mean_counts.append({'lat': m[0], 'lon': m[1], 'count': 0})

        street_info = repo.kgrewal_shin2.unclaimed_streets.find()
        for s in street_info:
            val = {'lat': s['LAT'], 'lon': s['LNG']}
            m2 = kmeans.closest(means, val)

            for m in mean_counts:
                if m['lat'] == m2['lat'] and m['lon'] == m2['lon']:
                    m['count'] += 1

        print(mean_counts)

        repo['kgrewal_shin2.street_kmeans'].insert_many(mean_counts)
        repo['kgrewal_shin2.street_kmeans'].metadata({'complete': True})
        print(repo['kgrewal_shin2.street_kmeans'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kgrewal_shin2', 'kgrewal_shin2')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:kgrewal_shin2#transformation4',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8w-nujj',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_streets = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_streets, this_script)
        doc.usage(get_streets, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Street+Kmeans&$select=lat,lon,count'
                   }
                  )

        streets = doc.entity('dat:kgrewal_shin2#kmeans_streets',
                             {prov.model.PROV_LABEL: 'Street Kmeans', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(streets, this_script)
        doc.wasGeneratedBy(streets, get_streets, endTime)
        doc.wasDerivedFrom(streets, resource, get_streets, get_streets, get_streets)

        repo.logout()

        return doc

    # Taken from https://stackoverflow.com/questions/41336756/find-the-closest-latitude-and-longitude
    @staticmethod
    def distance(lat1, lon1, lat2, lon2):
        p = 0.017453292519943295
        a = 0.5 - cos((lat2 - lat1) * p) / 2 + cos(lat1 * p) * cos(lat2 * p) * (1 - cos((lon2 - lon1) * p)) / 2
        return 12742 * asin(sqrt(a))

    @staticmethod
    def closest(data, v):
        return min(data, key=lambda p: kmeans.distance(v['lat'], v['lon'], p['lat'], p['lon']))


kmeans.execute()
doc = kmeans.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
