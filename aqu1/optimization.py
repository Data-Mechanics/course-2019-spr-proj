import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
from sklearn.cluster import KMeans
import math
import numpy as np

class optimization(dml.Algorithm):
    contributor = 'aqu1'
    reads = ['aqu1.schools_data', 'aqu1.mbta_stops_data']
    writes = ['aqu1.optimization']
    
    def get_centroids(t_stops, all_schools, trial):
        latitude = []
        longitude = []
        loc_schools = all_schools
        loc_schools = pd.DataFrame(list(loc_schools))
        loc_schools['Latitude'] = loc_schools['Latitude'].astype(dtype = np.float64)
        loc_schools['Longitude'] = loc_schools['Longitude'].astype(dtype = np.float64)
        loc_schools = loc_schools.groupby('City').mean()
        
        k = loc_schools['Latitude'].count()
  
        loc_schools = json.loads(loc_schools.to_json(orient = 'records'))

        coord_list = []
        tstops_list = []
        result = []
        
        # Generate list of centroids of average neighborhood coordinates for k-means clustering with train stops
        for row in loc_schools:
            if isinstance(row, str) == False:
                coord_list.append([row['Longitude'], row['Latitude']])

        mean_pts = np.array(coord_list)
        
        # create list of lists of t-stops
        if trial:
            kmeans = KMeans(n_clusters = 5, n_init = 1).fit(t_stops[:10])
        else:
            kmeans = KMeans(n_clusters = k, init = mean_pts, n_init = 1).fit(t_stops)
        
        # Optimized T-stops for closest distance to schools
        centroids = kmeans.cluster_centers_.tolist()
        print("The optimized T-stop coordinates are:", centroids, '\n')
        print("Number of T-stops (k in K-means):", len(centroids), '\n')
        for point in centroids:
           row = {}
           row['centroid'] = point
           result.append(row.copy())
           
        return result
        
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aqu1', 'aqu1')
        
        t_stops = repo.aqu1.mbta_stops_data.find()
        t_stops = pd.DataFrame(t_stops)
        t_stops = t_stops.drop(columns = '_id')
        t_stops = t_stops[['Longitude', 'Latitude']]

        all_schools = repo.aqu1.schools_data.find()
        centroids = optimization.get_centroids(t_stops, all_schools, trial)
        stops_output = {"type": "FeatureCollection", "features": []}
        
        repo.dropCollection("optimization")
        repo.createCollection("optimization")
        repo['aqu1.optimization'].insert_many(centroids)
        
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aqu1', 'aqu1')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('map', 'https://www.google.com/')
        
        this_script = doc.agent('alg:aqu1#optimization', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        # Optimization Report
        resource_optimization = doc.entity('map:maps', {'prov:label':'Optimization', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_centroids = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_centroids, this_script)
        doc.usage(get_centroids, resource_optimization, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
      
        optimization = doc.entity('dat:aqu1#optimization', {prov.model.PROV_LABEL:'T-stops Optimization', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(optimization, this_script)
        doc.wasGeneratedBy(optimization, resource_optimization, endTime)
        doc.wasDerivedFrom(optimization, resource_optimization, get_centroids, get_centroids, get_centroids)
        
        repo.logout()

        return doc
'''
optimization.execute()
doc = optimization.provenance()
#print(type(optimization.provenance()))
print(optimization.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
