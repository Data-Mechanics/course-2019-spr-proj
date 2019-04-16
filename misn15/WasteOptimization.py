import urllib.request
import dml
import json
import prov.model
import datetime
import uuid
import pandas as pd
from sklearn.cluster import DBSCAN
import numpy as np
import requests
from shapely.geometry import MultiPoint

class WasteOptimization(dml.Algorithm):
    contributor = 'misn15'
    reads = ['misn15.waste_all', 'misn15.schools', 'misn15.openSpace_centroids', 'misn15.population']
    writes = ['misn15.waste_optimal']

    @staticmethod
    def execute(trial = False):
        '''K-means algorithm for finding clusters of waste sites'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('misn15', 'misn15')

        # combine address with city name and find coordinates
        waste_all = list(repo['misn15.waste_all'].find())
        schools = list(repo['misn15.schools'].find())
        open_space = list(repo['misn15.openSpace_centroids'].find())
        population = list(repo['misn15.population'].find())

        if trial:
            waste_all = waste_all[0:100]
            schools = schools[0:100]
            open_space = open_space[0:100]

        # Define relational building blocks
        def product(R, S):
            return [(t, u) for t in R for u in S]

        def select(R, s):
            return [t for t in R if s(t)]

        def project(R, p):
            return [p(t) for t in R]

        def euclidian_dist(p, q):
            x1 = p[0]
            y1 = p[1]
            x2 = q[0]
            y2 = q[1]
            return (x1 - x2) ** 2 + (y1 - y2) ** 2

        # get only important columns from waste data set
        waste_list = []
        for x in waste_all:
            waste_list += [[x['Name'], x['Address'], x['Zip Code'], x['Coordinates'], x['Status'], x['FIPS']]]

        # DBSCAN clustering to find clusters of waste sites

        # get just the coordinates
        coords = np.zeros(shape = (len(waste_list), 2))
        for x in range(len(waste_list)):
            coords[x][0] = waste_list[x][3][0]
            coords[x][1] = waste_list[x][3][1]

        # get clusters
        db = DBSCAN(eps=0.00012, min_samples=5, algorithm='ball_tree', metric='haversine').fit(np.radians(coords))
        cluster_labels = db.labels_
        num_clusters = len(set(cluster_labels))

        #get all points in a cluster and compute its centroid
        clusters = pd.Series([coords[cluster_labels==n] for n in range(num_clusters)])
        centroids = []
        for cluster in clusters:
            if len(cluster) != 0:
                centroid = (MultiPoint(cluster).centroid.x, MultiPoint(cluster).centroid.y)
                centroids += [centroid]

        # Constraint Satisfaction problem

        # get list of school coordinates
        schools_list = []
        for x in schools:
            schools_list += [[x['geometry']['coordinates'][0], x['geometry']['coordinates'][1]]]

        # get list of waste coordinates
        waste_list = []
        for x in waste_all:
            waste_list += [x['Coordinates']]

        # get average distance of schools from waste cluster
        schools_dist = []
        for x in centroids:
            total_dist = 0
            distance = 0
            for y in schools_list:
                distance = euclidian_dist(x, y)
                total_dist += distance
            schools_dist += [[x, total_dist/len(schools_list)]]

        # get waste cluster furthest from open spaces
        open_dist = []
        for x in centroids:
            total_dist = 0
            distance = 0
            for y in open_space:
                distance = euclidian_dist(x, y['Coordinates'])
                total_dist += distance
            open_dist += [[x, total_dist/len(open_space)]]

        # combine average distance to open space and to schools for each centroid
        final_dist = product(open_dist, schools_dist)
        final_filtered = select(final_dist, lambda t: t[0][0] == t[1][0])
        final_project = project(final_filtered, lambda t: [(t[0][0][0], t[0][0][1]), t[0][1], t[1][1]])

        # find waste sites where population of that zip code is less than a threshold
        for x in final_project:
            params = urllib.parse.urlencode({'latitude': x[0][1], 'longitude': x[0][0], 'format': 'json'})
            url = 'https://geo.fcc.gov/api/census/block/find?' + params
            response = requests.get(url)
            data = response.json()
            geoid = data['Block']['FIPS'][0:11]
            x += [geoid]

        # Get population for every centroid
        pop_list = []
        for x in population:
            pop_list += [[x['B01003_001E'], x['state']+x['county']+x['tract']]]

        # add population to average distance data
        pop_dist = product(pop_list, final_project)
        final_pop = select(pop_dist, lambda t: t[0][1] == t[1][3])
        final_project = project(final_pop, lambda t: (t[0][1], t[0][0], t[1][0], t[1][1], t[1][2]))

        # sort average distance to schools and open spaces and population
        pts = []
        pts_dist = []
        pts_dist2 = []
        for x in final_project:
            pts += [x[1]]
            pts_dist += [x[3]]
            pts_dist2 += [x[4]]
        pts.sort(reverse=True)
        pts_dist.sort()
        pts_dist2.sort()

        # rank each distance and the population from 1 to 5
        pts_index = []
        pts_dist_index = []
        pts_dist2_index = []
        for i in range(1, len(pts)+1):
            pts_index += [(pts[i-1], i)]
            pts_dist_index += [(pts_dist[i-1], i)]
            pts_dist2_index += [(pts_dist2[i-1], i)]

        # add ranks of distances and the population to each centroid
        pts_product = product(pts_index, final_project)
        pts_select = select(pts_product, lambda t: t[0][0] == t[1][1])
        pts_project = project(pts_select, lambda t: (t[1][0], t[1][1], t[1][2], t[1][3], t[1][4], t[0][1]))

        pts_dist_product = product(pts_dist_index, pts_project)
        pts_dist_select = select(pts_dist_product, lambda t: t[0][0] == t[1][3])
        pts_dist_project = project(pts_dist_select, lambda t: (t[1][0], t[1][1], t[1][2], t[1][3], t[1][4], t[1][5], t[0][1]))

        pts_dist2_product = product(pts_dist2_index, pts_dist_project)
        pts_dist2_select = select(pts_dist2_product, lambda t: t[0][0] == t[1][4])
        pts_dist2_project = project(pts_dist2_select, lambda t: [t[1][0], t[1][1], t[1][2], t[1][3], t[1][4], t[1][5], t[1][6], t[0][1]])

        # sum up all of the ranks for each centroid to determine which centroids are the best
        for x in pts_dist2_project:
            dist_sum = x[-1]+x[-2]+x[-3]
            x += [dist_sum]

        pts_dist2_project.sort(key=lambda j: j[8])

        # Rank the centroids and make into a dictionary
        # 1 is the worst and 5 is the best waste site
        for i in range(1, len(pts_dist2_project)+1):
            pts_index += [(pts[i-1], i)]

        repo.dropCollection("misn15.waste_optimal")
        repo.createCollection("misn15.waste_optimal")

        i = 1
        for x in pts_dist2_project:
            entry = {'Coordinates': x[2], 'Population': x[1], 'Avg Dist to Open Spaces': x[3], 'Avg Dist to Schools': x[4], 'Rank': i}
            i += 1
            repo['misn15.waste_optimal'].insert_one(entry)

        repo['misn15.waste_optimal'].metadata({'complete':True})
        print(repo['misn15.waste_optimal'].metadata())

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
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/misn15/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/misn15/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        
        this_script = doc.agent('alg:WasteOptimization', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:waste_all', {'prov:label':'Boston Waste Sites', prov.model.PROV_TYPE:'ont:DataResource'})
        resource2 = doc.entity('dat:schools', {'prov:label':'All Schools in Boston', prov.model.PROV_TYPE:'ont:DataResource'})
        resource3 = doc.entity('dat:openSpace_centroids', {'prov:label': 'Centroids of Open Spaces in Boston', prov.model.PROV_TYPE: 'ont:DataResource'})
        resource4 = doc.entity('dat:population', {'prov:label': 'Population of Boston FIPS Codes', prov.model.PROV_TYPE: 'ont:DataResource'})
        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(this_run, this_script)

        doc.usage(get_merged, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                   }
                  )
        doc.usage(this_run, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                   }
                  )
        doc.usage(this_run, resource3, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'
                   }
                  )
        doc.usage(this_run, resource4, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'
                   }
                  )
        resource5 = doc.entity('dat:waste_optimal', {prov.model.PROV_LABEL:'Waste Centroids Ranked using Certain Criteria', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(resource5, this_script)
        doc.wasGeneratedBy(resource5, this_run, endTime)
        doc.wasDerivedFrom(resource5, resource, this_run, this_run, this_run)
        doc.wasDerivedFrom(resource5, resource2, this_run, this_run, this_run)
        doc.wasDerivedFrom(resource5, resource3, this_run, this_run, this_run)
        doc.wasDerivedFrom(resource5, resource4, this_run, this_run, this_run)

        return doc

WasteOptimization.execute(trial=True)
doc = WasteOptimization.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof
