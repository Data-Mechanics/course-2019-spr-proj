import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import matplotlib.pyplot as plt
import numpy as np
from sklearn.cluster import KMeans
import folium

def project(R, p):
    return [p(t) for t in R]

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]

def select(R, s):
    return [t for t in R if s(t)]

def not_null(r):
    return (r[0] != None and r[1] != None) and ((r[0]) != '-1' and (r[1] != '-1'))

class crimeLongLat(dml.Algorithm):
    contributor = 'jkmoy_mfflynn'
    reads = ['jkmoy_mfflynn.crime']
    writes = ['jkmoy_mfflynn.crimeLongLat']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jkmoy_mfflynn', 'jkmoy_mfflynn')

        # transformation here
        collection = list(repo['jkmoy_mfflynn.crime'].find())

        data1 = [(doc['Lat'], doc['Long'], doc['OFFENSE_CODE_GROUP']) for doc in collection]
        data2 = select(data1, not_null)
        
        if (trial):
            data2 = data2[0:100]

        #data = data[0:20]
        
        types = [i[2] for i in data2]
        #data = [(i[0],i[1]) for i in data2]
        data = [(i[0],i[1]) for i in data2 if i[0] != '' and i[1] != '']
        data = select(data, lambda t: float(t[0]) != -1.0)
        '''
        for x in data:
            print(x)
            if float(x[0]) == float(-1) or float(x[1]) == float(-1):
                break
        '''
        cluster_number = 23 #number of offical neighborhoods in boston
        kmeans = KMeans(n_clusters=cluster_number)
        kmeans.fit(data)

        means = []
        

        for i in range(cluster_number):
            mean = [[kmeans.cluster_centers_[i][0], kmeans.cluster_centers_[i][1]]]
            mean_helper = []
            types_crime = []
            for j in range(len(kmeans.labels_)):
                if kmeans.labels_[j] == i:
                    mean_helper.append([(data[j][0],data[j][1])])
                    types_crime.append(types[j])
            mean.append(mean_helper)
            mean.append(len(mean_helper))
            mean.append(types_crime)

            means.append(mean)
            

        # NEW DATASET HAS PROBLEMS WHERE
        # NO LOCATION IS DEFAULT to ('-1.00', '-1.00') COORDINATES
        final_dataset = [{'mean':tup[0], 'mean_points':tup[1], 'types':tup[3], 'num_points':tup[2]} for tup in means]
        for x in final_dataset:
            mp = x['mean_points']
            x['mean_points'] = [[float(x), float(y)] for [(x, y)] in mp]
        '''
        for y in final_dataset:
            for z in y['mean_points'][:50]:
                continue
            break
        '''

        
        repo.dropCollection('jkmoy_mfflynn.crimeLongLat')
        repo.createCollection('jkmoy_mfflynn.crimeLongLat')
        
        repo['jkmoy_mfflynn.crimeLongLat'].insert_many(final_dataset)
        repo['jkmoy_mfflynn.crimeLongLat'].metadata({'complete': True})

        print(repo['jkmoy_mfflynn.crimeLongLat'].metadata())

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
        repo.authenticate('jkmoy_mfflynn', 'jkmoy_mfflynn')
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/jkmoy_mfflynn') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/jkmoy_mfflynn') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:jkmoy_mfflynn#crimeLongLat', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:meanLocation', {'prov:label':'Means and location of crime', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_mean = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_mean, this_script)
        doc.usage(get_mean, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})

        cMean = doc.entity('dat:jkmoy_mfflynn#means', {prov.model.PROV_LABEL:'Means and location %', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(cMean, this_script)
        doc.wasGeneratedBy(cMean, get_mean, endTime)
        doc.wasDerivedFrom(cMean, resource, get_mean, get_mean, get_mean)

        repo.logout()
                  
        return doc

'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
averagePerDepartment.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
#crimeLongLat.execute()
## eof
