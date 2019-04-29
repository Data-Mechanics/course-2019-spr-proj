import dml
from pymongo import MongoClient
import prov.model
import datetime
import json
import uuid
import requests
import xmltodict
import csv
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import seaborn as sns; sns.set()
import numpy as np

class kmeans(dml.Algorithm):
    contributor = 'ekmak_gzhou_kaylaipp_shen99'
    reads = ['ekmak_gzhou_kaylaipp_shen99.zillow_property_data', 'ekmak_gzhou_kaylaipp_shen99.zillow_getsearchresults_data']
    writes = ['ekmak_gzhou_kaylaipp_shen99.type_amount']

    @staticmethod 
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ekmak_gzhou_kaylaipp_shen99','ekmak_gzhou_kaylaipp_shen99')
        collection = db.ekmak_gzhou_kaylaipp_shen99.type_amount
        df = pd.DataFrame(list(collection.find()))

    
        kmeans = KMeans(n_clusters=4)
        kmeans.fit(df)
        y_kmeans = kmeans.predict(df)

        


        plt.scatter(df[:1], c=y_kmeans, s=50, cmap='virdis')

        centers = kmeans.cluster_centers_

        plt.scatter(centers[:, 0], centers[:, 1], c='black', s=200, alpha=0.5);
    

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
        repo.authenticate('ekmak_gzhou_kaylaipp_shen99','ekmak_gzhou_kaylaipp_shen99')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_agent = doc.agent('alg:ekmak_gzhou_kaylaipp_shen99#type_amount',
		                      	{prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        this_entity = doc.entity('dat:ekmak_gzhou_kaylaipp_shen99#type_amount',
                            {prov.model.PROV_LABEL: 'Type and Amount', prov.model.PROV_TYPE: 'ont:DataSet'})

        property_data_resource = doc.entity('dat:ekmak_gzhou_kaylaipp_shen99#zillow_property_data',
		                  {prov.model.PROV_LABEL: 'Property Data', prov.model.PROV_TYPE: 'ont:DataSet'})

        getsearchresult_resource = doc.entity('dat:ekmak_gzhou_kaylaipp_shen99#zillow_getsearchresults_data',
		                  {prov.model.PROV_LABEL: 'getSearchResults', prov.model.PROV_TYPE: 'ont:DataSet'})

        get_type_amount = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.usage(get_type_amount, property_data_resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_type_amount, getsearchresult_resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        doc.wasAssociatedWith(get_type_amount, this_agent)

        doc.wasAttributedTo(this_entity, this_agent)

        doc.wasGeneratedBy(this_entity, get_type_amount, endTime)

        doc.wasDerivedFrom(this_entity, property_data_resource, getsearchresult_resource, get_type_amount, get_type_amount, get_type_amount)

        repo.logout()
                  
        return doc

def map(f, R):
    return [t for (k,v,s) in R for t in f(k,v)]
    
def reduce(f, R):
    keys = {k for (k,v) in R}
    return [f(k1, [v for (k2,v) in R if k1 == k2]) for k1 in keys]
def project(R, p):
    return [p(t) for t in R]
def product(R, S):
    return [(t,u) for t in R for u in S]
def select(R, s):
    return [t for t in R if s(t)]


'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

# type_amount.execute()
# print('generating num houses per street provenance...')
# print('')
# doc = type_amount.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

        







