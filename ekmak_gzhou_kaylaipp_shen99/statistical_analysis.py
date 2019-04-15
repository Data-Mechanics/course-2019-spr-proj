import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import zillow 
import requests
import xmltodict
import csv
from tqdm import tqdm
import numpy as np
from sklearn.metrics import matthews_corrcoef
from scipy import stats
from tqdm import tqdm

class statistical_analysis(dml.Algorithm):

    contributor = 'ekmak_gzhou_kaylaipp_shen99'
    reads = ['ekmak_gzhou_kaylaipp_shen99.price_clusters']
    writes = ['ekmak_gzhou_kaylaipp_shen99.statistical_analysis']

    @staticmethod
    def execute(trial = True):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ekmak_gzhou_kaylaipp_shen99','ekmak_gzhou_kaylaipp_shen99')

        # zillow_data = repo.ekmak_gzhou_kaylaipp_shen99.zillow_getsearchresults_data.find()
        # accessing_data = repo.ekmak_gzhou_kaylaipp_shen99.accessing_data.find()
        price_clusters = repo.ekmak_gzhou_kaylaipp_shen99.price_clusters.find()

        print('')
        print('inserting statistical analysis...')

        # clear database 
        repo.dropCollection("statistical_analysis")
        repo.createCollection("statistical_analysis")

        # calculate mean, std and varience for properties in each cluster 
        for info in tqdm(price_clusters): 
            cluster = info['cluster_num']
            prices = info['prices']
            prices = np.array(prices)
            mean = prices.mean()
            std = prices.std()
            varience = prices.var()
            min_price = prices.min()
            max_price = prices.max()
            repo['ekmak_gzhou_kaylaipp_shen99.statistical_analysis'].insert_one({'cluster_num':cluster, 'mean':mean, 'std':std, 'varience': varience, 'min_price':min_price, 'max_price':max_price})

        repo.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ekmak_gzhou_kaylaipp_shen99','ekmak_gzhou_kaylaipp_shen99')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_agent = doc.agent('alg:ekmak_gzhou_kaylaipp_shen99#statistical_analysis',
		                      	{prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        this_entity = doc.entity('dat:ekmak_gzhou_kaylaipp_shen99#statistical_analysis',
                            {prov.model.PROV_LABEL: 'Statistical Analysis', prov.model.PROV_TYPE: 'ont:DataSet'})

        # zillow_resource = doc.entity('dat:ekmak_gzhou_kaylaipp_shen99#zillow_getsearchresults_data',
		#                   {prov.model.PROV_LABEL: 'Zillow Search Results Data', prov.model.PROV_TYPE: 'ont:DataSet'})

        clusters_resource = doc.entity('dat:ekmak_gzhou_kaylaipp_shen99#price_clusters',
		                  {prov.model.PROV_LABEL: 'K Means cluster data', prov.model.PROV_TYPE: 'ont:DataSet'})

        # accessing_resource = doc.entity('dat:ekmak_gzhou_kaylaipp_shen99#accessing_data',
		#                   {prov.model.PROV_LABEL: 'Accessing Data', prov.model.PROV_TYPE: 'ont:DataSet'})

        get_statisical_analysis = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        # doc.usage(get_statisical_analysis, zillow_resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        # doc.usage(get_statisical_analysis, accessing_resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_statisical_analysis, clusters_resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        doc.wasAssociatedWith(get_statisical_analysis, this_agent)

        doc.wasAttributedTo(this_entity, this_agent)

        doc.wasGeneratedBy(this_entity, get_statisical_analysis, endTime)

        # doc.wasDerivedFrom(this_entity, zillow_resource, get_statisical_analysis, get_statisical_analysis, get_statisical_analysis)
        doc.wasDerivedFrom(this_entity, clusters_resource, get_statisical_analysis, get_statisical_analysis, get_statisical_analysis)
        # doc.wasDerivedFrom(this_entity, accessing_resource, get_statisical_analysis, get_statisical_analysis, get_statisical_analysis)

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
# statistical_analysis.execute()
# statistical_analysis.provenance()
# print('prov done! ')
## eof
