#Get distance of two coffee shops from the closest station
# approximate radius of earth in km
import argparse
import json
import pprint
import requests
import sys
import urllib
import uuid
import csv
import dml
import prov.model
import datetime

#get all distance from t station to corresponding coffee shops

def findMean():
	url = 'http://datamechanics.io/data/min_dist_final.json'
	response = urllib.request.urlopen(url).read().decode("utf-8")
	r = json.loads(response)
	lines = ['Green Line B', 'Green Line C', 'Green Line D', 'Green Line E']
	total_dist = 0
	count = 0 
	for line in lines:
		#looping through all green lines
		stations = r[line]
		for station in stations:
			total_dist += station['coffee_shop']['dist']
			count += 1
	return total_dist/count



class getMean(dml.Algorithm):
    contributor = 'ctrinh_fat60221_veeyn'
    reads = []
    writes = ['ctrinh_fat60221_veeyn.getMean']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ctrinh_fat60221_veeyn', 'ctrinh_fat60221_veeyn')
        mean = findMean()


        #Create id_name list


        repo.dropCollection("getMean")
        repo.createCollection("getMean")
        repo['ctrinh_fat60221_veeyn.getMean'].insert_many([{'MEAN':mean}])
        repo['ctrinh_fat60221_veeyn.getMean'].metadata({'complete':True})
        print(repo['ctrinh_fat60221_veeyn.getMean'].metadata())

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
        doc.add_namespace('dat', 'http://datamechanics.io/data/min_dist.csv') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.


        this_script = doc.agent('alg:ctrinh_fat60221_veeyn#getMean', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:min_dist.csv', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_mean = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_mean, this_script)
        doc.usage(get_mean, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )

        mean = doc.entity('dat:ctrinh_fat60221_veeyn#getMean', {prov.model.PROV_LABEL:'Mean Distance between All Station/Cafe Pairs', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(mean, this_script)
        doc.wasGeneratedBy(mean, get_mean, endTime)
        doc.wasDerivedFrom(mean, resource, get_mean, get_mean, get_mean)

        repo.logout()

        return doc


# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.

getMean.execute()
# doc = getMean.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))



