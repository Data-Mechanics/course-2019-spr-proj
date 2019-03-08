import urllib.request
import json
import csv
import dml
import prov.model
import datetime
import uuid
from bson.json_util import dumps
from pprint import pprint

class newBostonEthnicities(dml.Algorithm):
    contributor = 'dixyTW_veeyn'
    reads = ['dixyTW_veeyn.bostonEthnicities']
    writes = ['dixyTW_veeyn.newBostonEthnicities']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('dixyTW_veeyn', 'dixyTW_veeyn')

        #this filters out all records in the year of 2010 and projects neighborhood and percent o
        select1 = list(repo['dixyTW_veeyn.bostonEthnicities'].find({ "Decade": 2010 }, {"Neighborhood": 1, "Race and or Ethnicity": 1, "Percent of Population": 1}))
      

        #now, we will sum the total of number of people with key = neighborhood where decade = 2019
        pipeline = [
            {"$match": {"Decade": 2010}},
            {"$group": {"_id": "$Neighborhood", "Total People": {"$sum": "$Number of People"}}}
           
        ]
        aggregate1 = [element for element in list(repo['dixyTW_veeyn.bostonEthnicities'].aggregate(pipeline)) if element['_id'] != "Harbor Islands"]
       #print(aggregate1)

        #now we will append races + percent of population to aggregate transformations
       	for x in range(len(select1)):
       		for y in range(len(aggregate1)):
       			if select1[x]['Neighborhood'] == aggregate1[y]['_id']:
       				select1[x]['Total People'] = aggregate1[y]['Total People']

       	repo.dropCollection("newBostonEthnicities")
        repo.createCollection("newBostonEthnicites")
       	for x in range(len(select1)):
       		repo['dixyTW_veeyn.newBostonEthnicities'].insert_one(select1[x])
       	

       	repo['dixyTW_veeyn.newBostonEthnicities'].metadata({'complete':True})
       

        print(repo['dixyTW_veeyn.newBostonEthnicities'])

        repo.logout()

        endTime = datetime.datetime.now()
        
        return {"start": startTime, "end": endTime}

        #print(json.dumps(json.loads(doc.serialize()), indent=4))

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
        repo.authenticate('alice_bob', 'alice_bob')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:alice_bob#getData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_found, this_script)
        doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_found, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_lost, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        lost = doc.entity('dat:fat60221_veeyn#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(lost, this_script)
        doc.wasGeneratedBy(lost, get_lost, endTime)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        found = doc.entity('dat:fat60221_veeyn#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)
        doc.wasGeneratedBy(found, get_found, endTime)
        doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.logout()
                  
        return doc


# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
newBostonEthnicities.execute()
#doc = example.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof