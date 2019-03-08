import urllib.request
import json
import csv
import dml
import prov.model
import datetime
import uuid
from io import StringIO
from collections import OrderedDict

class getData (dml.Algorithm):
    contributor = 'dixyTW_veeyn'
    reads = []
    writes = ['dixyTW_veeyn.bostonEthnicities']
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('dixyTW_veeyn', 'dixyTW_veeyn')
        
        #for the data set bostonEthnicities
        url = 'http://datamechanics.io/data/dixyTW_veeyn/race_ethnicity.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("bostonEthnicities")
        repo.createCollection("bostonEthnicities")
        repo['dixyTW_veeyn.bostonEthnicities'].insert_many(r)
        repo['dixyTW_veeyn.bostonEthnicities'].metadata({'complete':True})
  
        #print(repo['dixyTW_veeyn.bostonEthnicities'].metadata())


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
        repo.authenticate('dixyTW_veeyn', 'dixyTW_veeyn')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:dixyTW_veeyn#getData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_data, this_script)

        doc.usage(get_data, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )

        ethnicity = doc.entity('dat:dixyTW_veeyn#ethnicity', {prov.model.PROV_LABEL:'ethnicity', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(ethnicity, this_script)
        doc.wasGeneratedBy(ethnicity, get_data, endTime)
        doc.wasDerivedFrom(ethnicity, resource, get_data, get_data, get_data)


        repo.logout()
                  
        return doc


# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
getData.execute()
#doc = example.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof
