import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class mbta(dml.Algorithm):
    contributor = 'ctrinh'
    reads = []
    writes = ['ctrinh.mbta']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ctrinh', 'ctrinh')

        url = 'https://api-v3.mbta.com/lines'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = [json.loads(response)]
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("mbta")
        repo.createCollection("mbta")
        repo['ctrinh.mbta'].insert_many(r)
        repo['ctrinh.mbta'].metadata({'complete':True})
        print(repo['ctrinh.mbta'].metadata())

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
        repo.authenticate('ctrinh', 'ctrinh')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('mbt', 'https://api-v3.mbta.com/')

        this_script = doc.agent('alg:ctrinh#mbta', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('mbt:lines', {'prov:label':'MBTA Line Document', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_mbta = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_mbta, this_script)
        doc.usage(get_mbta, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )

        mbta = doc.entity('dat:ctrinh#mbta', {prov.model.PROV_LABEL:'MBTA Lines', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(mbta, this_script)
        doc.wasGeneratedBy(mbta, get_mbta, endTime)
        doc.wasDerivedFrom(mbta, resource, get_mbta, get_mbta, get_mbta)

        repo.logout()

        return doc

'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
mbta.execute()
doc = mbta.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

## eof
