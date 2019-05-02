import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class parking(dml.Algorithm):
    contributor = 'ctrinh'
    reads = []
    writes = ['ctrinh.parking']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ctrinh', 'ctrinh')

        url = 'https://data.boston.gov/api/3/action/datastore_search?resource_id=8d38cc9d-8c58-462e-b2df-b793e9c05612&limit=572'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = [json.loads(response)]
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("parking")
        repo.createCollection("parking")
        repo['ctrinh.parking'].insert_many(r)
        repo['ctrinh.parking'].metadata({'complete':True})
        print(repo['ctrinh.parking'].metadata())

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
        doc.add_namespace('dbg', 'https://data.boston.gov/api/3/action/')

        this_script = doc.agent('alg:ctrinh#parking', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dbg:datastore_search', {'prov:label':'Park Boston Monthly 2015', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_parking = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_parking, this_script)
        doc.usage(get_parking, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?resource_id=8d38cc9d-8c58-462e-b2df-b793e9c05612&limit=$'
                  }
                  )

        parking = doc.entity('dat:ctrinh#parking', {prov.model.PROV_LABEL:'Parking Usage', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(parking, this_script)
        doc.wasGeneratedBy(parking, get_parking, endTime)
        doc.wasDerivedFrom(parking, resource, get_parking, get_parking, get_parking)

        repo.logout()

        return doc

'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
parking.execute()
doc = parking.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

## eof
