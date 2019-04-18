import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd

import json

def csv_to_json(url, trial):
    file = urllib.request.urlopen(url).read().decode("utf-8")  # retrieve file from datamechanics.io
    finalJson = []
    entries = file.split('\n')

    # print(entries[0])
    val = entries[0].split('\r')  # retrieve column names for keys
    del val[-1]
    keys = val[0].split(',')
    # print(val)

    if trial == True:
        for row in entries[1:10]:
            values = row.split(',')
            values[-1] = values[-1][:-1]
            dictionary = dict([(keys[i], values[i]) for i in range(len(keys))])
            finalJson.append(dictionary)
    else:
        for row in entries[1:-1]:
            values = row.split(',')
            values[-1] = values[-1][:-1]
            dictionary = dict([(keys[i], values[i]) for i in range(len(keys))])
            finalJson.append(dictionary)

    return finalJson

class getMBTA(dml.Algorithm):
    contributor = 'ctrinh_fat60221_veeyn'
    reads = []
    writes = ['ctrinh_fat60221_veeyn.getMBTA']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ctrinh_fat60221_veeyn', 'ctrinh_fat60221_veeyn')

        url = 'http://datamechanics.io/data/final_mbta.csv'
        mbtaJSON = csv_to_json(url, trial)

        # print(mbtaJSON[0])


        repo.dropCollection("getMBTA")
        repo.createCollection("getMBTA")
        repo['ctrinh_fat60221_veeyn.getMBTA'].insert_many(mbtaJSON)
        repo['ctrinh_fat60221_veeyn.getMBTA'].metadata({'complete':True})
        print(repo['ctrinh_fat60221_veeyn.getMBTA'].metadata())

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
        doc.add_namespace('dat', 'http://datamechanics.io/data/final_mbta.csv') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.


        this_script = doc.agent('alg:ctrinh_fat60221_veeyn#getMBTA', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:final_mbta.csv', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_mbta = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_mbta, this_script)
        doc.usage(get_mbta, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )

        mbta = doc.entity('dat:ctrinh_fat60221_veeyn#getMBTA', {prov.model.PROV_LABEL:'T Station Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(mbta, this_script)
        doc.wasGeneratedBy(mbta, get_mbta, endTime)
        doc.wasDerivedFrom(mbta, resource, get_mbta, get_mbta, get_mbta)

        repo.logout()

        return doc


# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
# getMBTA.execute()
# doc = getMBTA.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))