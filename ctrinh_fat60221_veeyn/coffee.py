import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd

import json
auth = json.load(open('../auth.json', 'r'))


import yelpfusion as yf
API_KEY= auth['services']['yelpfusionportal']['key']

class coffee(dml.Algorithm):
    contributor = 'ctrinh_fat60221_veeyn'
    reads = []
    writes = ['ctrinh_fat60221_veeyn.coffee']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ctrinh_fat60221_veeyn', 'ctrinh_fat60221_veeyn')

        r = {}

        res = yf.search(API_KEY, "coffee shop", "Boston, MA")

        # print(res)

        for num in range(len(res['businesses'])):
            name = res['businesses'][num]['name'].replace(".", "")
            r[name] = res['businesses'][num]['coordinates']

        # r = r.to_dict(orient='records')
        # r = json.loads(r)
        # print(r)
        # r = json.dumps(r, sort_keys=True, indent=2)
        # r = dict(r)
        # print(type(r))
        r = [r]

        repo.dropCollection("coffee")
        repo.createCollection("coffee")
        repo['ctrinh_fat60221_veeyn.coffee'].insert_many(r)
        repo['ctrinh_fat60221_veeyn.coffee'].metadata({'complete':True})
        print(repo['ctrinh_fat60221_veeyn.coffee'].metadata())

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
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('ylp', 'https://api.yelp.com/')

        this_script = doc.agent('alg:ctrinh_fat60221_veeyn#coffee', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('ylp:v3/businesses/search', {'prov:label':'Yelp Fusion', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_coffee = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_coffee, this_script)
        doc.usage(get_coffee, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )

        coffee = doc.entity('dat:ctrinh_fat60221_veeyn#coffee', {prov.model.PROV_LABEL:'Coffee Shop Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(coffee, this_script)
        doc.wasGeneratedBy(coffee, get_coffee, endTime)
        doc.wasDerivedFrom(coffee, resource, get_coffee, get_coffee, get_coffee)

        repo.logout()

        return doc


# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
coffee.execute()
doc = coffee.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof
