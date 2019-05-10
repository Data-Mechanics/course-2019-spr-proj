import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv

# some questions:
# what counts as non trivial transformation
# does execute count?
# what about my current script
# what about vee's 


class getAge(dml.Algorithm):
    contributor = 'dixyTW_veeyn'
    reads = []
    writes = ['dixyTW_veeyn.bostonAges']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('dixyTW_veeyn', 'dixyTW_veeyn')


        url = 'http://datamechanics.io/data/dixyTW_veeyn/age.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("bostonAges")
        repo.createCollection("bostonAges")

        repo['dixyTW_veeyn.bostonAges'].insert_one(r)
        repo['dixyTW_veeyn.bostonAges'].metadata({'complete':True})
        print(repo['dixyTW_veeyn.bostonAges'].metadata())


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

        this_script = doc.agent('alg:dixyTW_veeyn#getAge', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:dixyTW_veeyn/age.json', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_Age = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_Age, this_script)
        doc.usage(get_Age, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )



        bostonAges = doc.entity('dat:dixyTW_veeyn#bostonAges', {prov.model.PROV_LABEL:'Age distribution of Boston Neighbors', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bostonAges, this_script)
        doc.wasGeneratedBy(bostonAges, get_Age, endTime)
        doc.wasDerivedFrom(bostonAges, resource, get_Age, get_Age, get_Age)

        repo.logout()
                  
        return doc

'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.



print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
# example.execute()
# doc = example.provenance()
# print(doc.get_provn())
