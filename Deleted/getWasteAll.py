import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd


class getWasteAll(dml.Algorithm):
    contributor = 'misn15'
    reads = []
    writes = ['misn15.waste_all']

    @staticmethod
    def execute(trial = False):
        '''Retrieve waste data from datamechanics.io'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('misn15', 'misn15')

        # insert data about waste sites and waste generators from four sources
        # source one:
        url = 'http://datamechanics.io/data/hwgenids.json' 
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("waste_all")
        repo.createCollection("waste_all")
        repo['misn15.waste_all'].insert_many(r)

        # source two:
        url = 'http://datamechanics.io/data/misn15/master_waste/AUL_PT.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo['misn15.waste_all'].insert_many(r['features'])

        # source three:
        url = 'http://datamechanics.io/data/misn15/master_waste/c21e.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo['misn15.waste_all'].insert_many(r['features'])

        # source four:
        url = 'http://datamechanics.io/data/misn15/master_waste/solid2.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo['misn15.waste_all'].insert_many(r['features'])

        repo['misn15.waste_all'].metadata({'complete': True})
        print(repo['misn15.waste_all'].metadata())

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
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('waste', 'https://www.mass.gov/doc/list-of-massachusetts-hazardous-waste-generators-january-15-2019')

        this_script = doc.agent('alg:misn15#getWaste', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('waste:Boston_waste', {'prov:label':'Boston_waste', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_waste = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_waste, this_script)
        doc.usage(get_waste, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        waste = doc.entity('dat:misn15#waste', {prov.model.PROV_LABEL:'Boston Waste', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(waste, this_script)
        doc.wasGeneratedBy(waste, get_waste, endTime)
        doc.wasDerivedFrom(waste, resource, get_waste, get_waste, get_waste)
                  
        return doc

getWasteAll.execute()
doc = getWasteAll.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
