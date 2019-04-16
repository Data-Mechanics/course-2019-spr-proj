import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


class getWasteAll(dml.Algorithm):
    contributor = 'misn15'
    reads = []
    writes = ['misn15.hwgen', 'misn15.aul', 'misn15.waste']

    @staticmethod
    def execute(trial = False):
        '''Retrieve waste data from datamechanics.io'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('misn15', 'misn15')

        # source one
        url = 'http://datamechanics.io/data/hwgenids.json' 
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("hwgen")
        repo.createCollection("hwgen")
        repo['misn15.hwgen'].insert_many(r)
        repo['misn15.hwgen'].metadata({'complete':True})
        print(repo['misn15.hwgen'].metadata())

        # source two
        url = 'http://datamechanics.io/data/misn15/master_waste/AUL_PT.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("aul")
        repo.createCollection("aul")
        repo['misn15.aul'].insert_many(r['features'])
        repo['misn15.aul'].metadata({'complete':True})
        print(repo['misn15.aul'].metadata())

        # source three
        url = 'http://datamechanics.io/data/misn15/master_waste/c21e.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("waste")
        repo.createCollection("waste")
        repo['misn15.waste'].insert_many(r['features'])
        repo['misn15.waste'].metadata({'complete':True})
        print(repo['misn15.waste'].metadata())

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
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/misn15/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/misn15/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'http://datamechanics.io/data/')
        doc.add_namespace('bdp2', 'http://datamechanics.io/data/misn15/master_waste/')
        doc.add_namespace('bdp3', 'http://datamechanics.io/data/misn15/master_waste/')

        this_script = doc.agent('alg:getWasteAll', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:hwgenids', {'prov:label':'Boston_waste', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource2 = doc.entity('bdp2:AUL_PT', {'prov:label': 'Boston_waste', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'geojson'})
        resource3 = doc.entity('bdp3:c21e', {'prov:label': 'Boston_waste', prov.model.PROV_TYPE: 'ont:DataResource','ont:Extension': 'geojson'})
        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(this_run, this_script)
        doc.usage(this_run, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        doc.usage(this_run, resource2, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'
                   }
                  )
        doc.usage(this_run, resource3, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'
                   }
                  )

        resource4 = doc.entity('dat:hwgen', {prov.model.PROV_LABEL:'Boston Hazardous Waste/Oil', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(resource4, this_script)
        doc.wasGeneratedBy(resource4, this_run, endTime)
        doc.wasDerivedFrom(resource4, resource, this_run, this_run, this_run)

        resource5 = doc.entity('dat:aul', {prov.model.PROV_LABEL: 'Boston Waste with Limited Use', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(resource5, this_script)
        doc.wasGeneratedBy(resource5, this_run, endTime)
        doc.wasDerivedFrom(resource5, resource2, this_run, this_run, this_run)

        resource6 = doc.entity('dat:waste', {prov.model.PROV_LABEL: 'Boston Hazardous Waste', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(resource6, this_script)
        doc.wasGeneratedBy(resource6, this_run, endTime)
        doc.wasDerivedFrom(resource6, resource3, this_run, this_run, this_run)
                  
        return doc

getWasteAll.execute()
doc = getWasteAll.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof
