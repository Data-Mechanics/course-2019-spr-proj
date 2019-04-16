import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class getHealth(dml.Algorithm):
    contributor = 'misn15'
    reads = []
    writes = ['misn15.health']

    @staticmethod
    def execute(trial = False):
        '''Retrieve health data from datamechanics.io'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('misn15', 'misn15')

        url = 'https://chronicdata.cdc.gov/resource/csmm-fdhi.json?cityname=Boston'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)      
        repo.dropCollection("health")
        repo.createCollection("health")
        repo['misn15.health'].insert_many(r)
        repo['misn15.health'].metadata({'complete':True})
        print(repo['misn15.health'].metadata())

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
        doc.add_namespace('bdp', 'https://chronicdata.cdc.gov/resource/')

        this_script = doc.agent('alg:getHealth', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:csmm-fdhi.json?cityname=Boston', {'prov:label':'Boston_health', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(this_run, this_script)
        doc.usage(this_run, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?cityname=Boston'
                  }
                  )

        resource2 = doc.entity('dat:health', {prov.model.PROV_LABEL:'Boston Health', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(resource2, this_script)
        doc.wasGeneratedBy(resource2, this_run, endTime)
        doc.wasDerivedFrom(resource2, resource, this_run, this_run, this_run)
                  
        return doc

getHealth.execute()
doc = getHealth.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof
