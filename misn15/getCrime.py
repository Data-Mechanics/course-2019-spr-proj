import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class getCrime(dml.Algorithm):
    contributor = 'misn15'
    reads = []
    writes = ['misn15.crime']

    @staticmethod
    def execute(trial = False):
        '''Retrieve crime data for city of Boston'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('misn15', 'misn15')

        url = 'https://data.boston.gov/api/3/action/datastore_search_sql?sql=SELECT%20*%20from%20%2212cb3883-56f5-47de-afa5-3b1cf61b257b%22b&q=2016'
        response = urllib.request.urlopen(url)
        crime = json.load(response)
        r = crime['result']['records']
        
        repo.dropCollection("crime")
        repo.createCollection("crime")
        repo['misn15.crime'].insert_many(r)
        repo['misn15.crime'].metadata({'complete':True})
        print(repo['misn15.crime'].metadata())

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
        doc.add_namespace('bdp', 'https://data.boston.gov/api/3/action/')

        this_script = doc.agent('alg:getCrime', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:datastore_search_sql?sql=SELECT%20*%20from%20%2212cb3883-56f5-47de-afa5-3b1cf61b257b%22b&q=2016', {'prov:label':'Boston Crime', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(this_run, this_script)
        doc.usage(this_run, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query': 'sql?sql=SELECT%20*%20from%20%2212cb3883-56f5-47de-afa5-3b1cf61b257b%22b&q=2016'
                  }
                  )
        resource2 = doc.entity('dat:crime', {prov.model.PROV_LABEL:'Boston Crime', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(resource2, this_script)
        doc.wasGeneratedBy(resource2, this_run, endTime)
        doc.wasDerivedFrom(resource2, resource, this_run, this_run, this_run)
                  
        return doc

getCrime.execute()
doc = getCrime.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof
