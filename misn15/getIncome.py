import json
import dml
import prov.model
import datetime
import uuid
from census import Census
from us import states

class getIncome(dml.Algorithm):
    contributor = 'misn15'
    reads = []
    writes = ['misn15.income', 'misn15.population']

    @staticmethod
    def execute(trial = False):
        '''Retrieve income and population data from Census Bureau'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('misn15', 'misn15')

        c = Census("a839d0b0a206355591f27266b5205596d1bae45c", year=2017)
        income = c.acs5.state_county_tract('B06011_001E', states.MA.fips, '025', Census.ALL)
        population = c.acs5.state_county_tract('B01003_001E', states.MA.fips, '025', Census.ALL)

        repo.dropCollection("population")
        repo.createCollection("population")
        repo['misn15.population'].insert_many(population)
        repo['misn15.population'].metadata({'complete': True})
        print(repo['misn15.population'].metadata())

        repo.dropCollection("income")
        repo.createCollection("income")
        repo['misn15.income'].insert_many(income)
        repo['misn15.income'].metadata({'complete':True})
        print(repo['misn15.income'].metadata())

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
        doc.add_namespace('bdp', 'https://api.census.gov/data/')

        this_script = doc.agent('alg:getIncome', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:B06011_001E', {'prov:label':'Income for Each FIPS Code', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(this_run, this_script)
        doc.usage(this_run, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                   }
                  )
        resource2 = doc.entity('dat:income', {prov.model.PROV_LABEL:'Boston Income', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(resource2, this_script)
        doc.wasGeneratedBy(resource2, this_run, endTime)
        doc.wasDerivedFrom(resource2, resource, this_run, this_run, this_run)
                  
        return doc

getIncome.execute()
doc = getIncome.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof
