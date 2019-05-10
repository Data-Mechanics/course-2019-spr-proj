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
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://api.census.gov/data/')

        this_script = doc.agent('alg:misn15#getIncome', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:B06011_001E', {'prov:label':'Income for Each FIPS Code', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource2 = doc.entity('bdp:B01003_001E', {'prov:label': 'Population for Each FIPS Code', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})

        get_income = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_population = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_income, this_script)
        doc.wasAssociatedWith(get_population, this_script)
        doc.usage(get_income, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                   }
                  )
        doc.usage(get_population, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                   }
                  )
        income = doc.entity('dat:misn15#income', {prov.model.PROV_LABEL:'Boston Income', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(income, this_script)
        doc.wasGeneratedBy(income, get_income, endTime)
        doc.wasDerivedFrom(income, resource, get_income, get_income, get_income)

        population = doc.entity('dat:misn15#population', {prov.model.PROV_LABEL: 'Population for Boston Census Tracts', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(population, this_script)
        doc.wasGeneratedBy(population, get_income, endTime)
        doc.wasDerivedFrom(population, resource2, get_population, get_population, get_population)

        return doc

# getIncome.execute()
# doc = getIncome.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof
