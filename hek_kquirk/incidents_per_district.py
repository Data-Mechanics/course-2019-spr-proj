import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import bson.code

class incidents_per_district(dml.Algorithm):
    contributor = 'hek_kquirk'
    reads = ['hek_kquirk.boston_crime_incidents','hek_kquirk.district_race', 'hek_kquirk.district_income', 'hek_kquirk.district_education']
    writes = ['hek_kquirk.incidents_per_district']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('hek_kquirk', 'hek_kquirk')

        # Drop/recreate mongo collection
        repo.dropCollection("incidents_per_district")
        repo.createCollection("incidents_per_district")

        mapper = bson.code.Code("""
            function() {
                emit(this.DISTRICT, {'Count':1});
            }
        """)         

        reducer = bson.code.Code("""
            function(k, vs) {
                var sum = 0;

                vs.forEach(function(v,i) {
                    sum += parseFloat(String(v.Count).replace(/[$,\(\)]+/g,""));
                });
                return {'Count':sum};
            }
        """)

        repo['hek_kquirk.boston_crime_incidents'].map_reduce(mapper, reducer, "hek_kquirk.incidents_per_district")

        pipeline = [
            {"$lookup": {"from":"hek_kquirk.district_race", "localField": "_id", "foreignField": "_id", "as": "race_and_ethnicity"}},
            {"$lookup": {"from":"hek_kquirk.district_income", "localField": "_id", "foreignField": "_id", "as": "income_data"}},
            {"$lookup": {"from":"hek_kquirk.district_education", "localField": "_id", "foreignField": "_id", "as": "education_data"}},
            {"$out":"hek_kquirk.incidents_per_district"}
        ]
        repo['hek_kquirk.incidents_per_district'].aggregate(pipeline)

        repo['hek_kquirk.incidents_per_district'].metadata({'complete':True})
        print(repo['hek_kquirk.incidents_per_district'].metadata())

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
        repo.authenticate('hek_kquirk', 'hek_kquirk')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/')

        this_script = doc.agent('alg:hek_kquirk#incidents_per_district', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource_boston_crime_incidents = doc.entity('dat:hek_kquirk#boston_crime_incidents', {'prov:label':'Boston Neighborhoods crime incidents Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_district_race = doc.entity('dat:hek_kquirk#district_race', {'prov:label':'Boston race to Police Districts', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_district_income = doc.entity('dat:hek_kquirk#district_income', {'prov:label':'Boston income to Police Districts', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_district_education = doc.entity('dat:hek_kquirk#district_education', {'prov:label':'Boston education to Police Districts', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_incidents = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_race = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_income = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_education = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_incidents, this_script)
        doc.wasAssociatedWith(get_race, this_script)
        doc.wasAssociatedWith(get_income, this_script)
        doc.wasAssociatedWith(get_education, this_script)
        
        doc.usage(get_incidents, resource_boston_crime_incidents, startTime, None,
                  {
                      prov.model.PROV_TYPE:'ont:Retrieval'
                  }
        )
        doc.usage(get_race, resource_district_race, startTime, None,
                  {
                      prov.model.PROV_TYPE:'ont:Retrieval'
                  }
        )
        doc.usage(get_income, resource_district_income, startTime, None,
                  {
                      prov.model.PROV_TYPE:'ont:Retrieval'
                  }
        )
        doc.usage(get_education, resource_district_education, startTime, None,
                  {
                      prov.model.PROV_TYPE:'ont:Retrieval'
                  }
        )
        
        incidents_per_district = doc.entity('dat:hek_kquirk#incidents_per_district', {prov.model.PROV_LABEL:'Incidents and factors to Police District', prov.model.PROV_TYPE:'ont:DataSet'})
        
        doc.wasAttributedTo(incidents_per_district, this_script)
        
        doc.wasGeneratedBy(incidents_per_district, get_incidents, endTime)
        doc.wasGeneratedBy(incidents_per_district, get_race, endTime)
        doc.wasGeneratedBy(incidents_per_district, get_income, endTime)
        doc.wasGeneratedBy(incidents_per_district, get_education, endTime)
        
        doc.wasDerivedFrom(incidents_per_district, resource_boston_crime_incidents, get_incidents, get_incidents, get_incidents)
        doc.wasDerivedFrom(incidents_per_district, resource_district_race, get_race, get_race, get_race)
        doc.wasDerivedFrom(incidents_per_district, resource_district_income, get_income, get_income, get_income)
        doc.wasDerivedFrom(incidents_per_district, resource_district_education, get_education, get_education, get_education)

        repo.logout()
        
        return doc

'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

## eof
