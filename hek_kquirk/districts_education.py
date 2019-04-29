import geopandas
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import bson.code

class districts_education(dml.Algorithm):
    contributor = 'hek_kquirk'
    reads = ['hek_kquirk.education_level', 'hek_kquirk.neighborhood_district']
    writes = ['hek_kquirk.district_education']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('hek_kquirk', 'hek_kquirk')

        repo.dropCollection("hek_kquirk.district_education")
        repo.createCollection("hek_kquirk.district_education")
        repo.dropCollection("hek_kquirk.tmp_collection")
        repo.createCollection("hek_kquirk.tmp_collection")

        pipeline = [
            {"$lookup": {"from":"hek_kquirk.neighborhood_district", "localField": "Neighborhood", "foreignField": "_id", "as": "tmp"}},
            {"$project":{"District":"$tmp.district", "_id":"$Neighborhood", "total_population":"$Total population 25 years and over", "bachelor":"$Bachelor's Degree or more", "percent":"$%"}},
            {"$out":"hek_kquirk.tmp_collection"}
        ]

        repo['hek_kquirk.education_level'].aggregate(pipeline)

        mapper = bson.code.Code("""
            function() {
                var stats = {'total_population': parseFloat(String(this.total_population).replace(/[$,\(\)]+/g,"")), 'bachelor': parseFloat(String(this.bachelor).replace(/[$,\(\)]+/g,"")), 'percent':parseFloat(String(this.percent).replace(/[$%,\(\)]+/g,""))};
                emit(this.District[0], stats);
            }
        """)         

        reducer = bson.code.Code("""
            function(k, vs) {
                var tot_population = 0;
                var num_bachelor = 0;

                vs.forEach(function(v,i) {
                    tot_population += v.total_population;
                    num_bachelor += v.bachelor;
                });
                var percent = num_bachelor / tot_population * 100;
                return {'total_population':tot_population, 'bachelor': num_bachelor, 'percent': percent};
            }
        """)

        repo['hek_kquirk.tmp_collection'].map_reduce(mapper, reducer, "hek_kquirk.district_education")
        repo['hek_kquirk.district_education'].metadata({'complete':True})
        print(repo['hek_kquirk.district_education'].metadata())
        repo.dropCollection("hek_kquirk.tmp_collection")

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

        this_script = doc.agent('alg:hek_kquirk#districts_education', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource_education_level = doc.entity('dat:hek_kquirk#education_level', {'prov:label':'Boston Neighborhoods education level Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_neighborhood = doc.entity('dat:hek_kquirk#neighborhood_district', {'prov:label':'Boston Neighborhood to Police Districts', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        get_education_level = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_districts = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_education_level, this_script)
        doc.wasAssociatedWith(get_districts, this_script)
        
        doc.usage(get_education_level, resource_education_level, startTime, None,
                  {
                      prov.model.PROV_TYPE:'ont:Retrieval'
                  }
        )
        doc.usage(get_districts, resource_neighborhood, startTime, None,
                  {
                      prov.model.PROV_TYPE:'ont:Retrieval'
                  }
        )
        
        district_education = doc.entity('dat:hek_kquirk#district_education', {prov.model.PROV_LABEL:'Education level to Police District', prov.model.PROV_TYPE:'ont:DataSet'})
        
        doc.wasAttributedTo(district_education, this_script)
        
        doc.wasGeneratedBy(district_education, get_education_level, endTime)
        doc.wasGeneratedBy(district_education, get_districts, endTime)
        
        doc.wasDerivedFrom(district_education, resource_education_level, get_education_level, get_education_level, get_education_level)
        doc.wasDerivedFrom(district_education, resource_neighborhood, get_districts, get_districts, get_districts)

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