import geopandas
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import bson.code

class districts_income(dml.Algorithm):
    contributor = 'hek_kquirk'
    reads = ['hek_kquirk.per_capita_income', 'hek_kquirk.neighborhood_district']
    writes = ['hek_kquirk.district_income']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('hek_kquirk', 'hek_kquirk')

        repo.dropCollection("hek_kquirk.district_income")
        repo.createCollection("hek_kquirk.district_income")
        repo.dropCollection("hek_kquirk.tmp_collection")
        repo.createCollection("hek_kquirk.tmp_collection")

        pipeline = [
            {"$lookup": {"from":"hek_kquirk.neighborhood_district", "localField": "Neighborhood", "foreignField": "_id", "as": "tmp"}},
            {"$project":{"District":"$tmp.district", "_id":"$Neighborhood", "total_population":"$Total population", "aggregate_income":"$Aggregate income in the past 12 months (in 2017 Inflation-adjusted dollars)", "per_capita_income":"$Per Capita Income"}},
            {"$out":"hek_kquirk.tmp_collection"}
        ]

        repo['hek_kquirk.per_capita_income'].aggregate(pipeline)

        mapper = bson.code.Code("""
            function() {
                var stats = {'total_population': parseFloat(String(this.total_population).replace(/[$,\(\)]+/g,"")), 'aggregate_income': parseFloat(String(this.aggregate_income).replace(/[$,\(\)]+/g,"")), 'per_capita_income':parseFloat(String(this.per_capita_income).replace(/[$,\(\)]+/g,""))};
                emit(this.District[0], stats);
            }
        """)         

        reducer = bson.code.Code("""
            function(k, vs) {
                var tot_population = 0;
                var agg_income = 0;

                vs.forEach(function(v,i) {
                    tot_population += v.total_population;
                    agg_income += v.aggregate_income;
                });
                var per_cap = agg_income / tot_population;
                return {'total_population':tot_population, 'aggregate_income': agg_income, 'per_capita_income': per_cap};
            }
        """)

        repo['hek_kquirk.tmp_collection'].map_reduce(mapper, reducer, "hek_kquirk.district_income")
        repo['hek_kquirk.district_income'].metadata({'complete':True})
        print(repo['hek_kquirk.district_income'].metadata())
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

        this_script = doc.agent('alg:hek_kquirk#districts_income', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource_per_capita_income = doc.entity('dat:hek_kquirk#per_capita_income', {'prov:label':'Boston Neighborhoods per capita income Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_neighborhood = doc.entity('dat:hek_kquirk#neighborhood_district', {'prov:label':'Boston Neighborhood to Police Districts', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        get_per_capita_income = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_districts = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_per_capita_income, this_script)
        doc.wasAssociatedWith(get_districts, this_script)
        
        doc.usage(get_per_capita_income, resource_per_capita_income, startTime, None,
                  {
                      prov.model.PROV_TYPE:'ont:Retrieval'
                  }
        )
        doc.usage(get_districts, resource_neighborhood, startTime, None,
                  {
                      prov.model.PROV_TYPE:'ont:Retrieval'
                  }
        )
        
        district_income = doc.entity('dat:hek_kquirk#district_income', {prov.model.PROV_LABEL:'income to Police District', prov.model.PROV_TYPE:'ont:DataSet'})
        
        doc.wasAttributedTo(district_income, this_script)
        
        doc.wasGeneratedBy(district_income, get_per_capita_income, endTime)
        doc.wasGeneratedBy(district_income, get_districts, endTime)
        
        doc.wasDerivedFrom(district_income, resource_per_capita_income, get_per_capita_income, get_per_capita_income, get_per_capita_income)
        doc.wasDerivedFrom(district_income, resource_neighborhood, get_districts, get_districts, get_districts)

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