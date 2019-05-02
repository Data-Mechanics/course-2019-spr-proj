import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code


class aggregatePropertiesByOwner(dml.Algorithm):

    contributor = 'asadeg02_gxy9598'
    reads = ['asadeg02_gxy9598.age_numProperties_phone_ocupation_addr']
    writes = ['asadeg02_gxy9598.owner_num_properties_age']

    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asadeg02_gxy9598', 'asadeg02_gxy9598')

        repo.dropPermanent('asadeg02_gxy9598.owner_num_properties_age')
        repo.createPermanent('asadeg02_gxy9598.owner_num_properties_age')

        
        
        prop_per_owner =  repo.asadeg02_gxy9598.age_numProperties_phone_ocupation_addr.find()
        
        #count aggregation using address as keys
        mapper = Code("function () {"                   
                   "   emit(this.OWNER, {count:1, age: this.AGE});"
                   "}")        

        reducer = Code("""
            function(key, vs) {
                var num_properties = 0;
                vs.forEach(function(v, i) {                    
                    num_properties += v.count;                    
                });
                return {num_properties:num_properties, age: vs[0].age};
            }
        """)
        
        repo.asadeg02_gxy9598.age_numProperties_phone_ocupation_addr.map_reduce(
                           mapper, reducer, 'asadeg02_gxy9598.owner_num_properties_age')
  
        
        repo["asadeg02_gxy9598.owner_num_properties_age"].metadata({'complete':True})
        print(repo["asadeg02_gxy9598.owner_num_properties_age"].metadata())
        print('Load NUmber Of Properties Per Owner')       
        repo.logout()
        endTime = datetime.datetime.now()
        return {"Start ":startTime, "End ":endTime}

      ###############################################################################################################################################

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('cob', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bod', 'http://bostonopendata.boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:asadeg02_gxy9598#aggregatePropertiesByOwner', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})        
        aggregate_properties_by_owner = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Pairs The Number Of Properties of Each Owner And Their Age', prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasAssociatedWith(aggregate_properties_by_owner, this_script)
 
        resource_age_numProperties_phone_ocupation_addr = doc.entity('dat:asadeg02_gxy9598#age_numProperties_phone_ocupation_addr', {prov.model.PROV_LABEL:'Personal Info Of Owners', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.usage(aggregate_properties_by_owner, resource_age_numProperties_phone_ocupation_addr, startTime)

        owner_num_properties_age = doc.entity('dat:asadeg02_gxy9598#owner_num_properties_age', {'prov:label':'Number of properties and age of the owners', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.wasAttributedTo(owner_num_properties_age, this_script)
        doc.wasGeneratedBy(owner_num_properties_age, aggregate_properties_by_owner, endTime)
        doc.wasDerivedFrom(owner_num_properties_age, resource_age_numProperties_phone_ocupation_addr, aggregate_properties_by_owner, aggregate_properties_by_owner, aggregate_properties_by_owner)
        
        return doc
