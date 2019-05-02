import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
import time
from .helper.scrapeAssessors import scrapeAssessors as scrapper



class mergeValueWithCrimeRate(dml.Algorithm):
    
    contributor = 'asadeg02_gxy9598'
    reads = ['asadeg02_gxy9598.address_crime_rate']
    writes = ['asadeg02_gxy9598.property_value_for_dangerous_neighbourhoods']    
  
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asadeg02_gxy9598', 'asadeg02_gxy9598')        

        address_crime_rate =  repo.asadeg02_gxy9598.address_crime_rate.find()

        print(address_crime_rate)
        #sort crime rates in assending order
        rates = []
        for doc in address_crime_rate:            
            rates.append(int(doc['value']))            
        rates = list(set(rates))
        rates.sort(reverse=True)
          

        
        rates = rates[:1]        
        #merge by initial address_crime_rate dataset        
        address_crime_rate =  repo.asadeg02_gxy9598.address_crime_rate.find()
        sorted_crime_rates = []
        for doc in address_crime_rate:            
            if int(doc['value']) in rates:
                sorted_crime_rates.append(doc)        
        
        address_list = []
        for doc in sorted_crime_rates:
            if doc['_id'] != '':
                address_list.append(doc['_id']) 
       
        
        results = scrapper.scrapeAssessors(address_list)
        

        repo.dropCollection('asadeg02_gxy9598.property_value_for_dangerous_neighbourhoods')
        repo.createCollection('asadeg02_gxy9598.property_value_for_dangerous_neighbourhoods')     
        #repo['asadeg02_gxy9598.property_value_for_dangerous_neighbourhoods'].insert_many(results) 
        _id = 0
        for r in results:
            r['_id'] = _id
            repo["asadeg02_gxy9598.property_value_for_dangerous_neighbourhoods"].insert(r)
            _id += 1
        repo['asadeg02_gxy9598.property_value_for_dangerous_neighbourhoods'].metadata({'complete':True})
        print(repo['asadeg02_gxy9598.property_value_for_dangerous_neighbourhoods'].metadata())
        print('Load Property Value For the most dangerous Neighbourhoods')
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

        this_script = doc.agent('alg:asadeg02_gxy9598#mergeValueWithCrimeRate', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})        
        merge_value_with_crime_rate = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, 
                                        {'prov:label':'Finds The Property Value Of Properties In Dangerouse', prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasAssociatedWith(merge_value_with_crime_rate, this_script) 
        
        resource_crimes_incident = doc.entity('dat:asadeg02_gxy9598#address_crime_rate', {prov.model.PROV_LABEL:'Address Crime Rate', prov.model.PROV_TYPE:'ont:DataSet'})
        resource_property_value = doc.entity('cob:', {'prov:label':'Real Estate Assessments', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        doc.usage(merge_value_with_crime_rate, resource_crimes_incident, startTime)
        doc.usage(merge_value_with_crime_rate, resource_property_value, startTime)

        property_value_for_dangerous_neighbourhoods = doc.entity('dat:asadeg02_gxy9598#property_value_for_dangerous_neighbourhoods', 
                                       {prov.model.PROV_LABEL:'Value for Properties In Dangereous Neighbourhoods ', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.wasAttributedTo(property_value_for_dangerous_neighbourhoods, this_script)
        doc.wasGeneratedBy(property_value_for_dangerous_neighbourhoods, merge_value_with_crime_rate, endTime)
        doc.wasDerivedFrom(property_value_for_dangerous_neighbourhoods, resource_crimes_incident, merge_value_with_crime_rate, merge_value_with_crime_rate, merge_value_with_crime_rate)
        doc.wasDerivedFrom(property_value_for_dangerous_neighbourhoods, resource_property_value, merge_value_with_crime_rate, merge_value_with_crime_rate, merge_value_with_crime_rate)
        
        
        return doc