import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import time
from .helper.scrapeAssessors import scrapeAssessors as scrapper


class getMeanValueAndCrimeRate(dml.Algorithm):
    
    contributor = 'asadeg02_gxy9598'
    reads = ['asadeg02_gxy9598.Boston_street_segments', 'asadeg02_gxy9598.address_crime_rate']
    writes = ['asadeg02_gxy9598.crime_rate_mean_value']    
  
    @staticmethod
    def execute(trial = False):
         
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asadeg02_gxy9598', 'asadeg02_gxy9598')        

        street_names_types =  repo.asadeg02_gxy9598.Get_Boston_Streets.find({}, {'ST_NAME': 1, 'ST_TYPE': 1, '_id': 0})
        streets = set()
        for doc in street_names_types:
            if doc['ST_NAME'] != None and doc['ST_TYPE'] != None:
                streets.add(doc['ST_NAME'] + ' ' + doc['ST_TYPE'])
            elif doc['ST_TYPE'] == None:
                streets.add(doc['ST_NAME'])

        address_crime_rate = repo.asadeg02_gxy9598.address_crime_rate.find()

        # list of streets appearing in at least one entry in crime incident data set
        dangerous_streets = []

        street_crime_rate_dict = {}
        for doc in address_crime_rate:
            if doc['_id'] != None:
                dangerous_streets.append(doc['_id'])
                street_crime_rate_dict[doc['_id']] = doc['value']
        
        #list of streets which don't appear in crime incident data set even once
        safe_streets = []   
        for street in streets:
            if street not in dangerous_streets:
                safe_streets.append(street)

        
        crime_rate_mean_value = []
        
        if trial == True:
            dangerous_streets = dangerous_streets[:5]   
        
        for street in dangerous_streets:
            street_value_crime_rate = {'street': street, 'crime_rate': street_crime_rate_dict[street]}            
            results = scrapper.scrapeAssessors([street])            
            if len(results) > 0:
                mean_value = 0
                for result in results:                    
                    mean_value += float(result['VALUE'].replace('$', '').replace(',',''))
                street_value_crime_rate['value'] = mean_value/len(results)            
                crime_rate_mean_value.append(street_value_crime_rate)

        for street in safe_streets[:1]:
            street_value_crime_rate = {'street': street, 'crime_rate': 0}
            results = scrapper.scrapeAssessors([street])            
            if len(results) > 0:
                mean_value = 0
                for result in results:
                    mean_value += float(result['VALUE'].replace('$', '').replace(',',''))
                street_value_crime_rate['value'] = mean_value/len(results)
                crime_rate_mean_value.append(street_value_crime_rate)

        repo.dropCollection('asadeg02_gxy9598.crime_rate_mean_value')
        repo.createCollection('asadeg02_gxy9598.crime_rate_mean_value')     
         
        _id = 0
        for r in crime_rate_mean_value:
            r['_id'] = _id
            repo['asadeg02_gxy9598.crime_rate_mean_value'].insert(r)
            _id += 1
        repo['asadeg02_gxy9598.crime_rate_mean_value'].metadata({'complete':True})
        print(repo['asadeg02_gxy9598.crime_rate_mean_value'].metadata())
        print('Load Mean Value And Crime Rate For Addresses')
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

        this_script = doc.agent('alg:asadeg02_gxy9598#getMeanValueAndCrimeRate', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})        
        get_mean_value_and_crime_rate = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, 
                                        {'prov:label':'Computes The Mean Value And Crime Rate For Boston Addresses', prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasAssociatedWith(get_mean_value_and_crime_rate, this_script) 
        
        resource_Boston_street_segments = doc.entity('dat:asadeg02_gxy9598#Boston_street_segments', {prov.model.PROV_LABEL:'Boston Street Segments', prov.model.PROV_TYPE:'ont:DataSet'})
        resource_address_crime_rate = doc.entity('dat:asadeg02_gxy9598#address_crime_rate', {prov.model.PROV_LABEL:'Address Crime Rate', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.usage(get_mean_value_and_crime_rate, resource_Boston_street_segments, startTime)
        doc.usage(get_mean_value_and_crime_rate, resource_address_crime_rate, startTime)

        crime_rate_mean_value = doc.entity('dat:asadeg02_gxy9598#crime_rate_mean_value', 
                                       {prov.model.PROV_LABEL:'Property Mean Value And Crime Rate For Boston Addresses', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.wasAttributedTo(crime_rate_mean_value, this_script)
        doc.wasGeneratedBy(crime_rate_mean_value, get_mean_value_and_crime_rate)
        doc.wasDerivedFrom(crime_rate_mean_value, resource_Boston_street_segments, get_mean_value_and_crime_rate, get_mean_value_and_crime_rate, get_mean_value_and_crime_rate)
        doc.wasDerivedFrom(crime_rate_mean_value, resource_address_crime_rate, get_mean_value_and_crime_rate, get_mean_value_and_crime_rate, get_mean_value_and_crime_rate)
        
        return doc
