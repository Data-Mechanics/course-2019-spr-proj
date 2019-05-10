import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
from .helper.regression import regression as regr


class crimeRateValueRegression(dml.Algorithm):

    contributor = 'asadeg02_gxy9598'
    reads = ['asadeg02_gxy9598.crime_rate_mean_value']
    writes = ['asadeg02_gxy9598.regressions']

    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asadeg02_gxy9598', 'asadeg02_gxy9598')
        
        
        crime_rate_mean_value =  repo.asadeg02_gxy9598.crime_rate_mean_value.find()
        crime_rates = [doc['crime_rate'] for doc in crime_rate_mean_value]
        crime_rate_mean_value =  repo.asadeg02_gxy9598.crime_rate_mean_value.find()
        mean_values = [doc['value'] for doc in crime_rate_mean_value]
        
        #regr = regression.regression()
        if len(crime_rate) > 0:
            result = regr.getRegressionResults(crime_rates, mean_values, "crime-rate-value-regr", 20)       
            result['_id'] = 'crime_rate_mean_value_regression'

        
           repo.dropCollection('asadeg02_gxy9598.regressions')
           repo.createCollection('asadeg02_gxy9598.regressions') 
        
           #result is already a dictionary we store the coefficients and scores in the collection
           repo['asadeg02_gxy9598.regressions'].insert_one(result)
        
           
           repo['asadeg02_gxy9598.regressions'].metadata({'complete':True})
        print(repo['asadeg02_gxy9598.regressions'].metadata())
        print('Load Regression Results For Modeling The Relationship Between Crime Rate And Mean Property Value') 
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

        this_script = doc.agent('alg:asadeg02_gxy9598#crimeRateValueRegression', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})        
        crime_rate_mean_value_regression = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Compute A Linear Regression Model For Crime Ratre and Property Mean Value Per Street', prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasAssociatedWith(crime_rate_mean_value_regression, this_script)
 
        resource_crime_rate_mean_value = doc.entity('dat:asadeg02_gxy9598#crime_rate_mean_value', {prov.model.PROV_LABEL:'Crime Rate And Mean Property Value Per Street', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.usage(crime_rate_mean_value_regression, resource_crime_rate_mean_value, startTime)

        crime_rate_mean_value_regression = doc.entity('dat:asadeg02_gxy9598#regressions', {'prov:label':'Coefficients And Score Of Linear Regression Of Crime Rate And Mean Property Value', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.wasAttributedTo(crime_rate_mean_value_regression, this_script)
        doc.wasGeneratedBy(crime_rate_mean_value_regression, crime_rate_mean_value_regression, endTime)
        doc.wasDerivedFrom(crime_rate_mean_value_regression, resource_crime_rate_mean_value, crime_rate_mean_value_regression, crime_rate_mean_value_regression, crime_rate_mean_value_regression)
        
        return doc
