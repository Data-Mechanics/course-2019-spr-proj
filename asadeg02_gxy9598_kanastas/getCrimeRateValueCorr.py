import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sys
from .helper.statistics import statistics

class getCrimeRateValueCorr(dml.Algorithm):
    contributor = 'asadeg02_gxy9598'
    reads = ['asadeg02_gxy9598.crime_rate_mean_value']
    writes = ['asadeg02_gxy9598.correlations']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asadeg02_gxy9598', 'asadeg02_gxy9598')
        repo.dropCollection("asadeg02_gxy9598.correlations")
        repo.createCollection("asadeg02_gxy9598.correlations")

        crime_value = repo.asadeg02_gxy9598.crime_rate_mean_value.find()
        
        
        x = []
        y = []
        corr_crime_value = 0
        for doc in crime_value:                
                x += [doc["crime_rate"]]
                y += [doc["value"]]

        #stats = statistics.statistics()
        if len(x) > 0:
            results = {}
            results['_id'] = 'crime_rate_mean_value_corelation'
            results['correlation_value'] = statistics.corr(x,y)
            results['p_value'] = statistics.p(x,y)        
        
            print('correlation coefficient between crime rate and mean peroperty value is: ' + str(results['correlation_value']))
            print('p-value for correlation coefficient between crime rate and mean peroperty value is: ' + str(results['p_value']))
            repo['asadeg02_gxy9598.correlations'].insert_one(results)
        repo.logout()
        
        repo['asadeg02_gxy9598.correlations'].metadata({'complete':True})
        print(repo['asadeg02_gxy9598.correlations'].metadata())
        print("Finish calculating correlation value for crime rate and average property value per street.")
        repo.logout()
        endTime = datetime.datetime.now()
        return {"start": startTime, "end": endTime}

    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('cob', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bod', 'http://bostonopendata.boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:asadeg02_gxy9598#Correlation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})


        resource_correlation = doc.entity('dat:asadeg02_gxy9598#crime_rate_mean_value', {prov.model.PROV_LABEL:'crime_rate_mean_value', prov.model.PROV_TYPE:'ont:DataSet'})
        get_correlation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get crime value', prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasAssociatedWith(get_correlation, this_script)
        doc.usage(get_correlation, resource_correlation , startTime)
        correlation = doc.entity('dat:asadeg02_gxy9598#Correlations', {prov.model.PROV_LABEL:'Correlation report', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(correlation, this_script)
        doc.wasGeneratedBy(correlation, get_correlation, endTime)
        doc.wasDerivedFrom(correlation, resource_correlation, get_correlation, get_correlation, get_correlation)
        
        return doc       
