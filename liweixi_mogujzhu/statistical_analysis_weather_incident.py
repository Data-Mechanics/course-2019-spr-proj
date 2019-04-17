import urllib.request
import json
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
import dml
import prov.model
import datetime
import uuid
import numpy
import pandas as pd
import scipy.stats

from random import shuffle
from math import sqrt


# This script is to compute the correlation coefficient between 
# daily weather (temperature, rain, wind etc.) and fire incident
class statistical_analysis_weather_incident(dml.Algorithm):
    contributor = 'liweixi_mogujzhu'
    reads = ['liweixi_mogujzhu.weather_fire_incident_transformation', 'liweixi_mogujzhu.prediction_weather_incident']
    writes = ['liweixi_mogujzhu.statistical_analysis_weather_incident']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('liweixi_mogujzhu', 'liweixi_mogujzhu')
        repo.dropCollection("statistical_analysis_weather_incident")
        repo.createCollection("statistical_analysis_weather_incident")
        
        # create the dataset of different weather features and fire incident
        data_name = 'liweixi_mogujzhu.weather_fire_incident_transformation'
        # retrieve all the data
        data = pd.DataFrame(list(repo[data_name].find()))
        print(data.shape)

        # if trial mode, use half of the data for analysis
        if trial:
            data = data[:data.shape[0]//2]
        # TMAX and fire incident
        for feature in ["TMAX", "TAVG", "TMIN", "AWND", "PRCP", "SNOW"]:
            result = scipy.stats.pearsonr(data[feature], data['NINCIDENT'])
            print("Correlation coefficient between",feature,"and number of incident", result[0])
            print("P-value between",feature,"and number of incident", result[1])

        # Create the training data and target
        data2_name = 'liweixi_mogujzhu.prediction_weather_incident'
        data_model = pd.DataFrame(list(repo[data2_name].find()))
        x = [max(i[0],i[1],i[2]) for i in data_model.values]
        y = data_model["TRUE_VALUE"]
        result = scipy.stats.pearsonr(x, y)
        print("Correlation coefficient between model predict and true value", result[0])
        print("P-value between model predict and true value", result[1])
        # insert result into mongoDB
        # repo['liweixi_mogujzhu.statistical_analysis_weather_incident'].insert_many(insert_data.to_dict('records'))
        # repo['liweixi_mogujzhu.statistical_analysis_weather_incident'].metadata({'complete': True})
        # print(repo['liweixi_mogujzhu.statistical_analysis_weather_incident'].metadata())
        repo.logout()
        endTime = datetime.datetime.now()
        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('liweixi_mogujzhu', 'liweixi_mogujzhu')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.


        this_script = doc.agent('alg:liweixi_mogujzhu#statistical_analysis_weather_incident',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource1 = doc.entity('dat:liweixi_mogujzhu#weather_fire_incident_transformation',
                              {'prov:label': 'Boston Weather and Fire Incident', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        resource2 = doc.entity('dat:liweixi_mogujzhu#prediction_weather_incident',
                              {'prov:label': 'Prediction Boston Weather and Fire Incident', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_statistical_analysis_weather_incident = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_statistical_analysis_weather_incident, this_script)

        statistical_analysis_weather_incident = doc.entity('dat:liweixi_mogujzhu#weather_fire_incident_transformation',
                          {prov.model.PROV_LABEL: 'Boston Weather and Fire Incident', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(statistical_analysis_weather_incident, this_script)
        doc.wasGeneratedBy(statistical_analysis_weather_incident, get_statistical_analysis_weather_incident, endTime)
        doc.wasDerivedFrom(statistical_analysis_weather_incident, resource1,
                           get_statistical_analysis_weather_incident, get_statistical_analysis_weather_incident,
                           get_statistical_analysis_weather_incident)
        doc.wasDerivedFrom(statistical_analysis_weather_incident, resource2,
                           get_statistical_analysis_weather_incident, get_statistical_analysis_weather_incident,
                           get_statistical_analysis_weather_incident)
        repo.logout()

        return doc

def permute(x):
    shuffled = [xi for xi in x]
    shuffle(shuffled)
    return shuffled

def avg(x): # Average
    return sum(x)/len(x)

def stddev(x): # Standard deviation.
    m = avg(x)
    return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

def cov(x, y): # Covariance.
    return sum([(xi-avg(x))*(yi-avg(y)) for (xi,yi) in zip(x,y)])/len(x)

def corr(x, y): # Correlation coefficient.
    if stddev(x)*stddev(y) != 0:
        return cov(x, y)/(stddev(x)*stddev(y))

def p(x, y):
    c0 = corr(x, y)
    corrs = []
    for k in range(0, 200):
        y_permuted = permute(y)
        corrs.append(corr(x, y_permuted))
    return len([c for c in corrs if abs(c) >= abs(c0)]) / len(corrs)

# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
if __name__ == "__main__":
    statistical_analysis_weather_incident.execute()
    # doc = prediction_weather_incident.provenance()
    # print(doc.get_provn())
    # print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof