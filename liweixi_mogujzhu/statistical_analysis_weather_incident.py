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

from random import shuffle
from math import sqrt


# This script is to compute the correlation coefficient between 
# daily weather (temperature, rain, wind etc.) and fire incident
class statistical_analysis_weather_incident(dml.Algorithm):
    contributor = 'liweixi_mogujzhu'
    reads = ['liweixi_mogujzhu.weather_fire_incident_transformation']
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
        data1 = [(x, y) for x in data['TMAX'] ]

        print(NINCIDENT)
        # data_ = [(18, 28), (24, 18), (27, 31), (14, 15), (46, 23),
        # (36, 19), (27, 10), (34, 25), (19, 15), (13, 13),
        # (4, 2), (17, 20), (28, 12), (36, 11), (26, 14),
        # (19, 19), (24, 13), (25, 6), (20, 8), (17, 22),
        # (18, 8), (25, 12), (28, 27), (31, 28), (35, 22),
        # (17, 8), (19, 19), (23, 23), (22, 11)]
        # x = [xi for (xi, yi) in data]
        # y = [yi for (xi, yi) in data]
        



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
        resource = doc.entity('dat:liweixi_mogujzhu#weather_fire_incident_transformation',
                              {'prov:label': 'Boston Weather and Fire Incident', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_statistical_analysis_weather_incident = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_statistical_analysis_weather_incident, this_script)

        statistical_analysis_weather_incident = doc.entity('dat:liweixi_mogujzhu#weather_fire_incident_transformation',
                          {prov.model.PROV_LABEL: 'Boston Weather and Fire Incident', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(statistical_analysis_weather_incident, this_script)
        doc.wasGeneratedBy(statistical_analysis_weather_incident, get_statistical_analysis_weather_incident, endTime)
        doc.wasDerivedFrom(statistical_analysis_weather_incident, resource,
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

# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
if __name__ == "__main__":
    statistical_analysis_weather_incident.execute()
    # doc = prediction_weather_incident.provenance()
    # print(doc.get_provn())
    # print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof