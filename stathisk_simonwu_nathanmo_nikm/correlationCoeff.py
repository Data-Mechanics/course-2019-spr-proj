import urllib.request
import dml
import prov.model
import datetime
import uuid
from math import sqrt
from io import StringIO
import pandas as pd

class transformGeneral(dml.Algorithm):
    contributor = 'stathisk_simonwu_nathanmo_nikm'
    reads = ['stathisk_simonwu_nathanmo_nikm.weighted', 'stathisk_simonwu_nathanmo_nikm.majority']
    writes = ['stathisk_simonwu_nathanmo_nikm.correlationCoeff']


    @staticmethod
    def removePeriods(d):
        """Takes a dictionary and removes periods from the keys"""
        for key in d:
            if '.' in key:
                newKey = key.replace('.', '')
                d[newKey] = d[key]
                del d[key]
        return d

    @staticmethod
    def avg(x):  # Average
        return sum(x) / len(x)

    @staticmethod
    def stddev(x):  # Standard deviation.
        m = avg(x)
        return sqrt(sum([(xi - m) ** 2 for xi in x]) / len(x))

    @staticmethod
    def cov(x, y):  # Covariance.
        return sum([(xi - avg(x)) * (yi - avg(y)) for (xi, yi) in zip(x, y)]) / len(x)

    @staticmethod
    def corr(x, y):  # Correlation coefficient.
        if stddev(x) * stddev(y) != 0:
            return cov(x, y) / (stddev(x) * stddev(y))



    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('stathisk_simonwu_nathanmo_nikm', 'stathisk_simonwu_nathanmo_nikm')
        myCol = repo['stathisk_simonwu_nathanmo_nikm.weighted']
        weighted = myCol.find()
        myCol2 = repo['stathisk_simonwu_nathanmo_nikm.majority']
        majority = myCol2.find()
        weightedResults = []
        majorityResults = []
        for row in weighted:
            weightedResults.append(row['Result'])
        for row in majority:
            majorityResults.append(row['Result'])

        cc = corr(majorityResults, weightedResults)
        d = {}
        d['weightedMajority'] = cc







        repo.createCollection("stathisk_simonwu_nathanmo_nikm.correlationCoeff")
        #need to remove periods

        repo['stathisk_simonwu_nathanmo_nikm.correlationCoeff'].insert(d)
        repo['stathisk_simonwu_nathanmo_nikm.correlationCoeff'].metadata({'complete': True})
        # print(repo['stathisk_simonwu_nathanmo_nikm.aggGeneralData'].metadata())

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
        repo.authenticate('stathisk_simonwu_nathanmo_nikm', 'stathisk_simonwu_nathanmo_nikm')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:stathisk_simonwu_nathanmo_nikm#correlationCoeff',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource = doc.entity('dat:stathisk_simonwu_nathanmo_nikm.weighted',
                              {'prov:label': 'Information on general election', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'csv'})


        get_general = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_general, this_script)

        doc.usage(get_general, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:DataResource'})
        # change 'avg' title to 'agg' above later
        cc = doc.entity('dat:stathisk_simonwu_nathanmo_nikm.weighted#weighted',
                                                    {prov.model.PROV_LABEL: 'weighted poll questions',
                                                     prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(cc, this_script)
        doc.wasDerivedFrom(cc, resource, get_general, get_general, get_general)
        doc.wasGeneratedBy(cc, resource, endTime)


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