import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np

from random import shuffle
from math import sqrt

class correlation(dml.Algorithm):
    # contributor = 'Jinghang_Yuan'

    #project 2 contributors:
    contributor = 'xcao19_yjhang_zy0105'
    reads = ['Jinghang_Yuan.ZIPCounter']
    writes = ['Jinghang_Yuan.correlation']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('Jinghang_Yuan', 'Jinghang_Yuan')

        data=repo['Jinghang_Yuan.ZIPCounter'].find()
        if trial:
            data = data.head(20)
        ave_val = []
        centerNum = []
        centerPoolNum = []
        policeStationNum = []
        schoolNum = []

        for i in data:
            ave_val += [i["val_avg"]]
            centerNum += [i["centerNum"]]
            centerPoolNum += [i["centerPoolNum"]]
            policeStationNum += [i["policeStationNum"]]
            schoolNum += [i["schoolNum"]]

        def permute(x):
            shuffled = [xi for xi in x]
            shuffle(shuffled)
            return shuffled

        def avg(x):  # Average
            return sum(x) / len(x)

        def stddev(x):  # Standard deviation.
            m = avg(x)
            return sqrt(sum([(xi - m) ** 2 for xi in x]) / len(x))

        def cov(x, y):  # Covariance.
            return sum([(xi - avg(x)) * (yi - avg(y)) for (xi, yi) in zip(x, y)]) / len(x)

        def corr(x, y):  # Correlation coefficient.
            if stddev(x) * stddev(y) != 0:
                return cov(x, y) / (stddev(x) * stddev(y))

        res = []

        # print("avg_value vs centerNum")
        corr_avg_value_centerNum = corr(ave_val,centerNum)
        # print(corr_avg_value_centerNum)
        # print("----------")

        res.append({'avg_value vs centerNum': corr_avg_value_centerNum})

        # print("avg_value vs centerPoolNum")
        corr_avg_value_centerPoolNum = corr(ave_val, centerPoolNum)
        # print(corr_avg_value_centerPoolNum)
        # print("----------")

        res.append({'avg_value vs centerPoolNum': corr_avg_value_centerPoolNum})

        # print("avg_value vs policeStationNum")
        corr_avg_value_policeStationNum = corr(ave_val, policeStationNum)
        # print(corr_avg_value_policeStationNum)
        # print("----------")

        res.append({'avg_value vs policeStationNum': corr_avg_value_policeStationNum})

        # print("avg_value vs schoolNum")
        corr_avg_value_schoolNum = corr(ave_val, schoolNum)
        # print(corr_avg_value_schoolNum)
        # print("----------")

        res.append({'avg_value vs schoolNum': corr_avg_value_schoolNum})

        # print(res)

        repo.dropCollection("correlation")
        repo.createCollection("correlation")
        repo["Jinghang_Yuan.correlation"].insert_many(res)

        # print(list(repo['Jinghang_Yuan.correlation'].find()))

        repo.logout()
        endTime = datetime.datetime.now()
        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('Jinghang_Yuan', 'Jinghang_Yuan')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')


        #---entities---
        resource = doc.entity('dat: Jinghang_Yuan#Jinghang_Yuan.ZIPCounter', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        res = doc.entity('dat:Jinghang_Yuan#Jinghang_Yuan.correlation',
                            {prov.model.PROV_LABEL: 'result', prov.model.PROV_TYPE: 'ont:DataSet'})
        #---agents---
        this_script = doc.agent('alg:Xcao19_yjhang_zy0105#correlation',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        #---algs/activities---
        get_correlation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.usage(get_correlation, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '.find({}, {val_avg:1, centerNum: 1, centerPoolNum: 1, policeStationNum: 1, schoolNum: 1, _id:0})'
                   }
                  )

        doc.wasAttributedTo(res, this_script)
        doc.wasGeneratedBy(res, get_correlation, endTime)
        doc.wasDerivedFrom(res, resource, get_correlation, get_correlation, get_correlation)
        doc.wasAssociatedWith(get_correlation, this_script)
        repo.logout()

        return doc

#correlation.execute()