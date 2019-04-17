import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
import numpy as np
from scipy import stats

from random import shuffle
from math import sqrt

import logging

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class stats_analysis(dml.Algorithm):
    contributor = 'kzhang21_ryuc_zui_sarms'
    reads = ['kzhang21_ryuc_zui_sarms.yelp_business']
    writes = ['kzhang21_ryuc_zui_sarms.statistics']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kzhang21_ryuc_zui_sarms', 'kzhang21_ryuc_zui_sarms')

        repo.dropCollection("statistics")
        repo.createCollection("statistics")

        yelp_business = repo['kzhang21_ryuc_zui_sarms.yelp_business']
        stats_variables = repo['kzhang21_ryuc_zui_sarms.statistics']

        yelp_files = list(yelp_business.find({}, {"price": 1, "rating": 1, "violation_rate": 1, "review_count": 1, "_id": 0}))

        log.info('One example: %s', yelp_files[0])

        ## New data
        for row in yelp_files:
            if 'price' in row.keys():
                row['price'] = len(row['price'])
            else:
                row['price'] = None

        log.info('One example: %s', yelp_files[0])

        ## Helper functions
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

        def p(x, y):
            c0 = corr(x, y)
            corrs = []

            for k in range(0, 2000):
                y_permuted = permute(y)
                corrs.append(corr(x, y_permuted))
            return len([c for c in corrs if abs(c) >= abs(c0)]) / len(corrs)

        df_yelp = pd.DataFrame(yelp_files).apply(pd.Series)
        def getStats(one, two):
            x = df_yelp[one]
            y = df_yelp[two]

            log.info('%s and %s statistics', one, two)
            (corr_1, p_value_1) = stats.pearsonr(x, y)
            log.info('correlation: %.4f, p-value: %.4f', corr_1, p_value_1)

        getStats("review_count", "rating")
        getStats("review_count", "price")
        getStats("review_count", "violation_rate")
        getStats("rating", "price")
        getStats("rating", "violation_rate")
        getStats("price", "violation_rate")

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kzhang21_ryuc_zui_sarms', 'kzhang21_ryuc_zui_sarms')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:kzhang21_ryuc_zui_sarms#yelp_business',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:business.json',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_business = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_business, this_script)
        doc.usage(get_business, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'
                   }
                  )

        yelp_business = doc.entity('dat:kzhang21_ryuc_zui_sarms#yelp_business',
                                   {prov.model.PROV_LABEL: 'Yelp Businesses', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(yelp_business, this_script)
        doc.wasGeneratedBy(yelp_business, get_business, endTime)
        doc.wasDerivedFrom(yelp_business, resource, get_business, get_business, get_business)

        repo.logout()

        return doc

## eof