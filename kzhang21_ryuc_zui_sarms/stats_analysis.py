import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
import numpy as np
from scipy import stats

class stats_analysis(dml.Algorithm):
    contributor = 'kzhang21_ryuc_zui_sarms'
    reads = ['kzhang21_ryuc_zui_sarms.yelp_business']
    writes = []

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kzhang21_ryuc_zui_sarms', 'kzhang21_ryuc_zui_sarms')

        yelp_business = repo['kzhang21_ryuc_zui_sarms.yelp_business']

        df_yelp = pd.DataFrame(list(yelp_business.find()))
        new_df = pd.DataFrame()

        ## CORRELATION between review and rating
        corr_coef = np.corrcoef(df_yelp['review_count'], df_yelp['rating'])
        p_value = stats.pearsonr(df_yelp['review_count'], df_yelp['rating'])

        print(p_value)
        print(corr_coef)



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