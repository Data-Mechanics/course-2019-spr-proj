import datetime
import logging
import uuid

import dml
import pandas as pd
import prov.model
from scipy import stats

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class stats_analysis(dml.Algorithm):
    contributor = 'kzhang21_ryuc_zui_sarms'
    reads = ['kzhang21_ryuc_zui_sarms.yelp_business']
    writes = ['kzhang21_ryuc_zui_sarms.statistics']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kzhang21_ryuc_zui_sarms', 'kzhang21_ryuc_zui_sarms')

        repo.dropCollection("statistics")
        repo.createCollection("statistics")

        yelp_business = repo['kzhang21_ryuc_zui_sarms.yelp_business']
        stats_variables = repo['kzhang21_ryuc_zui_sarms.statistics']

        yelp_files = list(
            yelp_business.find({}, {"price": 1, "rating": 1, "violation_rate": 1, "review_count": 1, "_id": 0}))

        log.info('One example: %s', yelp_files[0])

        ## New data
        for row in yelp_files:
            if 'price' in row.keys():
                row['price'] = len(row['price'])
            else:
                row['price'] = None

        df_yelp = pd.DataFrame(yelp_files).apply(pd.Series)

        def getStats(one, two):
            filtered_df = df_yelp[df_yelp[one].notnull() & df_yelp[two].notnull()]
            x = filtered_df[one]
            y = filtered_df[two]

            log.info('%s and %s statistics', one, two)
            (corr_1, p_value_1) = stats.pearsonr(x, y)
            log.info('correlation: %.4f, p-value: %.4f', corr_1, p_value_1)

            append = {'x': one, 'y': two, 'correlation': corr_1, 'p_value': p_value_1}
            stats_variables.insert_one(append)

        getStats("review_count", "rating")
        getStats("review_count", "price")
        getStats("review_count", "violation_rate")
        getStats("rating", "price")
        getStats("rating", "violation_rate")
        getStats("price", "violation_rate")

        stats_variables.metadata({'complete': True})
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
        yelp_business = doc.entity('dat:kzhang21_ryuc_zui_sarms#yelp_business',
                                   {'prov:label': 'yelp data', prov.model.PROV_TYPE: 'ont:DataSet'})
        get_stats = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        statistics = doc.entity('dat:kzhang21_ryuc_zui_sarms#statistics',
                                {'prov:label': 'statistics data', prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAssociatedWith(get_stats, this_script)
        doc.usage(get_stats, yelp_business, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'
                   }
                  )

        doc.wasAttributedTo(statistics, this_script)
        doc.wasGeneratedBy(statistics, get_stats, endTime)
        doc.wasDerivedFrom(yelp_business, statistics, get_stats, get_stats, get_stats)

        repo.logout()

        return doc

# eof