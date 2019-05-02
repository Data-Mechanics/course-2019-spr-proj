# Problem- To learn if bigger streets are generally male or unknown gendered? - correlation coefficient of gender vs. length of street

import dml
import prov.model
import datetime
import uuid
from math import *
from random import random
import scipy.stats

class statistics(dml.Algorithm):
    contributor = 'kgrewal_shin2'
    reads = ['kgrewal_shin2.street_names']
    writes = ['kgrewal_shin2.street_length_vs_gender']


    ### Statistics Methods from class ###########################

    @staticmethod
    def permute(x):
        shuffled = [xi for xi in x]
        random(shuffled)
        return shuffled

    @staticmethod
    def avg(x):  # Average
        return sum(x) / len(x)

    @staticmethod
    def stddev(x):  # Standard deviation.
        m = statistics.avg(x)
        return sqrt(sum([(xi - m) ** 2 for xi in x]) / len(x))

    @staticmethod
    def cov(x, y):  # Covariance.
        return sum([(xi - statistics.avg(x)) * (yi - statistics.avg(y)) for (xi, yi) in zip(x, y)]) / len(x)

    @staticmethod
    def corr(x, y):  # Correlation coefficient.
        if statistics.stddev(x) * statistics.stddev(y) != 0:
            return statistics.cov(x, y) / (statistics.stddev(x) * statistics.stddev(y))

    @staticmethod
    def p(x, y):
        c0 = statistics.corr(x, y)
        corrs = []
        for k in range(0, 2000):
            y_permuted = statistics.permute(y)
            corrs.append(statistics.corr(x, y_permuted))
        return len([c for c in corrs if abs(c) >= abs(c0)]) / len(corrs)

    ############################################################


    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kgrewal_shin2', 'kgrewal_shin2')

        repo.dropCollection('street_length_vs_gender')
        repo.createCollection('street_length_vs_gender')

        street_info = repo.kgrewal_shin2.street_names.find()

        lengths = []
        genders = []

        for x in street_info:
            lengths.append(x['rank'])
            gender = x['gender']
            # KEY: 0 = male/unknown, 1 = female
            if gender == "unknown" or gender == "male":
                genders.append(0)
            else:
                genders.append(1)

        corr, p = scipy.stats.pearsonr(lengths, genders)
        corrInfo = [{'corr':  corr, 'p-value': p}]

        repo['kgrewal_shin2.street_length_vs_gender'].insert_many(corrInfo)
        repo['kgrewal_shin2.street_length_vs_gender'].metadata({'complete': True})
        # print(repo['kgrewal_shin2.street_length_vs_gender'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        #Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kgrewal_shin2', 'kgrewal_shin2')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:kgrewal_shin2#statistics',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8w-nujj',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_streets = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_streets, this_script)
        doc.usage(get_streets, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Statistics&$select=corr,p-value'
                   }
                  )

        streets = doc.entity('dat:kgrewal_shin2#statistics',
                             {prov.model.PROV_LABEL: 'Street Statistics', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(streets, this_script)
        doc.wasGeneratedBy(streets, get_streets, endTime)
        doc.wasDerivedFrom(streets, resource, get_streets, get_streets, get_streets)

        repo.logout()

        return doc


# statistics.execute()
# # doc = statistics.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))


