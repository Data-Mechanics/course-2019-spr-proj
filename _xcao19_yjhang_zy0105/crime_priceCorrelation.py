import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

from random import shuffle
from math import sqrt

class crime_priceCorrelation(dml.Algorithm):# contributor = 'Jinghang_Yuan'

    contributor = 'xcao19_yjhang_zy0105'
    reads = []
    writes = []

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('xcao19_yjhang_zy0105', 'xcao19_yjhang_zy0105')

        url = 'http://datamechanics.io/data/airbnb_neighborhood_crime_rate.csv.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)

        ave_price = []
        crime = []

        for i in r:
            ave_price += [i["price"]]
            crime += [i["crime_rate"]]

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

        print("ave_price vs crime")
        corr_ave_price_crime = corr(ave_price,crime)
        print(corr_ave_price_crime)
        print("----------")

        # res.append({'avg_value vs centerPoolNum': corr_avg_value_centerPoolNum})

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

#crime_priceCorrelation.execute()
#crime_priceCorrelation.provenance()
# doc = crime_priceCorrelation.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

#eof