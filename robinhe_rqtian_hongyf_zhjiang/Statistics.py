'''
Problem:
    The number of injuries depends on the severity of the crash. Here in the dataset, we have columns of data about
    injuries in every accident, non-fatal and fatal ones. We also have columns about number of cars involved in the
    accident. We want to find out if there is any relation between them.

    Problem:
        the relation between the number of cars involved in the accident and the number of injuries.

    Solution:
        1, calculate average and standard deviation of non-fatal injuries, fatal injuries, injuries and number of cars.
        2, calculate correlation coefficient between these dimensions.

'''
import numpy as np
import math
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


class statistics(dml.Algorithm):
    contributor = 'robinhe_rqtian_hongyf_zhjiang'
    reads = []
    writes = ['robinhe_rqtian_hongyf_zhjiang.statistics']

    @staticmethod
    def average(arr):
        sum = 0
        count = 0
        for item in arr:
            count += 1
            sum += item
        # print(sum,count)
        avg = sum/count
        return avg


    @staticmethod
    def standard_deviation(arr):
        avg = statistics.average(arr)
        sum = 0
        count = 0
        for item in arr:
            count += 1
            sum += np.square(item - avg)
        std = np.sqrt(sum/count)
        return std

    @staticmethod
    def covariance(x,y):
        avg_x = statistics.average(x)
        avg_y = statistics.average(y)
        return sum([(xi - avg_x) * (yi - avg_y) for (xi, yi) in zip(x, y)]) / len(x)

    @staticmethod
    def corr(x, y):  # Correlation coefficient.
        if statistics.standard_deviation(x) * statistics.standard_deviation(y) != 0:
            return statistics.covariance(x, y) / (statistics.standard_deviation(x) * statistics.standard_deviation(y))

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('robinhe_rqtian_hongyf_zhjiang', 'robinhe_rqtian_hongyf_zhjiang')

        print("loading data")
        url = 'http://datamechanics.io/data/robinhe_rqtian_hongyf_zhjiang/crash_data_01_19.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        print("data loaded, calculating")

        output = []
        inter = []
        if trial:
            length = round(len(r)/10)
        else:
            length = len(r)
        for idx in range(0,length):
            item = r[idx]
            try:
                inter.append([int(item['Number of NonFatal Injuries']),
                int(item['Number of Fatal Injuries']),
                int(item['Number of NonFatal Injuries'] + item['Number of Fatal Injuries']),
                int(item['Number of Vehicles'])])
            except ValueError:
                continue
        inter = np.array(inter)
        print(inter.shape)
        avg_non_fatal = statistics.average(inter[:, 0])
        avg_fatal = statistics.average(inter[:, 1])
        avg_injuries = statistics.average(inter[:, 2])
        avg_car = statistics.average(inter[:, 3])
        std_non_fatal = statistics.standard_deviation(inter[:, 0])
        std_fatal = statistics.standard_deviation(inter[:, 1])
        std_injuries = statistics.standard_deviation(inter[:, 2])
        std_car = statistics.standard_deviation(inter[:, 3])
        cov_nonfatal_car = statistics.covariance(inter[:, 0],inter[:,3])
        cov_fatal_car = statistics.covariance(inter[:, 1], inter[:,3])
        cov_injuries_car = statistics.covariance(inter[:,2], inter[:,3])
        corr_nonfatal_car = statistics.corr(inter[:, 0],inter[:,3])
        corr_fatal_car = statistics.corr(inter[:, 1], inter[:,3])
        corr_injuries_car = statistics.corr(inter[:,2], inter[:,3])

        output.append({'avg_non_fatal':avg_non_fatal,
                       'avg_fatal':avg_fatal,
                       'avg_injuries':avg_injuries,
                       'avg_car':avg_car,
                       'std_non_fatal':std_non_fatal,
                       'std_fatal':std_fatal,
                       'std_injuries':std_injuries,
                       'std_car':std_car,
                       'cov_nonfatal_car':cov_nonfatal_car,
                       'cov_fatal_car':cov_fatal_car,
                       'cov_injuries_car':cov_injuries_car,
                       'corr_nonfatal_car':corr_nonfatal_car,
                       'corr_fatal_car':corr_fatal_car,
                       'corr_injuries_car':corr_injuries_car})
        s = json.dumps(output, sort_keys=True, indent=2)
        print(output)
        repo.dropCollection("statistics")
        repo.createCollection("statistics")
        repo['robinhe_rqtian_hongyf_zhjiang.statistics'].insert_many(output)
        repo['robinhe_rqtian_hongyf_zhjiang.statistics'].metadata({'complete': True})
        print(repo['robinhe_rqtian_hongyf_zhjiang.statistics'].metadata())
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
        repo.authenticate('robinhe_rqtian_hongyf_zhjiang', 'robinhe_rqtian_hongyf_zhjiang')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:robinhe_rqtian_hongyf_zhjiang#statistics',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8w-nujj',
                              {'prov:label': 'crash accidents data', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_statistics = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_statistics, this_script)
        doc.usage(get_statistics, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )

        statistics_injury_car = doc.entity('dat:robinhe_rqtian_hongyf_zhjiang#statistics',
                          {prov.model.PROV_LABEL: 'statistics of relevance between car number and injury number', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(statistics_injury_car, this_script)
        doc.wasGeneratedBy(statistics_injury_car, get_statistics, endTime)
        doc.wasDerivedFrom(statistics_injury_car, resource, get_statistics, get_statistics, get_statistics)

        repo.logout()

        return doc


# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
# statistics.execute(False)
# doc = statistics.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof